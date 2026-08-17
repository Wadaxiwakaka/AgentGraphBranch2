"""特殊 root 用户的本地会话入口与直接 Agent 通信能力。"""

from __future__ import annotations

import asyncio
import json
import re
import ssl
from copy import deepcopy
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Any
from uuid import uuid4

import httpx
from fastapi import FastAPI

from core import (
    AgentConfig,
    AgentGraphError,
    ChatSpace,
    PeerConfig,
    UserMessageRequest,
    register_api_error_handlers,
)


_AGENT_ID = re.compile(r"^[A-Za-z0-9_-]{1,64}$")
_ERROR_CODE = re.compile(r"^[A-Z0-9_-]{1,64}$")
_PROTOCOL_VERSION = "1.0"
_TOPOLOGY_BYTES_PER_NODE = 16 * 1024


class User:
    """代表不调用模型、仅把本地用户请求转发给直接可见 Agent 的 root。

    参数:
        config: 已由 ``AgentConfig`` 校验且 ``id`` 必须精确为 ``root`` 的配置。
        http_client: 可选异步 HTTP 客户端；注入时生命周期仍由调用方管理。
        storage_root: ``ChatSpace.save`` 使用的会话归档根目录。

    返回值:
        构造后公开固定 ``id == "root"``、配置、端口和按目标 id 分隔的会话映射。

    异常:
        ValueError: 传入普通 Agent 配置时抛出，因为普通节点必须使用
            ``AgentRemote`` 运行时。

    状态变化:
        只初始化 root 的内存会话和 HTTP 客户端。root 是人类到 Agent 网络的网关，
        不承担模型推理，因此不会创建 OpenAI 客户端、system instructions 或模型 tools。
    """

    def __init__(
        self,
        config: AgentConfig,
        *,
        http_client: httpx.AsyncClient | None = None,
        storage_root: Path = Path("chat_history"),
    ) -> None:
        """保存 root 配置并建立独立的直接邻居与会话索引。

        参数:
            config: ``id`` 为 ``root`` 的配置对象。
            http_client: 可选、由调用方拥有的异步 HTTP 客户端。
            storage_root: 后续关闭会话时使用的持久化根目录。

        返回值:
            ``None``；初始化结果保存在当前实例属性中。

        异常:
            ValueError: ``config.id`` 不是 ``root`` 时抛出。

        状态变化:
            创建空 ``chat_spaces``，并在未注入客户端时创建一个由本实例拥有的
            ``httpx.AsyncClient``；不会初始化任何模型相关对象。
        """

        if config.id != "root":
            raise ValueError("User 只接受 root 配置")

        self.config = config
        self.id = "root"
        self.port = config.port
        self.chat_spaces: dict[str, ChatSpace] = {}
        self.storage_root = Path(storage_root)
        self._peers: dict[str, PeerConfig] = {
            peer.id: peer for peer in config.agents
        }
        self._chat_locks: dict[str, asyncio.Lock] = {}
        if http_client is None:
            self._http_client = httpx.AsyncClient(
                timeout=config.http_timeout_seconds,
                verify=True,
            )
            self._owns_http_client = True
            self._owned_http_clients: list[httpx.AsyncClient] = [self._http_client]
        else:
            self._http_client = http_client
            self._owns_http_client = False
            self._owned_http_clients = []
        self._http_clients_by_ca: dict[str, httpx.AsyncClient] = {}
        self._owned_http_client_closed = False

    def _peer_url(self, peer: PeerConfig, suffix: str) -> str:
        return (
            f"{peer.protocol}://{peer.ip}:{peer.port}"
            f"/v1/agents/{peer.id}/{suffix}"
        )

    def _http_client_for_peer(self, peer: PeerConfig) -> httpx.AsyncClient:
        if not self._owns_http_client or peer.ca_file is None:
            return self._http_client
        ca_key = str(peer.ca_file)
        existing = self._http_clients_by_ca.get(ca_key)
        if existing is not None:
            return existing
        verify_context = ssl.create_default_context(cafile=ca_key)
        client = httpx.AsyncClient(
            timeout=self.config.http_timeout_seconds,
            verify=verify_context,
        )
        self._http_clients_by_ca[ca_key] = client
        self._owned_http_clients.append(client)
        return client

    def _retry_after_from_response(self, response: httpx.Response) -> float:
        try:
            payload = response.json()
        except (json.JSONDecodeError, ValueError):
            payload = None
        return self._retry_after_from_payload(payload)

    def _retry_after_from_payload(self, payload: Any) -> float:
        if isinstance(payload, dict):
            error = payload.get("error")
            if isinstance(error, dict):
                value = error.get("retry_after_seconds")
                if isinstance(value, (int, float)) and not isinstance(value, bool):
                    return float(value)
        return 60.0

    def _topology_byte_limit(self, requested_max_nodes: Any = None) -> int:
        effective_nodes = self.config.topology_max_nodes
        if (
            isinstance(requested_max_nodes, int)
            and not isinstance(requested_max_nodes, bool)
            and requested_max_nodes > 0
        ):
            effective_nodes = min(effective_nodes, requested_max_nodes)
        return max(1, effective_nodes) * _TOPOLOGY_BYTES_PER_NODE

    async def _post_to_peer(
        self,
        peer: PeerConfig,
        suffix: str,
        payload: dict[str, Any],
    ) -> httpx.Response:
        headers = {
            # Bearer 只进入请求头，不能进入 URL、异常文本或日志。
            "Authorization": f"Bearer {peer.key.get_secret_value()}",
        }
        try:
            client = self._http_client_for_peer(peer)
            response = await client.post(
                self._peer_url(peer, suffix),
                headers=headers,
                json=payload,
            )
        except httpx.TimeoutException:
            raise AgentGraphError(
                code="DOWNSTREAM_TIMEOUT",
                message="目标 Agent 响应超时",
                status_code=504,
            ) from None
        except (OSError, httpx.ConnectError, httpx.NetworkError, httpx.RequestError):
            raise AgentGraphError(
                code="DOWNSTREAM_UNAVAILABLE",
                message="目标 Agent 暂时不可达",
                status_code=502,
            ) from None

        if response.status_code == 409:
            raise AgentGraphError(
                code="AGENT_BUSY",
                message="该Agent正在进行其它对话，请等待1min后重试",
                status_code=409,
                retry_after_seconds=self._retry_after_from_response(response),
            )
        if response.status_code in {401, 403}:
            raise AgentGraphError(
                code="AUTHENTICATION_FAILED",
                message="目标 Agent 认证失败",
                status_code=502,
            )
        if not 200 <= response.status_code < 300:
            raise AgentGraphError(
                code="DOWNSTREAM_ERROR",
                message="目标 Agent 请求失败",
                status_code=502,
            )
        return response

    async def _fetch_topology_from_peer(
        self,
        peer: PeerConfig,
        payload: dict[str, Any],
    ) -> dict[str, Any]:
        """流式读取一个直接 peer 的 topology，并在 JSON 解码前限制原始字节。"""

        headers = {
            "Authorization": f"Bearer {peer.key.get_secret_value()}",
            # topology 显式禁用内容压缩，避免解码炸弹绕过原始传输字节预算。
            "Accept-Encoding": "identity",
        }
        byte_limit = self._topology_byte_limit(payload.get("max_nodes"))
        try:
            client = self._http_client_for_peer(peer)
            async with client.stream(
                "POST",
                self._peer_url(peer, "topology"),
                headers=headers,
                json=payload,
            ) as response:
                content_encoding = response.headers.get(
                    "content-encoding",
                    "",
                ).strip().lower()
                if content_encoding not in {"", "identity"}:
                    raise AgentGraphError(
                        code="TOPOLOGY_UNSUPPORTED_CONTENT_ENCODING",
                        message="目标 Agent 拓扑响应使用了不支持的压缩编码",
                        status_code=502,
                    )

                raw_content_length = response.headers.get("content-length")
                try:
                    content_length = int(raw_content_length or "0")
                except ValueError:
                    content_length = 0
                if content_length > byte_limit:
                    raise AgentGraphError(
                        code="TOPOLOGY_RESPONSE_TOO_LARGE",
                        message="目标 Agent 拓扑响应过大",
                        status_code=502,
                    )
                if response.status_code in {401, 403}:
                    raise AgentGraphError(
                        code="AUTHENTICATION_FAILED",
                        message="目标 Agent 认证失败",
                        status_code=502,
                    )
                if response.status_code != 409 and not 200 <= response.status_code < 300:
                    raise AgentGraphError(
                        code="DOWNSTREAM_ERROR",
                        message="目标 Agent 请求失败",
                        status_code=502,
                    )

                response_body = bytearray()

                async def iter_raw_chunks():
                    if response.is_stream_consumed:
                        # MockTransport 的预载 response 可能已持有完整 raw content；真实
                        # 网络响应走 aiter_raw，二者都不会触发自动解压的 aiter_bytes。
                        yield response.content
                        return
                    async for raw_chunk in response.aiter_raw():
                        yield raw_chunk

                async for chunk in iter_raw_chunks():
                    if len(response_body) + len(chunk) > byte_limit:
                        raise AgentGraphError(
                            code="TOPOLOGY_RESPONSE_TOO_LARGE",
                            message="目标 Agent 拓扑响应过大",
                            status_code=502,
                        )
                    response_body.extend(chunk)
                status_code = response.status_code
        except AgentGraphError:
            raise
        except httpx.TimeoutException:
            raise AgentGraphError(
                code="DOWNSTREAM_TIMEOUT",
                message="目标 Agent 响应超时",
                status_code=504,
            ) from None
        except (OSError, httpx.HTTPError):
            raise AgentGraphError(
                code="DOWNSTREAM_UNAVAILABLE",
                message="目标 Agent 暂时不可达",
                status_code=502,
            ) from None

        try:
            decoded_payload = json.loads(response_body)
        except (json.JSONDecodeError, UnicodeDecodeError, ValueError, TypeError):
            decoded_payload = None
        if status_code == 409:
            raise AgentGraphError(
                code="AGENT_BUSY",
                message="该Agent正在进行其它对话，请等待1min后重试",
                status_code=409,
                retry_after_seconds=self._retry_after_from_payload(decoded_payload),
            )
        if not isinstance(decoded_payload, dict):
            raise AgentGraphError(
                code="DOWNSTREAM_PROTOCOL_ERROR",
                message="目标 Agent 返回了无效拓扑响应",
                status_code=502,
            )
        data = decoded_payload.get("data")
        if (
            not isinstance(data, dict)
            or not isinstance(data.get("nodes"), list)
            or not isinstance(data.get("edges"), list)
            or not isinstance(data.get("errors"), list)
            or not any(
                isinstance(node, dict) and node.get("id") == peer.id
                for node in data.get("nodes", [])
            )
        ):
            raise AgentGraphError(
                code="DOWNSTREAM_PROTOCOL_ERROR",
                message="目标 Agent 返回了无效拓扑响应",
                status_code=502,
            )
        return data

    async def talk_to(
        self,
        msg: str,
        to_id: str,
        request_id: str | None = None,
    ) -> str:
        """向 root allowlist 中的直接 Agent 发送一条用户消息。

        参数:
            msg: 要交给目标 Agent 的用户文本；不会写入日志。
            to_id: 目标 Agent id，必须精确匹配 root 配置中的直接邻居。
            request_id: 可选调用链 UUID 字符串；省略时为本次请求生成新 UUID。

        返回值:
            目标成功响应 ``data`` 中的纯文本回复。

        异常:
            AgentGraphError: 目标不在 allowlist、下游 busy/auth/超时/连接失败、
                非成功 HTTP 状态或成功响应格式无效时抛出安全的领域异常。

        状态变化:
            获取或创建 owner=root、instructions/tools 为空的目标 ``ChatSpace``，先追加
            user 消息；远端成功后追加 assistant 回复。下游请求刻意不自动重试，因为
            一次消息可能已经触发远端模型，重试会产生重复推理和重复副作用。
        """

        peer = self._peers.get(to_id) if isinstance(to_id, str) else None
        if peer is None:
            raise AgentGraphError(
                code="PEER_NOT_ALLOWED",
                message="目标 Agent 不在 root 可见列表中",
                status_code=403,
            )

        # 每个目标拥有独立锁：同一 ChatSpace 的 user/assistant 对必须保持顺序，
        # 不同目标又不应互相阻塞，才能既保证上下文一致性又保留跨 Agent 并发。
        chat_lock = self._chat_locks.setdefault(peer.id, asyncio.Lock())
        async with chat_lock:
            chat = self.chat_spaces.get(peer.id)
            if chat is None:
                chat = ChatSpace(
                    owner_id=self.id,
                    peer_id=peer.id,
                    instructions="",
                    tools=[],
                    storage_root=self.storage_root,
                )
                self.chat_spaces[peer.id] = chat
            chat.add_msg(msg, "user")

            response = await self._post_to_peer(
                peer,
                "messages",
                {
                    "from_id": self.id,
                    "conversation_id": chat.conversation_id,
                    "message": msg,
                    "request_id": request_id or str(uuid4()),
                },
            )
            try:
                payload = response.json()
            except (json.JSONDecodeError, ValueError):
                payload = None
            data = payload.get("data") if isinstance(payload, dict) else None
            if not isinstance(data, str):
                raise AgentGraphError(
                    code="DOWNSTREAM_PROTOCOL_ERROR",
                    message="目标 Agent 返回了无效响应",
                    status_code=502,
                )
            chat.add_msg(data, "assistant")
            return data

    def get_history(self, to_id: str) -> list[dict[str, Any]]:
        """返回指定 root 会话的可读消息深拷贝。

        参数:
            to_id: 要查询的直接 Agent id；尚无本地会话时返回空列表。

        返回值:
            ``ChatSpace.messages`` 的递归深拷贝，或不存在会话时的独立空列表。

        异常:
            会话中对象无法深拷贝时传播复制异常；正常文本消息不会触发该情况。

        状态变化:
            不修改会话。调用方对返回列表及其嵌套字典的修改不会污染内部历史。
        """

        chat = self.chat_spaces.get(to_id)
        if chat is None:
            return []
        return deepcopy(chat.messages)

    async def close_chat(
        self,
        to_id: str,
        request_id: str | None = None,
    ) -> dict[str, Any]:
        """关闭 root 与一个直接 Agent 的远端及本地会话。

        参数:
            to_id: 目标 Agent id，必须位于 root 的直接 allowlist。
            request_id: 可选调用链 UUID 字符串；省略时生成新 UUID。

        返回值:
            远端成功 envelope 的 ``closed`` 与 ``saved`` 布尔状态；本地没有活动
            ChatSpace 时直接返回 ``closed=false``，因为运行时没有可安全关闭的分支 id。

        异常:
            AgentGraphError: 目标不允许、下游通信失败或成功响应结构无效时抛出。
            OSError: 远端已成功后，本地 ``ChatSpace.save`` 失败时传播，并保留本地会话。

        状态变化:
            与 ``talk_to`` 共用目标级锁。必须先确认远端关闭成功，再保存并删除本地
            ChatSpace；若先删除，本次网络失败会让仍然存在的远端上下文失去本地记录。
            无本地会话时不发送请求，避免猜测或生成会话 id 后误关其它分支。
        """

        peer = self._peers.get(to_id) if isinstance(to_id, str) else None
        if peer is None:
            raise AgentGraphError(
                code="PEER_NOT_ALLOWED",
                message="目标 Agent 不在 root 可见列表中",
                status_code=403,
            )

        chat_lock = self._chat_locks.setdefault(peer.id, asyncio.Lock())
        async with chat_lock:
            chat = self.chat_spaces.get(peer.id)
            if chat is None:
                return {"closed": False, "saved": False}
            response = await self._post_to_peer(
                peer,
                "conversations/close",
                {
                    "from_id": self.id,
                    "conversation_id": chat.conversation_id,
                    "request_id": request_id or str(uuid4()),
                },
            )
            try:
                payload = response.json()
            except (json.JSONDecodeError, ValueError):
                payload = None
            data = payload.get("data") if isinstance(payload, dict) else None
            if (
                not isinstance(data, dict)
                or not isinstance(data.get("closed"), bool)
                or not isinstance(data.get("saved"), bool)
            ):
                raise AgentGraphError(
                    code="DOWNSTREAM_PROTOCOL_ERROR",
                    message="目标 Agent 返回了无效响应",
                    status_code=502,
                )

            chat.save()
            del self.chat_spaces[peer.id]
            return {
                "closed": data["closed"],
                "saved": data["saved"],
            }

    def _sanitize_topology_node(self, node: Any) -> dict[str, Any] | None:
        if not isinstance(node, dict):
            return None
        node_id = node.get("id")
        if not isinstance(node_id, str) or _AGENT_ID.fullmatch(node_id) is None:
            return None
        sanitized: dict[str, Any] = {"id": node_id}
        bounded_text_fields = {
            "introduction": 2000,
            "host": 255,
            "protocol_version": 32,
            "status": 64,
        }
        for field, max_length in bounded_text_fields.items():
            value = node.get(field)
            if isinstance(value, str):
                sanitized[field] = value[:max_length]
        port = node.get("port")
        if isinstance(port, int) and not isinstance(port, bool) and 1 <= port <= 65535:
            sanitized["port"] = port
        return sanitized

    def _sanitize_topology_edge(self, edge: Any) -> dict[str, str] | None:
        if not isinstance(edge, dict):
            return None
        from_id = edge.get("from_id")
        to_id = edge.get("to_id")
        if (
            not isinstance(from_id, str)
            or not isinstance(to_id, str)
            or _AGENT_ID.fullmatch(from_id) is None
            or _AGENT_ID.fullmatch(to_id) is None
        ):
            return None
        return {"from_id": from_id, "to_id": to_id}

    def _sanitize_topology_error(self, error: Any) -> dict[str, str] | None:
        if not isinstance(error, dict):
            return None
        peer_id = error.get("peer_id")
        code = error.get("code")
        if not isinstance(peer_id, str) or _AGENT_ID.fullmatch(peer_id) is None:
            return None
        if not isinstance(code, str) or _ERROR_CODE.fullmatch(code) is None:
            code = "DOWNSTREAM_PARTIAL_ERROR"
        return {
            "peer_id": peer_id,
            "code": code,
            # message 来自不可信下游，固定替换以免夹带 URL、Bearer 或模型密钥。
            "message": "下游 Agent 报告局部拓扑错误",
        }

    async def discover_topology(self) -> dict[str, list]:
        """从 root 的直接邻居开始发现并合并有向 Agent 拓扑。

        参数:
            无；遍历深度和节点上限来自 root 的 ``AgentConfig``。

        返回值:
            含去重 ``nodes``、``edges`` 和局部 ``errors`` 的字典。结果始终包含
            root 节点及 root 到每个直接 peer 的可信边。

        异常:
            常见下游 busy/auth/超时/连接/协议失败不会中断整体发现，而会转换为
            ``status=unreachable`` 节点和安全的局部错误；本地编程错误仍会传播。

        状态变化:
            只对 root 配置中的直接 peer 各发起至多一次认证 topology 请求；每个
            peer 负责继续递归。返回结果只白名单保留公开字段，绝不合并 key、
            Authorization、OpenAI 字段或不可信下游错误正文。
        """

        max_nodes = self.config.topology_max_nodes
        root_node = {
            "id": self.id,
            "introduction": self.config.introduction,
            "host": self.config.host,
            "port": self.port,
            "protocol_version": _PROTOCOL_VERSION,
            "status": "reachable",
        }
        nodes: list[dict[str, Any]] = [root_node]
        node_ids = {self.id}
        node_indexes = {self.id: 0}
        node_record_ids: set[str] = set()
        edges: list[dict[str, str]] = []
        edge_keys: set[tuple[str, str]] = set()
        untrusted_edge_keys: set[tuple[str, str]] = set()
        errors: list[dict[str, str]] = []
        visited_ids = [self.id]
        visited = {self.id}
        record_budget = max_nodes
        remaining_processing_records = max_nodes * 3
        truncated = False

        def record_count() -> int:
            return (
                len(node_record_ids)
                + len(untrusted_edge_keys)
                + len(errors)
            )

        def mark_truncated() -> None:
            nonlocal truncated
            truncated = True

        def remember_visited(node_id: str) -> None:
            if node_id in visited:
                return
            if len(visited_ids) >= max_nodes:
                mark_truncated()
                return
            visited.add(node_id)
            visited_ids.append(node_id)

        def append_node(
            node: dict[str, Any],
            *,
            replace_existing: bool = False,
        ) -> None:
            node_id = node["id"]
            if node_id in node_ids:
                if replace_existing and node_id != self.id:
                    nodes[node_indexes[node_id]] = node
                return
            if len(nodes) >= max_nodes or record_count() >= record_budget:
                mark_truncated()
                return
            node_indexes[node_id] = len(nodes)
            nodes.append(node)
            node_ids.add(node_id)
            node_record_ids.add(node_id)

        def append_edge(
            edge: dict[str, str],
            *,
            trusted_direct: bool = False,
        ) -> None:
            key = (edge["from_id"], edge["to_id"])
            if key in edge_keys:
                if trusted_direct:
                    # child 可能抢先伪造 root→peer；真实配置边到达时必须升级为可信，
                    # 从统一不可信记录预算中释放该重复 key。
                    untrusted_edge_keys.discard(key)
                return
            if not trusted_direct and record_count() >= record_budget:
                mark_truncated()
                return
            edges.append(edge)
            edge_keys.add(key)
            if not trusted_direct:
                untrusted_edge_keys.add(key)

        def append_error(error: dict[str, str]) -> None:
            if record_count() >= record_budget:
                mark_truncated()
                return
            errors.append(error)

        def bounded_child_items(items: list[Any]):
            nonlocal remaining_processing_records
            allowed = min(len(items), remaining_processing_records)
            if len(items) > allowed:
                mark_truncated()
            iterator = iter(items)
            for _ in range(allowed):
                try:
                    item = next(iterator)
                except StopIteration:
                    return
                remaining_processing_records -= 1
                yield item

        for peer in self._peers.values():
            # 配置中的 root→peer 是可信拓扑事实，不受不可信响应记录预算影响。
            append_edge(
                {"from_id": self.id, "to_id": peer.id},
                trusted_direct=True,
            )
            remaining_nodes = max_nodes - len(nodes)
            if remaining_nodes <= 0 or record_count() >= record_budget:
                mark_truncated()
                continue

            try:
                data = await self._fetch_topology_from_peer(
                    peer,
                    {
                        "visited_ids": list(visited_ids),
                        "depth": 1,
                        "max_depth": self.config.topology_max_depth,
                        "max_nodes": remaining_nodes,
                    },
                )
            except AgentGraphError as error:
                remember_visited(peer.id)
                append_node(
                    {
                        "id": peer.id,
                        "introduction": "description unavailable",
                        "host": peer.ip,
                        "port": peer.port,
                        "protocol_version": _PROTOCOL_VERSION,
                        "status": "unreachable",
                    },
                    # 其它分支可能先伪造该直接 peer；真实直连失败必须覆盖其状态。
                    replace_existing=True,
                )
                append_error(
                    {
                        "peer_id": peer.id,
                        "code": error.code,
                        "message": "无法访问该 Agent 的拓扑信息",
                    }
                )
                continue

            child_nodes = data.get("nodes")
            if isinstance(child_nodes, list):
                for child_node in bounded_child_items(child_nodes):
                    sanitized_node = self._sanitize_topology_node(child_node)
                    if sanitized_node is None:
                        continue
                    remember_visited(sanitized_node["id"])
                    append_node(
                        sanitized_node,
                        # 当前 authenticated peer 对自身的声明优先于间接同 id 副本。
                        replace_existing=sanitized_node["id"] == peer.id,
                    )
            if peer.id not in node_ids:
                remember_visited(peer.id)
                append_node(
                    {
                        "id": peer.id,
                        "introduction": "description unavailable",
                        "host": peer.ip,
                        "port": peer.port,
                        "protocol_version": _PROTOCOL_VERSION,
                        "status": "reachable",
                    }
                )

            child_edges = data.get("edges")
            if isinstance(child_edges, list):
                for child_edge in bounded_child_items(child_edges):
                    sanitized_edge = self._sanitize_topology_edge(child_edge)
                    if sanitized_edge is not None:
                        append_edge(sanitized_edge)
            child_errors = data.get("errors")
            if isinstance(child_errors, list):
                for child_error in bounded_child_items(child_errors):
                    sanitized_error = self._sanitize_topology_error(child_error)
                    if sanitized_error is not None:
                        append_error(sanitized_error)

        if truncated and not any(
            error.get("code") == "TOPOLOGY_TRUNCATED" for error in errors
        ):
            if record_count() >= record_budget:
                removed_record = False
                for edge_index in range(len(edges) - 1, -1, -1):
                    edge = edges[edge_index]
                    edge_key = (edge["from_id"], edge["to_id"])
                    if edge_key not in untrusted_edge_keys:
                        continue
                    del edges[edge_index]
                    edge_keys.discard(edge_key)
                    untrusted_edge_keys.discard(edge_key)
                    removed_record = True
                    break
                if not removed_record and errors:
                    errors.pop()
                    removed_record = True
                if not removed_record:
                    for node_index in range(len(nodes) - 1, 0, -1):
                        node_id = nodes[node_index]["id"]
                        if node_id not in node_record_ids:
                            continue
                        del nodes[node_index]
                        node_ids.discard(node_id)
                        node_record_ids.discard(node_id)
                        node_indexes.pop(node_id, None)
                        for index in range(node_index, len(nodes)):
                            node_indexes[nodes[index]["id"]] = index
                        break
            errors.append(
                {
                    "peer_id": self.id,
                    "code": "TOPOLOGY_TRUNCATED",
                    "message": "拓扑结果已按安全预算截断",
                }
            )

        return {"nodes": nodes, "edges": edges, "errors": errors}

    async def _close_owned_http_client(self) -> None:
        if self._owned_http_client_closed:
            return
        self._owned_http_client_closed = True
        if self._owns_http_client:
            first_error: BaseException | None = None
            for client in self._owned_http_clients:
                try:
                    await client.aclose()
                except BaseException as error:
                    if first_error is None:
                        first_error = error
            if first_error is not None:
                raise first_error

    def create_app(self) -> FastAPI:
        """创建仅供本机前端或 CLI 使用的 root FastAPI 应用。

        参数:
            无。

        返回值:
            提供健康检查、用户消息、历史、关闭和拓扑路由，并使用 AgentGraph 统一
            ``{"data": ...}`` / ``{"error": ...}`` envelope 的 FastAPI 应用。

        异常:
            构造应用通常不抛出；请求期间领域异常和请求校验异常由统一 handler
            转换为安全 JSON，未知异常转换为 500 envelope。

        状态变化:
            应用不要求 Agent Bearer，安全前提是 root 默认只绑定 ``127.0.0.1``。
            lifespan 关闭时只关闭 ``User`` 自己创建的 HTTP 客户端，注入客户端保持
            调用方所有权。
        """

        @asynccontextmanager
        async def lifespan(app: FastAPI):
            del app
            try:
                yield
            finally:
                await self._close_owned_http_client()

        app = FastAPI(lifespan=lifespan)
        register_api_error_handlers(app)

        @app.get("/healthz")
        async def healthz() -> dict[str, Any]:
            """返回 root HTTP 服务健康状态。

            参数:
                无。
            返回值:
                统一 ``data`` envelope 中的 ``status=ok``。
            状态变化:
                不访问会话、网络或模型，仅报告当前进程可处理请求。
            """

            return {"data": {"status": "ok"}}

        @app.post("/v1/user/chats/{to_id}/messages")
        async def messages(
            to_id: str,
            body: UserMessageRequest,
        ) -> dict[str, Any]:
            """把本地用户消息转发给 root 的一个直接 peer。

            参数:
                to_id: root allowlist 中的目标 Agent id。
                body: 已校验的消息与可选 request_id。
            返回值:
                统一 ``data`` envelope 中的目标文本回复。
            状态变化:
                root API 无需 Agent Bearer，部署安全依赖默认绑定回环地址；调用
                ``talk_to`` 更新目标会话并发起一次下游请求，root 自身不调用模型。
            """

            answer = await self.talk_to(
                body.message,
                to_id,
                str(body.request_id) if body.request_id is not None else None,
            )
            return {"data": answer}

        @app.get("/v1/user/chats/{to_id}")
        async def history(to_id: str) -> dict[str, Any]:
            """读取一个 root 本地会话的深拷贝历史。

            参数:
                to_id: 要查询的直接 peer id。
            返回值:
                统一 ``data`` envelope 中的可读消息列表。
            状态变化:
                只读取内存快照，不获取聊天锁、不访问网络且不调用模型。
            """

            return {"data": self.get_history(to_id)}

        @app.delete("/v1/user/chats/{to_id}")
        async def close(to_id: str) -> dict[str, Any]:
            """请求关闭 root 与一个直接 peer 的会话。

            参数:
                to_id: root allowlist 中的目标 peer id。
            返回值:
                统一 ``data`` envelope 中的远端 closed/saved 状态。
            状态变化:
                与消息操作共用目标锁；仅在远端成功后保存并删除本地 ChatSpace。
            """

            return {"data": await self.close_chat(to_id)}

        @app.get("/v1/user/topology")
        async def topology() -> dict[str, Any]:
            """发现 root 可见的有界有向拓扑。

            参数:
                无。
            返回值:
                统一 ``data`` envelope 中的 nodes、edges 与局部 errors。
            状态变化:
                只调用直接 peer 的认证 topology endpoint；不获取聊天锁、不修改会话，
                root 自身不调用模型。
            """

            return {"data": await self.discover_topology()}

        return app
