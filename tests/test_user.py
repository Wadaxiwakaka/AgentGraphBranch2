from __future__ import annotations

import asyncio
import gzip
import importlib
import inspect
import json
from pathlib import Path
from types import SimpleNamespace
from typing import Any
from uuid import uuid4

import httpx
import pytest

from core import AgentConfig, AgentGraphError, ChatSpace, PeerConfig


def _load_user_class() -> type[Any]:
    """延迟导入 User，让 RED 阶段明确指向缺失的生产模块。"""

    try:
        module = importlib.import_module("User")
    except ModuleNotFoundError:
        pytest.fail("User 模块尚未实现")
    return module.User


def make_root_config(
    *,
    peers: list[PeerConfig] | None = None,
    **overrides: object,
) -> AgentConfig:
    """构造 root User 测试所需的完整有效配置。"""

    values: dict[str, object] = {
        "id": "root",
        "introduction": "本地用户入口。",
        "host": "127.0.0.1",
        "port": 9000,
        "agents": peers or [],
    }
    values.update(overrides)
    return AgentConfig(**values)


def test_user_rejects_non_root_config(tmp_path: Path) -> None:
    user_class = _load_user_class()
    config = AgentConfig(
        id="worker",
        introduction="普通节点",
        port=9100,
        key="worker-secret",
        openai_baseurl="https://models.example.test/v1",
        openai_key="model-secret",
        model="test-model",
    )

    with pytest.raises(ValueError, match="root"):
        user_class(config, http_client=object(), storage_root=tmp_path)


@pytest.mark.asyncio
async def test_talk_to_reuses_root_chat_and_records_user_assistant_roles(
    tmp_path: Path,
) -> None:
    user_class = _load_user_class()
    peer = PeerConfig(
        id="peer-a",
        ip="10.20.30.40",
        protocol="https",
        port=9443,
        key="peer-target-secret",
    )
    requests: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        message = request.read().decode("utf-8")
        answer = "第二次回复" if "第二次问题" in message else "第一次回复"
        return httpx.Response(200, json={"data": answer})

    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
        user = user_class(
            make_root_config(peers=[peer]),
            http_client=client,
            storage_root=tmp_path,
        )
        first_request_id = str(uuid4())

        first = await user.talk_to("第一次问题", "peer-a", first_request_id)
        conversation_id = user.chat_spaces["peer-a"].conversation_id
        second = await user.talk_to("第二次问题", "peer-a")

    assert first == "第一次回复"
    assert second == "第二次回复"
    assert user.id == "root"
    assert user.chat_spaces["peer-a"].conversation_id == conversation_id
    chat = user.chat_spaces["peer-a"]
    assert chat.owner_id == "root"
    assert chat.peer_id == "peer-a"
    assert chat.instructions == ""
    assert chat.tools == []
    assert chat.messages == [
        {"role": "user", "content": "第一次问题"},
        {"role": "assistant", "content": "第一次回复"},
        {"role": "user", "content": "第二次问题"},
        {"role": "assistant", "content": "第二次回复"},
    ]
    assert len(requests) == 2
    assert requests[0].method == "POST"
    assert requests[0].url == httpx.URL(
        "https://10.20.30.40:9443/v1/agents/peer-a/messages"
    )
    assert requests[0].headers["Authorization"] == "Bearer peer-target-secret"
    assert json.loads(requests[0].content) == {
        "from_id": "root",
        "conversation_id": conversation_id,
        "message": "第一次问题",
        "request_id": first_request_id,
    }
    assert json.loads(requests[1].content)["conversation_id"] == conversation_id


@pytest.mark.asyncio
async def test_talk_to_rejects_non_allowlisted_peer_without_network_call(
    tmp_path: Path,
) -> None:
    user_class = _load_user_class()
    calls = 0

    def handler(_request: httpx.Request) -> httpx.Response:
        nonlocal calls
        calls += 1
        return httpx.Response(200, json={"data": "unexpected"})

    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
        user = user_class(
            make_root_config(),
            http_client=client,
            storage_root=tmp_path,
        )
        with pytest.raises(AgentGraphError) as caught:
            await user.talk_to("不应发送", "peer-a")

    assert caught.value.code == "PEER_NOT_ALLOWED"
    assert caught.value.status_code == 403
    assert user.chat_spaces == {}
    assert calls == 0


@pytest.mark.asyncio
async def test_same_peer_talks_are_serialized_and_keep_context_order(
    tmp_path: Path,
) -> None:
    user_class = _load_user_class()
    peer = PeerConfig(id="peer-a", ip="127.0.0.1", port=9201, key="peer-secret")
    first_started = asyncio.Event()
    release_first = asyncio.Event()
    starts: list[str] = []

    async def handler(request: httpx.Request) -> httpx.Response:
        message = json.loads(request.content)["message"]
        starts.append(message)
        if message == "第一条":
            first_started.set()
            await release_first.wait()
        return httpx.Response(200, json={"data": f"回复:{message}"})

    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
        user = user_class(
            make_root_config(peers=[peer]),
            http_client=client,
            storage_root=tmp_path,
        )
        first_task = asyncio.create_task(user.talk_to("第一条", "peer-a"))
        await asyncio.wait_for(first_started.wait(), timeout=1)
        second_task = asyncio.create_task(user.talk_to("第二条", "peer-a"))
        try:
            await asyncio.sleep(0)
            await asyncio.sleep(0)
            assert starts == ["第一条"]
        finally:
            release_first.set()
            await asyncio.gather(first_task, second_task, return_exceptions=True)

    assert await first_task == "回复:第一条"
    assert await second_task == "回复:第二条"
    assert user.chat_spaces["peer-a"].messages == [
        {"role": "user", "content": "第一条"},
        {"role": "assistant", "content": "回复:第一条"},
        {"role": "user", "content": "第二条"},
        {"role": "assistant", "content": "回复:第二条"},
    ]


@pytest.mark.asyncio
async def test_different_peer_talks_can_overlap_without_context_pollution(
    tmp_path: Path,
) -> None:
    user_class = _load_user_class()
    peers = [
        PeerConfig(id="peer-a", ip="127.0.0.1", port=9201, key="secret-a"),
        PeerConfig(id="peer-b", ip="127.0.0.1", port=9202, key="secret-b"),
    ]
    started: set[str] = set()
    both_started = asyncio.Event()

    async def handler(request: httpx.Request) -> httpx.Response:
        target_id = request.url.path.split("/")[3]
        started.add(target_id)
        if len(started) == 2:
            both_started.set()
        await asyncio.wait_for(both_started.wait(), timeout=1)
        return httpx.Response(200, json={"data": f"来自 {target_id}"})

    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
        user = user_class(
            make_root_config(peers=peers),
            http_client=client,
            storage_root=tmp_path,
        )
        results = await asyncio.gather(
            user.talk_to("给 A", "peer-a"),
            user.talk_to("给 B", "peer-b"),
        )

    assert results == ["来自 peer-a", "来自 peer-b"]
    assert user.chat_spaces["peer-a"].messages == [
        {"role": "user", "content": "给 A"},
        {"role": "assistant", "content": "来自 peer-a"},
    ]
    assert user.chat_spaces["peer-b"].messages == [
        {"role": "user", "content": "给 B"},
        {"role": "assistant", "content": "来自 peer-b"},
    ]


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("status_code", "expected_code", "expected_status"),
    [
        (409, "AGENT_BUSY", 409),
        (401, "AUTHENTICATION_FAILED", 502),
        (403, "AUTHENTICATION_FAILED", 502),
    ],
)
async def test_talk_to_converts_busy_and_auth_to_safe_agent_graph_errors(
    tmp_path: Path,
    status_code: int,
    expected_code: str,
    expected_status: int,
) -> None:
    user_class = _load_user_class()
    peer = PeerConfig(
        id="peer-a",
        ip="downstream.internal.test",
        port=9201,
        key="never-leak-peer-secret",
    )
    calls = 0

    def handler(_request: httpx.Request) -> httpx.Response:
        nonlocal calls
        calls += 1
        return httpx.Response(
            status_code,
            json={"error": {"retry_after_seconds": 7}},
        )

    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
        user = user_class(
            make_root_config(peers=[peer]),
            http_client=client,
            storage_root=tmp_path,
        )
        with pytest.raises(AgentGraphError) as caught:
            await user.talk_to("敏感消息正文", "peer-a")

    error = caught.value
    assert error.code == expected_code
    assert error.status_code == expected_status
    assert error.retry_after_seconds == (7.0 if status_code == 409 else None)
    assert calls == 1
    serialized = repr(error.__dict__) + str(error)
    assert "never-leak-peer-secret" not in serialized
    assert "downstream.internal.test" not in serialized
    assert "敏感消息正文" not in serialized


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("raised", "expected_code", "expected_status"),
    [
        (httpx.ReadTimeout("slow peer"), "DOWNSTREAM_TIMEOUT", 504),
        (httpx.ConnectError("cannot connect"), "DOWNSTREAM_UNAVAILABLE", 502),
    ],
)
async def test_talk_to_converts_transport_errors_without_retrying(
    tmp_path: Path,
    raised: Exception,
    expected_code: str,
    expected_status: int,
) -> None:
    user_class = _load_user_class()
    peer = PeerConfig(id="peer-a", ip="127.0.0.1", port=9201, key="peer-secret")
    calls = 0

    def handler(_request: httpx.Request) -> httpx.Response:
        nonlocal calls
        calls += 1
        raise raised

    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
        user = user_class(
            make_root_config(peers=[peer]),
            http_client=client,
            storage_root=tmp_path,
        )
        with pytest.raises(AgentGraphError) as caught:
            await user.talk_to("只发送一次", "peer-a")

    assert caught.value.code == expected_code
    assert caught.value.status_code == expected_status
    assert calls == 1
    assert user.chat_spaces["peer-a"].messages == [
        {"role": "user", "content": "只发送一次"}
    ]


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "response",
    [
        httpx.Response(200, json={"data": {"unexpected": True}}),
        httpx.Response(200, content=b"not-json"),
    ],
)
async def test_talk_to_rejects_invalid_success_payload(
    tmp_path: Path,
    response: httpx.Response,
) -> None:
    user_class = _load_user_class()
    peer = PeerConfig(id="peer-a", ip="127.0.0.1", port=9201, key="peer-secret")

    async with httpx.AsyncClient(
        transport=httpx.MockTransport(lambda _request: response)
    ) as client:
        user = user_class(
            make_root_config(peers=[peer]),
            http_client=client,
            storage_root=tmp_path,
        )
        with pytest.raises(AgentGraphError) as caught:
            await user.talk_to("格式测试", "peer-a")

    assert caught.value.code == "DOWNSTREAM_PROTOCOL_ERROR"
    assert caught.value.status_code == 502
    assert user.chat_spaces["peer-a"].messages == [
        {"role": "user", "content": "格式测试"}
    ]


@pytest.mark.asyncio
async def test_get_history_returns_empty_or_deep_copied_messages(
    tmp_path: Path,
) -> None:
    user_class = _load_user_class()
    peer = PeerConfig(id="peer-a", ip="127.0.0.1", port=9201, key="peer-secret")

    async with httpx.AsyncClient(
        transport=httpx.MockTransport(
            lambda _request: httpx.Response(200, json={"data": "回复"})
        )
    ) as client:
        user = user_class(
            make_root_config(peers=[peer]),
            http_client=client,
            storage_root=tmp_path,
        )
        assert user.get_history("peer-a") == []
        await user.talk_to("问题", "peer-a")

    history = user.get_history("peer-a")
    history[0]["content"] = "被调用方修改"
    history.append({"role": "assistant", "content": "伪造"})

    assert user.get_history("peer-a") == [
        {"role": "user", "content": "问题"},
        {"role": "assistant", "content": "回复"},
    ]


@pytest.mark.asyncio
async def test_close_chat_calls_remote_before_saving_and_deleting_local_chat(
    tmp_path: Path,
) -> None:
    user_class = _load_user_class()
    peer = PeerConfig(id="peer-a", ip="127.0.0.1", port=9201, key="peer-secret")
    request_id = str(uuid4())
    observed_payloads: list[dict[str, Any]] = []
    user: Any

    def handler(request: httpx.Request) -> httpx.Response:
        assert request.url.path == "/v1/agents/peer-a/conversations/close"
        assert "peer-a" in user.chat_spaces
        assert list(tmp_path.rglob("*.json")) == []
        observed_payloads.append(json.loads(request.content))
        return httpx.Response(200, json={"data": {"closed": True, "saved": True}})

    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
        user = user_class(
            make_root_config(peers=[peer]),
            http_client=client,
            storage_root=tmp_path,
        )
        chat = ChatSpace("root", "peer-a", storage_root=tmp_path)
        chat.add_msg("待归档", "user")
        user.chat_spaces["peer-a"] = chat

        result = await user.close_chat("peer-a", request_id)

    assert result == {"closed": True, "saved": True}
    assert observed_payloads == [
        {
            "from_id": "root",
            "conversation_id": chat.conversation_id,
            "request_id": request_id,
        }
    ]
    assert "peer-a" not in user.chat_spaces
    saved_files = list(tmp_path.rglob("*.json"))
    assert len(saved_files) == 1
    assert json.loads(saved_files[0].read_text(encoding="utf-8"))["messages"] == [
        {"role": "user", "content": "待归档"}
    ]


@pytest.mark.asyncio
async def test_close_chat_preserves_local_chat_when_remote_fails(
    tmp_path: Path,
) -> None:
    user_class = _load_user_class()
    peer = PeerConfig(id="peer-a", ip="127.0.0.1", port=9201, key="peer-secret")

    async with httpx.AsyncClient(
        transport=httpx.MockTransport(lambda _request: httpx.Response(503))
    ) as client:
        user = user_class(
            make_root_config(peers=[peer]),
            http_client=client,
            storage_root=tmp_path,
        )
        chat = ChatSpace("root", "peer-a", storage_root=tmp_path)
        chat.add_msg("必须保留", "user")
        user.chat_spaces["peer-a"] = chat

        with pytest.raises(AgentGraphError) as caught:
            await user.close_chat("peer-a")

    assert caught.value.code == "DOWNSTREAM_ERROR"
    assert user.chat_spaces["peer-a"] is chat
    assert list(tmp_path.rglob("*.json")) == []


@pytest.mark.asyncio
async def test_close_chat_is_locally_idempotent_without_unknown_remote_branch(
    tmp_path: Path,
) -> None:
    user_class = _load_user_class()
    peer = PeerConfig(id="peer-a", ip="127.0.0.1", port=9201, key="peer-secret")
    calls = 0

    def handler(_request: httpx.Request) -> httpx.Response:
        nonlocal calls
        calls += 1
        return httpx.Response(
            200,
            json={"data": {"closed": False, "saved": False}},
        )

    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
        user = user_class(
            make_root_config(peers=[peer]),
            http_client=client,
            storage_root=tmp_path,
        )
        result = await user.close_chat("peer-a")

    assert result == {"closed": False, "saved": False}
    assert calls == 0
    assert user.chat_spaces == {}


@pytest.mark.asyncio
async def test_discover_topology_merges_deduplicates_and_keeps_partial_failures(
    tmp_path: Path,
) -> None:
    user_class = _load_user_class()
    peers = [
        PeerConfig(id="peer-a", ip="127.0.0.1", port=9201, key="secret-a"),
        PeerConfig(id="peer-b", ip="127.0.0.1", port=9202, key="secret-b"),
    ]
    requests: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        target_id = request.url.path.split("/")[3]
        if target_id == "peer-b":
            raise httpx.ConnectError("private full URL must not escape")
        return httpx.Response(
            200,
            json={
                "data": {
                    "nodes": [
                        {
                            "id": "peer-a",
                            "introduction": "直接节点 A",
                            "host": "127.0.0.1",
                            "port": 9201,
                            "protocol_version": "1.0",
                            "status": "reachable",
                            "key": "child-node-secret",
                        },
                        {
                            "id": "child-c",
                            "introduction": "下游节点 C",
                            "host": "10.0.0.3",
                            "port": 9303,
                            "status": "reachable",
                            "openai_key": "model-secret",
                        },
                        {"id": "child-c", "introduction": "重复节点"},
                        {
                            "id": "peer-b",
                            "introduction": "由其它分支伪造的可达状态",
                            "status": "reachable",
                        },
                    ],
                    "edges": [
                        {"from_id": "root", "to_id": "peer-a"},
                        {"from_id": "peer-a", "to_id": "child-c"},
                        {"from_id": "peer-a", "to_id": "child-c"},
                        {"from_id": "bad id", "to_id": "child-c"},
                    ],
                    "errors": [
                        {
                            "peer_id": "child-c",
                            "code": "CHILD_BUSY",
                            "message": "secret-a https://private.example.test/path",
                            "Authorization": "Bearer leaked",
                        }
                    ],
                    "key": "envelope-secret",
                }
            },
        )

    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
        user = user_class(
            make_root_config(peers=peers),
            http_client=client,
            storage_root=tmp_path,
        )
        result = await user.discover_topology()

    assert [node["id"] for node in result["nodes"]] == [
        "root",
        "peer-a",
        "child-c",
        "peer-b",
    ]
    assert result["edges"] == [
        {"from_id": "root", "to_id": "peer-a"},
        {"from_id": "peer-a", "to_id": "child-c"},
        {"from_id": "root", "to_id": "peer-b"},
    ]
    assert result["errors"] == [
        {
            "peer_id": "child-c",
            "code": "CHILD_BUSY",
            "message": "下游 Agent 报告局部拓扑错误",
        },
        {
            "peer_id": "peer-b",
            "code": "DOWNSTREAM_UNAVAILABLE",
            "message": "无法访问该 Agent 的拓扑信息",
        },
    ]
    assert result["nodes"][-1]["status"] == "unreachable"
    assert len(requests) == 2
    for request, peer in zip(requests, peers, strict=True):
        assert request.method == "POST"
        assert request.url.path == f"/v1/agents/{peer.id}/topology"
        assert request.headers["Accept-Encoding"] == "identity"
        assert request.headers["Authorization"] == (
            f"Bearer {peer.key.get_secret_value()}"
        )
    assert json.loads(requests[0].content) == {
        "visited_ids": ["root"],
        "depth": 1,
        "max_depth": user.config.topology_max_depth,
        "max_nodes": user.config.topology_max_nodes - 1,
    }
    assert json.loads(requests[1].content) == {
        "visited_ids": ["root", "peer-a", "child-c", "peer-b"],
        "depth": 1,
        "max_depth": user.config.topology_max_depth,
        "max_nodes": user.config.topology_max_nodes - 4,
    }
    serialized = json.dumps(result, ensure_ascii=False)
    for forbidden in (
        "secret-a",
        "secret-b",
        "child-node-secret",
        "model-secret",
        "envelope-secret",
        "Authorization",
        "openai_key",
        "private.example.test",
    ):
        assert forbidden not in serialized


@pytest.mark.asyncio
async def test_root_topology_enforces_one_global_node_and_record_budget(
    tmp_path: Path,
) -> None:
    user_class = _load_user_class()
    peers = [
        PeerConfig(id="peer-a", ip="127.0.0.1", port=9201, key="secret-a"),
        PeerConfig(id="peer-b", ip="127.0.0.1", port=9202, key="secret-b"),
    ]

    def handler(request: httpx.Request) -> httpx.Response:
        peer_id = request.url.path.split("/")[3]
        return httpx.Response(
            200,
            json={
                "data": {
                    "nodes": [
                        {"id": peer_id, "status": "reachable"},
                        {"id": f"{peer_id}-child-1", "status": "reachable"},
                        {"id": f"{peer_id}-child-2", "status": "reachable"},
                    ],
                    "edges": [
                        {"from_id": peer_id, "to_id": f"{peer_id}-child-1"},
                        {"from_id": peer_id, "to_id": f"{peer_id}-child-2"},
                    ],
                    "errors": [
                        {
                            "peer_id": peer_id,
                            "code": "CHILD_PARTIAL",
                            "message": "untrusted",
                        }
                    ],
                }
            },
        )

    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
        user = user_class(
            make_root_config(peers=peers, topology_max_nodes=2),
            http_client=client,
            storage_root=tmp_path,
        )
        result = await user.discover_topology()

    assert len(result["nodes"]) <= 2
    assert {tuple(edge.values()) for edge in result["edges"]} >= {
        ("root", "peer-a"),
        ("root", "peer-b"),
    }
    trusted_direct = {
        ("root", "peer-a"),
        ("root", "peer-b"),
    }
    untrusted_edges = [
        edge
        for edge in result["edges"]
        if (edge["from_id"], edge["to_id"]) not in trusted_direct
    ]
    untrusted_record_count = (
        max(0, len(result["nodes"]) - 1)
        + len(untrusted_edges)
        + len(result["errors"])
    )
    assert untrusted_record_count <= 2
    assert any(
        error["code"] == "TOPOLOGY_TRUNCATED" for error in result["errors"]
    )


@pytest.mark.asyncio
async def test_root_topology_shares_visited_and_remaining_nodes_across_peers(
    tmp_path: Path,
) -> None:
    user_class = _load_user_class()
    peers = [
        PeerConfig(id="peer-a", ip="127.0.0.1", port=9201, key="secret-a"),
        PeerConfig(id="peer-b", ip="127.0.0.1", port=9202, key="secret-b"),
    ]
    payloads: dict[str, dict[str, Any]] = {}

    def handler(request: httpx.Request) -> httpx.Response:
        peer_id = request.url.path.split("/")[3]
        payloads[peer_id] = json.loads(request.content)
        if peer_id == "peer-a":
            return httpx.Response(
                200,
                json={
                    "data": {
                        "nodes": [
                            {"id": "peer-a", "status": "reachable"},
                            {"id": "shared-child", "status": "reachable"},
                        ],
                        "edges": [],
                        "errors": [],
                    }
                },
            )
        return httpx.Response(
            200,
            json={
                "data": {
                    "nodes": [{"id": "peer-b", "status": "reachable"}],
                    "edges": [],
                    "errors": [],
                }
            },
        )

    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
        user = user_class(
            make_root_config(peers=peers, topology_max_nodes=5),
            http_client=client,
            storage_root=tmp_path,
        )
        result = await user.discover_topology()

    assert payloads["peer-a"] == {
        "visited_ids": ["root"],
        "depth": 1,
        "max_depth": user.config.topology_max_depth,
        "max_nodes": 4,
    }
    assert payloads["peer-b"] == {
        "visited_ids": ["root", "peer-a", "shared-child"],
        "depth": 1,
        "max_depth": user.config.topology_max_depth,
        "max_nodes": 2,
    }
    assert len(result["nodes"]) <= 5


class _CountingTopologyStream(httpx.AsyncByteStream):
    """按块交付 topology body，并记录生产代码实际消费的块数。"""

    def __init__(self, chunks: list[bytes]) -> None:
        self.chunks = chunks
        self.read_count = 0

    async def __aiter__(self):
        for chunk in self.chunks:
            self.read_count += 1
            yield chunk

    async def aclose(self) -> None:
        return None


@pytest.mark.asyncio
async def test_root_topology_stops_oversized_stream_before_json_and_continues(
    tmp_path: Path,
) -> None:
    user_class = _load_user_class()
    peers = [
        PeerConfig(id="peer-a", ip="127.0.0.1", port=9201, key="secret-a"),
        PeerConfig(id="peer-b", ip="127.0.0.1", port=9202, key="secret-b"),
    ]
    oversized_body = json.dumps(
        {
            "data": {
                "nodes": [{"id": "peer-a", "status": "reachable"}],
                "edges": [],
                "errors": [],
                "padding": "x" * 120_000,
            }
        }
    ).encode("utf-8")
    chunks = [oversized_body[index : index + 16_000] for index in range(0, len(oversized_body), 16_000)]
    oversized_stream = _CountingTopologyStream(chunks)
    requests: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        peer_id = request.url.path.split("/")[3]
        if peer_id == "peer-a":
            return httpx.Response(
                200,
                headers={"Content-Type": "application/json"},
                stream=oversized_stream,
            )
        return httpx.Response(
            200,
            json={
                "data": {
                    "nodes": [{"id": "peer-b", "status": "reachable"}],
                    "edges": [],
                    "errors": [],
                }
            },
        )

    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
        user = user_class(
            make_root_config(peers=peers, topology_max_nodes=4),
            http_client=client,
            storage_root=tmp_path,
        )
        result = await user.discover_topology()

    assert oversized_stream.read_count < len(chunks)
    assert all(request.headers["Accept-Encoding"] == "identity" for request in requests)
    assert any(node["id"] == "peer-b" for node in result["nodes"])
    assert any(
        error["peer_id"] == "peer-a"
        and error["code"] == "TOPOLOGY_RESPONSE_TOO_LARGE"
        for error in result["errors"]
    )


@pytest.mark.asyncio
async def test_root_topology_rejects_compression_before_read_and_continues(
    tmp_path: Path,
) -> None:
    user_class = _load_user_class()
    peers = [
        PeerConfig(id="peer-a", ip="127.0.0.1", port=9201, key="secret-a"),
        PeerConfig(id="peer-b", ip="127.0.0.1", port=9202, key="secret-b"),
    ]
    compressed_body = gzip.compress(
        json.dumps(
            {
                "data": {
                    "nodes": [{"id": "peer-a", "status": "reachable"}],
                    "edges": [],
                    "errors": [],
                }
            }
        ).encode("utf-8")
    )
    compressed_stream = _CountingTopologyStream([compressed_body])
    requests: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        peer_id = request.url.path.split("/")[3]
        if peer_id == "peer-a":
            return httpx.Response(
                200,
                headers={
                    "Content-Type": "application/json",
                    "Content-Encoding": "gzip",
                },
                stream=compressed_stream,
            )
        return httpx.Response(
            200,
            json={
                "data": {
                    "nodes": [{"id": "peer-b", "status": "reachable"}],
                    "edges": [],
                    "errors": [],
                }
            },
        )

    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
        user = user_class(
            make_root_config(peers=peers, topology_max_nodes=5),
            http_client=client,
            storage_root=tmp_path,
        )
        result = await user.discover_topology()

    assert compressed_stream.read_count == 0
    assert all(request.headers["Accept-Encoding"] == "identity" for request in requests)
    assert any(node["id"] == "peer-b" for node in result["nodes"])
    assert any(
        error["peer_id"] == "peer-a"
        and error["code"] == "TOPOLOGY_UNSUPPORTED_CONTENT_ENCODING"
        for error in result["errors"]
    )


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "data",
    [
        {},
        {"nodes": [], "edges": []},
        {"nodes": [], "errors": []},
        {"edges": [], "errors": []},
        {"nodes": {}, "edges": [], "errors": []},
        {"nodes": [], "edges": {}, "errors": []},
        {"nodes": [], "edges": [], "errors": {}},
    ],
    ids=[
        "all-missing",
        "errors-missing",
        "edges-missing",
        "nodes-missing",
        "nodes-not-list",
        "edges-not-list",
        "errors-not-list",
    ],
)
async def test_user_topology_fetch_rejects_incomplete_success_data(
    tmp_path: Path,
    data: dict[str, Any],
) -> None:
    user_class = _load_user_class()
    peer = PeerConfig(
        id="peer-a",
        ip="127.0.0.1",
        port=9201,
        key="test-peer-key",
    )

    async with httpx.AsyncClient(
        transport=httpx.MockTransport(
            lambda _request: httpx.Response(200, json={"data": data})
        )
    ) as client:
        user = user_class(
            make_root_config(peers=[peer]),
            http_client=client,
            storage_root=tmp_path,
        )
        with pytest.raises(AgentGraphError) as caught:
            await user._fetch_topology_from_peer(
                peer,
                {
                    "visited_ids": ["root"],
                    "depth": 1,
                    "max_depth": 4,
                    "max_nodes": 10,
                },
            )

    assert caught.value.code == "DOWNSTREAM_PROTOCOL_ERROR"
    assert "test-peer-key" not in repr(caught.value.__dict__)


@pytest.mark.asyncio
async def test_user_topology_missing_direct_self_rejects_batch_and_overrides_forgery(
    tmp_path: Path,
) -> None:
    user_class = _load_user_class()
    peers = [
        PeerConfig(id="peer-a", ip="127.0.0.1", port=9201, key="key-a"),
        PeerConfig(id="peer-b", ip="127.0.0.1", port=9202, key="key-b"),
        PeerConfig(id="peer-c", ip="127.0.0.1", port=9203, key="key-c"),
    ]
    calls: list[str] = []

    def handler(request: httpx.Request) -> httpx.Response:
        peer_id = request.url.path.split("/")[3]
        calls.append(peer_id)
        if peer_id == "peer-a":
            return httpx.Response(
                200,
                json={
                    "data": {
                        "nodes": [
                            {"id": "peer-a", "status": "reachable"},
                            {
                                "id": "peer-b",
                                "status": "reachable",
                                "introduction": "indirect forged status",
                            },
                        ],
                        "edges": [],
                        "errors": [],
                    }
                },
            )
        if peer_id == "peer-b":
            return httpx.Response(
                200,
                json={
                    "data": {
                        "nodes": [
                            {"id": "poisoned-child", "status": "reachable"}
                        ],
                        "edges": [
                            {"from_id": "peer-b", "to_id": "poisoned-child"}
                        ],
                        "errors": [
                            {
                                "peer_id": "poisoned-child",
                                "code": "POISONED_ERROR",
                                "message": "must not merge",
                            }
                        ],
                    }
                },
            )
        return httpx.Response(
            200,
            json={
                "data": {
                    "nodes": [{"id": "peer-c", "status": "reachable"}],
                    "edges": [],
                    "errors": [],
                }
            },
        )

    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
        user = user_class(
            make_root_config(peers=peers, topology_max_nodes=20),
            http_client=client,
            storage_root=tmp_path,
        )
        result = await user.discover_topology()

    nodes_by_id = {node["id"]: node for node in result["nodes"]}
    assert calls == ["peer-a", "peer-b", "peer-c"]
    assert nodes_by_id["peer-b"]["status"] == "unreachable"
    assert "poisoned-child" not in nodes_by_id
    assert {tuple(edge.values()) for edge in result["edges"]} == {
        ("root", "peer-a"),
        ("root", "peer-b"),
        ("root", "peer-c"),
    }
    assert any(
        error["peer_id"] == "peer-b"
        and error["code"] == "DOWNSTREAM_PROTOCOL_ERROR"
        for error in result["errors"]
    )
    assert not any(
        error["peer_id"] == "poisoned-child" for error in result["errors"]
    )
    assert nodes_by_id["peer-c"]["status"] == "reachable"


@pytest.mark.asyncio
async def test_root_api_exposes_chat_close_history_and_topology_envelopes(
    tmp_path: Path,
) -> None:
    user_class = _load_user_class()
    peer = PeerConfig(id="peer-a", ip="127.0.0.1", port=9201, key="peer-secret")

    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path.endswith("/messages"):
            return httpx.Response(200, json={"data": "API 回复"})
        if request.url.path.endswith("/conversations/close"):
            return httpx.Response(
                200,
                json={"data": {"closed": True, "saved": True}},
            )
        if request.url.path.endswith("/topology"):
            return httpx.Response(
                200,
                json={
                    "data": {
                        "nodes": [{"id": "peer-a", "status": "reachable"}],
                        "edges": [],
                        "errors": [],
                    }
                },
            )
        raise AssertionError(f"unexpected downstream path: {request.url.path}")

    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as peer_http:
        user = user_class(
            make_root_config(peers=[peer]),
            http_client=peer_http,
            storage_root=tmp_path,
        )
        app = user.create_app()
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=app),
            base_url="http://root.test",
        ) as client:
            health = await client.get("/healthz")
            message = await client.post(
                "/v1/user/chats/peer-a/messages",
                json={"message": "API 问题", "request_id": str(uuid4())},
            )
            history = await client.get("/v1/user/chats/peer-a")
            topology = await client.get("/v1/user/topology")
            close = await client.delete("/v1/user/chats/peer-a")
            empty_history = await client.get("/v1/user/chats/peer-a")

    assert health.status_code == 200
    assert health.json() == {"data": {"status": "ok"}}
    assert message.status_code == 200
    assert message.json() == {"data": "API 回复"}
    assert history.json() == {
        "data": [
            {"role": "user", "content": "API 问题"},
            {"role": "assistant", "content": "API 回复"},
        ]
    }
    assert topology.status_code == 200
    assert topology.json()["data"]["nodes"][0]["id"] == "root"
    assert close.json() == {"data": {"closed": True, "saved": True}}
    assert empty_history.json() == {"data": []}


@pytest.mark.asyncio
async def test_root_api_uses_unified_domain_and_validation_error_envelopes(
    tmp_path: Path,
) -> None:
    user_class = _load_user_class()
    async with httpx.AsyncClient(
        transport=httpx.MockTransport(lambda _request: httpx.Response(500))
    ) as peer_http:
        user = user_class(
            make_root_config(),
            http_client=peer_http,
            storage_root=tmp_path,
        )
        app = user.create_app()
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=app),
            base_url="http://root.test",
        ) as client:
            disallowed = await client.post(
                "/v1/user/chats/not-visible/messages",
                json={"message": "不应发送"},
            )
            invalid = await client.post(
                "/v1/user/chats/not-visible/messages",
                json={
                    "message": "do-not-echo-user-body",
                    "request_id": "do-not-echo-invalid-request-id",
                },
            )

    assert disallowed.status_code == 403
    assert disallowed.json() == {
        "error": {
            "code": "PEER_NOT_ALLOWED",
            "message": "目标 Agent 不在 root 可见列表中",
        }
    }
    assert invalid.status_code == 422
    invalid_payload = invalid.json()
    assert invalid_payload["error"]["code"] == "VALIDATION_ERROR"
    assert invalid_payload["error"]["message"] == "请求参数校验失败"
    assert invalid_payload["error"]["details"]
    serialized = json.dumps(invalid_payload, ensure_ascii=False)
    assert "do-not-echo-user-body" not in serialized
    assert "do-not-echo-invalid-request-id" not in serialized


@pytest.mark.asyncio
async def test_root_app_lifespan_closes_only_user_owned_http_client(
    tmp_path: Path,
) -> None:
    user_class = _load_user_class()
    owned_user = user_class(make_root_config(), storage_root=tmp_path / "owned")
    owned_app = owned_user.create_app()

    assert not owned_user._http_client.is_closed
    async with owned_app.router.lifespan_context(owned_app):
        assert not owned_user._http_client.is_closed
    assert owned_user._http_client.is_closed

    injected_client = httpx.AsyncClient(
        transport=httpx.MockTransport(lambda _request: httpx.Response(500))
    )
    injected_user = user_class(
        make_root_config(),
        http_client=injected_client,
        storage_root=tmp_path / "injected",
    )
    injected_app = injected_user.create_app()

    async with injected_app.router.lifespan_context(injected_app):
        assert not injected_client.is_closed
    assert not injected_client.is_closed
    await injected_client.aclose()


@pytest.mark.asyncio
async def test_root_public_http_handlers_document_contract_in_chinese(
    tmp_path: Path,
) -> None:
    user_class = _load_user_class()
    peer_http = httpx.AsyncClient(
        transport=httpx.MockTransport(lambda _request: httpx.Response(500))
    )
    user = user_class(
        make_root_config(),
        http_client=peer_http,
        storage_root=tmp_path,
    )
    app = user.create_app()
    expected_phrases = {
        ("/healthz", "GET"): [],
        ("/v1/user/chats/{to_id}/messages", "POST"): [
            "无需 Agent Bearer",
            "回环地址",
            "root 自身不调用模型",
        ],
        ("/v1/user/chats/{to_id}", "GET"): ["不获取聊天锁", "不调用模型"],
        ("/v1/user/chats/{to_id}", "DELETE"): ["远端成功后", "本地"],
        ("/v1/user/topology", "GET"): ["不获取聊天锁", "不调用模型"],
    }

    for (path, method), phrases in expected_phrases.items():
        route = next(
            route
            for route in app.routes
            if getattr(route, "path", None) == path
            and method in getattr(route, "methods", set())
        )
        doc = inspect.getdoc(route.endpoint)
        assert doc is not None
        assert "参数:" in doc
        assert "返回值:" in doc
        assert "状态变化:" in doc
        for phrase in phrases:
            assert phrase in doc

    await peer_http.aclose()


class _OwnedHTTPClient:
    """记录 User 自建 HTTP 客户端的 TLS 参数、请求和关闭次数。"""

    def __init__(self, **kwargs: Any) -> None:
        self.kwargs = kwargs
        self.post_calls: list[dict[str, Any]] = []
        self.aclose_calls = 0

    async def post(self, url: str, **kwargs: Any) -> httpx.Response:
        self.post_calls.append({"url": url, **kwargs})
        return httpx.Response(200, json={"data": "CA 回复"})

    async def aclose(self) -> None:
        self.aclose_calls += 1


@pytest.mark.asyncio
async def test_owned_user_uses_peer_ca_specific_client_and_closes_all_clients(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    module = importlib.import_module("User")
    created_http: list[_OwnedHTTPClient] = []
    ca_context = object()
    ca_calls: list[str] = []

    def make_http_client(**kwargs: Any) -> _OwnedHTTPClient:
        client = _OwnedHTTPClient(**kwargs)
        created_http.append(client)
        return client

    def make_ca_context(*, cafile: str) -> object:
        ca_calls.append(cafile)
        return ca_context

    monkeypatch.setattr(module.httpx, "AsyncClient", make_http_client)
    if hasattr(module, "ssl"):
        monkeypatch.setattr(module.ssl, "create_default_context", make_ca_context)
    else:
        monkeypatch.setattr(
            module,
            "ssl",
            SimpleNamespace(create_default_context=make_ca_context),
            raising=False,
        )
    ca_file = tmp_path / "peer-ca.pem"
    peer = PeerConfig(
        id="secure-peer",
        ip="secure-peer.local",
        protocol="https",
        port=9443,
        key="peer-secret",
        ca_file=ca_file,
    )
    user = module.User(make_root_config(peers=[peer]), storage_root=tmp_path)

    result = await user.talk_to("安全请求", "secure-peer")
    app = user.create_app()
    async with app.router.lifespan_context(app):
        pass

    assert result == "CA 回复"
    assert ca_calls == [str(ca_file)]
    assert len(created_http) == 2
    assert created_http[0].kwargs == {"timeout": 60.0, "verify": True}
    assert created_http[0].post_calls == []
    assert created_http[1].kwargs == {"timeout": 60.0, "verify": ca_context}
    assert len(created_http[1].post_calls) == 1
    assert [client.aclose_calls for client in created_http] == [1, 1]


@pytest.mark.asyncio
async def test_invalid_peer_ca_becomes_safe_agent_graph_error(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    module = importlib.import_module("User")

    def make_http_client(**kwargs: Any) -> _OwnedHTTPClient:
        return _OwnedHTTPClient(**kwargs)

    def fail_ca_context(*, cafile: str) -> object:
        raise OSError(f"cannot load {cafile} with peer-secret")

    monkeypatch.setattr(module.httpx, "AsyncClient", make_http_client)
    if hasattr(module, "ssl"):
        monkeypatch.setattr(module.ssl, "create_default_context", fail_ca_context)
    else:
        monkeypatch.setattr(
            module,
            "ssl",
            SimpleNamespace(create_default_context=fail_ca_context),
            raising=False,
        )
    peer = PeerConfig(
        id="secure-peer",
        protocol="https",
        port=9443,
        key="peer-secret",
        ca_file=tmp_path / "missing-ca.pem",
    )
    user = module.User(make_root_config(peers=[peer]), storage_root=tmp_path)

    with pytest.raises(AgentGraphError) as caught:
        await user.talk_to("消息", "secure-peer")
    app = user.create_app()
    async with app.router.lifespan_context(app):
        pass

    assert caught.value.code == "DOWNSTREAM_UNAVAILABLE"
    serialized = repr(caught.value.__dict__) + str(caught.value)
    assert "peer-secret" not in serialized
    assert "missing-ca" not in serialized
