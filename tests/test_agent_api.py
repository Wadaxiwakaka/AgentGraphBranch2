from __future__ import annotations

import asyncio
import importlib
import inspect
import json
import logging
from pathlib import Path
from types import SimpleNamespace
from typing import Any
from uuid import uuid4

import httpx
import pytest
from fastapi import FastAPI
from fastapi.exceptions import RequestValidationError

from AgentRemote import AgentRemote
from User import User
from core import (
    AgentConfig,
    AgentGraphError,
    ConfigError,
    PeerConfig,
    register_api_error_handlers,
)
from tests.helpers import (
    FakeMessageItem,
    FakeOpenAIClient,
    FakeOutputText,
    FakeResponse,
)
from tool_system.contract import AgentTool, ToolArguments, ToolSpec


CONVERSATION_ID = "00000000-0000-0000-0000-000000000001"
OTHER_CONVERSATION_ID = "00000000-0000-0000-0000-000000000002"


def make_agent_config(**overrides: object) -> AgentConfig:
    """构造 API 测试使用的完整普通 Agent 配置。"""

    values: dict[str, object] = {
        "id": "worker",
        "introduction": "负责 API 测试。",
        "host": "127.0.0.1",
        "port": 9100,
        "key": "worker-secret",
        "openai_baseurl": "https://models.example.test/v1",
        "openai_key": "model-secret",
        "model": "test-model",
    }
    values.update(overrides)
    return AgentConfig(**values)


def final_response(text: str, item_id: str = "msg_api") -> FakeResponse:
    """构造一个完整的最终 assistant 文本响应。"""

    return FakeResponse(
        output=[
            FakeMessageItem(
                id=item_id,
                content=[FakeOutputText(text=text)],
            )
        ],
        output_text=text,
    )


def test_registered_error_handlers_document_their_safety_contract() -> None:
    """防止统一错误 handler 的中文安全契约文档再次退化。"""

    app = FastAPI()
    register_api_error_handlers(app)

    for error_type in (AgentGraphError, RequestValidationError, Exception):
        handler = app.exception_handlers[error_type]
        documentation = inspect.getdoc(handler)
        assert documentation is not None
        for keyword in ("参数", "返回值", "异常", "安全", "状态变化"):
            assert keyword in documentation


@pytest.mark.asyncio
async def test_agent_api_exposes_health_and_authenticated_success_routes(
    tmp_path: Path,
) -> None:
    openai_client = FakeOpenAIClient([final_response("API 回复")])
    peer_http = httpx.AsyncClient(
        transport=httpx.MockTransport(lambda request: httpx.Response(500))
    )
    remote = AgentRemote(
        make_agent_config(),
        http_client=peer_http,
        openai_client=openai_client,
        storage_root=tmp_path,
    )
    app = remote.create_app()
    headers = {"Authorization": "Bearer worker-secret"}

    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app),
        base_url="http://agent.test",
    ) as client:
        health = await client.get("/healthz")
        profile = await client.get("/v1/agents/worker/profile", headers=headers)
        topology = await client.post(
            "/v1/agents/worker/topology",
            headers=headers,
            json={"visited_ids": ["root"], "depth": 0, "max_depth": 4, "max_nodes": 10},
        )
        message = await client.post(
            "/v1/agents/worker/messages",
            headers=headers,
            json={
                "from_id": "root",
                "conversation_id": CONVERSATION_ID,
                "message": "API 问题",
                "request_id": str(uuid4()),
            },
        )
        close = await client.post(
            "/v1/agents/worker/conversations/close",
            headers=headers,
            json={
                "from_id": "root",
                "conversation_id": CONVERSATION_ID,
                "request_id": str(uuid4()),
            },
        )

    await peer_http.aclose()
    assert health.status_code == 200
    assert health.json() == {"data": {"status": "ok"}}
    assert profile.status_code == 200
    assert profile.json() == {"data": remote.get_profile()}
    assert topology.status_code == 200
    assert topology.json()["data"]["nodes"][0]["id"] == "worker"
    assert message.status_code == 200
    assert message.json() == {"data": "API 回复"}
    assert close.status_code == 200
    assert close.json() == {"data": {"closed": True, "saved": True}}
    assert remote.chat_spaces == {}


@pytest.mark.asyncio
@pytest.mark.parametrize("runtime_kind", ["root", "remote"])
async def test_shared_http_boundary_consumes_unhandled_route_exception(
    tmp_path: Path,
    caplog: pytest.LogCaptureFixture,
    runtime_kind: str,
) -> None:
    sentinel = "UNHANDLED_ROUTE_SENTINEL_7F3A"
    peer_http = httpx.AsyncClient(
        transport=httpx.MockTransport(lambda _request: httpx.Response(500))
    )
    if runtime_kind == "root":
        runtime: Any = User(
            AgentConfig(
                id="root",
                introduction="本地异常边界测试",
                port=9000,
            ),
            http_client=peer_http,
            storage_root=tmp_path / "root",
        )
    else:
        runtime = AgentRemote(
            make_agent_config(),
            http_client=peer_http,
            openai_client=FakeOpenAIClient([]),
            storage_root=tmp_path / "remote",
        )
    app = runtime.create_app()

    @app.get("/test/unhandled")
    async def unhandled_route() -> None:
        raise RuntimeError(sentinel)

    caplog.set_level(logging.DEBUG)
    async with httpx.AsyncClient(
        # 使用默认 raise_app_exceptions=True，证明 sentinel 不会逃逸到调用方。
        transport=httpx.ASGITransport(app=app),
        base_url="http://agent.test",
    ) as client:
        response = await client.get("/test/unhandled")

    await peer_http.aclose()
    assert response.status_code == 500
    assert response.json() == {
        "error": {
            "code": "INTERNAL_ERROR",
            "message": "服务器内部错误",
        }
    }
    assert sentinel not in response.text
    assert sentinel not in caplog.text


@pytest.mark.asyncio
async def test_agent_public_http_handlers_and_auth_dependency_document_contract(
    tmp_path: Path,
) -> None:
    peer_http = httpx.AsyncClient(
        transport=httpx.MockTransport(lambda _request: httpx.Response(500))
    )
    remote = AgentRemote(
        make_agent_config(),
        http_client=peer_http,
        openai_client=FakeOpenAIClient([]),
        storage_root=tmp_path,
    )
    app = remote.create_app()
    expected_phrases = {
        ("/healthz", "GET"): [],
        ("/v1/agents/{target_id}/profile", "GET"): [
            "不获取聊天锁",
            "不调用模型",
        ],
        ("/v1/agents/{target_id}/messages", "POST"): [
            "ConversationKey",
            "模型",
        ],
        ("/v1/agents/{target_id}/conversations/close", "POST"): [
            "ConversationKey",
            "不调用模型",
        ],
        ("/v1/agents/{target_id}/topology", "POST"): [
            "不获取聊天锁",
            "不调用模型",
        ],
    }

    protected_route = None
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
        if path.endswith("/profile"):
            protected_route = route

    assert protected_route is not None
    authenticate = protected_route.dependant.dependencies[0].call
    auth_doc = inspect.getdoc(authenticate)
    assert auth_doc is not None
    assert "参数:" in auth_doc
    assert "返回值:" in auth_doc
    assert "状态变化:" in auth_doc
    assert "Bearer" in auth_doc
    assert "常量时间" in auth_doc
    await peer_http.aclose()


class _CountingRequestStream(httpx.AsyncByteStream):
    """记录 ASGITransport 向应用交付了多少个请求体字节块。"""

    def __init__(self, chunks: list[bytes]) -> None:
        self.chunks = chunks
        self.chunks_sent = 0

    async def __aiter__(self):
        for chunk in self.chunks:
            self.chunks_sent += 1
            yield chunk

    async def aclose(self) -> None:
        return None


def _oversized_topology_body() -> bytes:
    return json.dumps(
        {
            "visited_ids": ["root"] * 4_000,
            "depth": 0,
            "max_depth": 4,
            "max_nodes": 1,
        }
    ).encode()


@pytest.mark.asyncio
async def test_topology_api_rejects_content_length_before_reading_body(
    tmp_path: Path,
) -> None:
    peer_http = httpx.AsyncClient(
        transport=httpx.MockTransport(lambda request: httpx.Response(500))
    )
    remote = AgentRemote(
        make_agent_config(topology_max_nodes=1),
        http_client=peer_http,
        openai_client=FakeOpenAIClient([]),
        storage_root=tmp_path,
    )
    body = _oversized_topology_body()
    request_stream = _CountingRequestStream([body])

    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=remote.create_app()),
        base_url="http://agent.test",
    ) as client:
        response = await client.post(
            "/v1/agents/worker/topology",
            headers={
                "Authorization": "Bearer worker-secret",
                "Content-Type": "application/json",
                "Content-Length": str(len(body)),
            },
            content=request_stream,
        )

    await peer_http.aclose()
    assert response.status_code == 413
    assert response.json() == {
        "error": {
            "code": "TOPOLOGY_REQUEST_TOO_LARGE",
            "message": "拓扑请求体过大",
        }
    }
    assert request_stream.chunks_sent == 0


@pytest.mark.asyncio
async def test_topology_api_stops_reading_chunked_body_at_byte_limit(
    tmp_path: Path,
) -> None:
    peer_http = httpx.AsyncClient(
        transport=httpx.MockTransport(lambda request: httpx.Response(500))
    )
    remote = AgentRemote(
        make_agent_config(topology_max_nodes=1),
        http_client=peer_http,
        openai_client=FakeOpenAIClient([]),
        storage_root=tmp_path,
    )
    body = _oversized_topology_body()
    chunks = [body[index : index + 2_048] for index in range(0, len(body), 2_048)]
    request_stream = _CountingRequestStream(chunks)

    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=remote.create_app()),
        base_url="http://agent.test",
    ) as client:
        response = await client.post(
            "/v1/agents/worker/topology",
            headers={
                "Authorization": "Bearer worker-secret",
                "Content-Type": "application/json",
            },
            content=request_stream,
        )

    await peer_http.aclose()
    assert response.status_code == 413
    assert response.json() == {
        "error": {
            "code": "TOPOLOGY_REQUEST_TOO_LARGE",
            "message": "拓扑请求体过大",
        }
    }
    assert request_stream.chunks_sent < len(chunks)


@pytest.mark.asyncio
async def test_topology_body_limit_does_not_affect_message_or_profile_routes(
    tmp_path: Path,
) -> None:
    peer_http = httpx.AsyncClient(
        transport=httpx.MockTransport(lambda request: httpx.Response(500))
    )
    remote = AgentRemote(
        make_agent_config(topology_max_nodes=1),
        http_client=peer_http,
        openai_client=FakeOpenAIClient([final_response("大消息仍可处理")]),
        storage_root=tmp_path,
    )
    headers = {"Authorization": "Bearer worker-secret"}

    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=remote.create_app()),
        base_url="http://agent.test",
    ) as client:
        profile = await client.get("/v1/agents/worker/profile", headers=headers)
        message = await client.post(
            "/v1/agents/worker/messages",
            headers=headers,
            json={
                "from_id": "root",
                "conversation_id": CONVERSATION_ID,
                "message": "问" * 9_000,
                "request_id": str(uuid4()),
            },
        )

    await peer_http.aclose()
    assert profile.status_code == 200
    assert message.status_code == 200
    assert message.json() == {"data": "大消息仍可处理"}
@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("headers", "expected_status"),
    [
        ({}, 401),
        ({"Authorization": "Basic abc"}, 401),
        ({"Authorization": "Bearer wrong-secret"}, 401),
    ],
)
async def test_agent_api_rejects_missing_malformed_and_wrong_bearer(
    tmp_path: Path,
    headers: dict[str, str],
    expected_status: int,
) -> None:
    peer_http = httpx.AsyncClient(
        transport=httpx.MockTransport(lambda request: httpx.Response(500))
    )
    remote = AgentRemote(
        make_agent_config(),
        http_client=peer_http,
        openai_client=FakeOpenAIClient([]),
        storage_root=tmp_path,
    )
    app = remote.create_app()

    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app),
        base_url="http://agent.test",
    ) as client:
        response = await client.get(
            "/v1/agents/worker/profile",
            headers=headers,
        )

    await peer_http.aclose()
    assert response.status_code == expected_status
    assert response.json() == {
        "error": {"code": "AUTHENTICATION_FAILED", "message": "认证失败"}
    }
    assert response.headers["www-authenticate"] == "Bearer"


@pytest.mark.asyncio
async def test_agent_api_returns_unified_404_for_wrong_target(tmp_path: Path) -> None:
    peer_http = httpx.AsyncClient(
        transport=httpx.MockTransport(lambda request: httpx.Response(500))
    )
    remote = AgentRemote(
        make_agent_config(),
        http_client=peer_http,
        openai_client=FakeOpenAIClient([]),
        storage_root=tmp_path,
    )
    app = remote.create_app()

    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app),
        base_url="http://agent.test",
    ) as client:
        response = await client.get(
            "/v1/agents/not-worker/profile",
            headers={"Authorization": "Bearer worker-secret"},
        )

    await peer_http.aclose()
    assert response.status_code == 404
    assert response.json() == {
        "error": {"code": "TARGET_NOT_FOUND", "message": "目标 Agent 不存在"}
    }


@pytest.mark.asyncio
async def test_agent_api_converts_validation_error_without_echoing_input(
    tmp_path: Path,
) -> None:
    peer_http = httpx.AsyncClient(
        transport=httpx.MockTransport(lambda request: httpx.Response(500))
    )
    remote = AgentRemote(
        make_agent_config(),
        http_client=peer_http,
        openai_client=FakeOpenAIClient([]),
        storage_root=tmp_path,
    )
    app = remote.create_app()
    sentinel = "MESSAGE-BODY-MUST-NOT-BE-ECHOED"

    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app),
        base_url="http://agent.test",
    ) as client:
        response = await client.post(
            "/v1/agents/worker/messages",
            headers={"Authorization": "Bearer worker-secret"},
            json={
                "from_id": "root",
                "conversation_id": CONVERSATION_ID,
                "message": sentinel,
                "request_id": "not-a-uuid",
            },
        )

    await peer_http.aclose()
    assert response.status_code == 422
    payload = response.json()
    assert payload["error"]["code"] == "VALIDATION_ERROR"
    assert payload["error"]["message"] == "请求参数校验失败"
    assert isinstance(payload["error"]["details"], list)
    assert sentinel not in json.dumps(payload, ensure_ascii=False)
    assert "traceback" not in json.dumps(payload).lower()


class _BusyResponsesClient:
    """阻塞首个 API 模型调用，让第二个 caller 命中真实 busy 状态机。"""

    def __init__(self) -> None:
        self.entered = asyncio.Event()
        self.release = asyncio.Event()

    async def create(self, **kwargs: Any) -> FakeResponse:
        self.entered.set()
        await self.release.wait()
        return final_response("首个请求完成", "msg_busy")


class _BusyOpenAIClient:
    def __init__(self) -> None:
        self.responses = _BusyResponsesClient()


@pytest.mark.asyncio
async def test_agent_api_busy_uses_unified_409_and_retry_after(tmp_path: Path) -> None:
    openai_client = _BusyOpenAIClient()
    peer_http = httpx.AsyncClient(
        transport=httpx.MockTransport(lambda request: httpx.Response(500))
    )
    remote = AgentRemote(
        make_agent_config(),
        http_client=peer_http,
        openai_client=openai_client,
        storage_root=tmp_path,
    )
    app = remote.create_app()
    headers = {"Authorization": "Bearer worker-secret"}

    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app),
        base_url="http://agent.test",
    ) as client:
        first = asyncio.create_task(
            client.post(
                "/v1/agents/worker/messages",
                headers=headers,
                json={
                    "from_id": "root",
                    "conversation_id": CONVERSATION_ID,
                    "message": "占用",
                    "request_id": str(uuid4()),
                },
            )
        )
        await openai_client.responses.entered.wait()
        busy = await client.post(
            "/v1/agents/worker/messages",
            headers=headers,
            json={
                "from_id": "other",
                "conversation_id": OTHER_CONVERSATION_ID,
                "message": "插队",
                "request_id": str(uuid4()),
            },
        )
        openai_client.responses.release.set()
        first_response = await first

    await peer_http.aclose()
    assert first_response.status_code == 200
    assert busy.status_code == 409
    assert busy.headers["retry-after"] == "60"
    assert busy.json() == {
        "error": {
            "code": "AGENT_BUSY",
            "message": "该Agent正在进行其它对话，请等待1min后重试",
            "retry_after_seconds": 60,
        }
    }


class _OwnedHTTPClient:
    def __init__(self, **kwargs: Any) -> None:
        self.kwargs = kwargs
        self.aclose_calls = 0
        self.post_calls: list[dict[str, Any]] = []

    async def post(self, url: str, **kwargs: Any) -> httpx.Response:
        self.post_calls.append({"url": url, **kwargs})
        return httpx.Response(200, json={"data": "CA 请求成功"})

    async def aclose(self) -> None:
        self.aclose_calls += 1


class _OwnedOpenAIClient:
    def __init__(self, **kwargs: Any) -> None:
        self.kwargs = kwargs
        self.close_calls = 0

    async def close(self) -> None:
        self.close_calls += 1


class _LifecycleArguments(ToolArguments):
    value: str


def _lifecycle_tool(
    name: str,
    events: list[str],
    *,
    fail_startup: bool = False,
    fail_shutdown: bool = False,
) -> type[AgentTool]:
    class LifecycleTool(AgentTool):
        spec = ToolSpec(
            name=name,
            description=f"Lifecycle test tool {name}.",
            arguments_model=_LifecycleArguments,
        )

        async def startup(self) -> None:
            events.append(f"start:{name}")
            if fail_startup:
                raise RuntimeError(f"startup-secret:{name}")

        async def shutdown(self) -> None:
            events.append(f"stop:{name}")
            if fail_shutdown:
                raise RuntimeError(f"shutdown-secret:{name}")

        async def execute(
            self,
            arguments: _LifecycleArguments,
        ) -> dict[str, Any]:
            return {"ok": True, "value": arguments.value}

    return LifecycleTool


@pytest.mark.asyncio
async def test_app_lifespan_closes_only_runtime_owned_clients(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    module = importlib.import_module("AgentRemote")
    created_http: list[_OwnedHTTPClient] = []
    created_openai: list[_OwnedOpenAIClient] = []

    def make_http_client(**kwargs: Any) -> _OwnedHTTPClient:
        client = _OwnedHTTPClient(**kwargs)
        created_http.append(client)
        return client

    def make_openai_client(**kwargs: Any) -> _OwnedOpenAIClient:
        client = _OwnedOpenAIClient(**kwargs)
        created_openai.append(client)
        return client

    monkeypatch.setattr(module.httpx, "AsyncClient", make_http_client)
    monkeypatch.setattr(module, "AsyncOpenAI", make_openai_client, raising=False)

    owned_remote = AgentRemote(make_agent_config(), storage_root=tmp_path)
    owned_app = owned_remote.create_app()
    async with owned_app.router.lifespan_context(owned_app):
        pass

    assert len(created_http) == 1
    assert created_http[0].kwargs == {"timeout": 60.0, "verify": True}
    assert created_http[0].aclose_calls == 1
    assert len(created_openai) == 1
    assert created_openai[0].kwargs == {
        "base_url": "https://models.example.test/v1",
        "api_key": "model-secret",
        "timeout": 600.0,
    }
    assert created_openai[0].close_calls == 1

    injected_http = _OwnedHTTPClient()
    injected_openai = _OwnedOpenAIClient()
    injected_remote = AgentRemote(
        make_agent_config(),
        http_client=injected_http,
        openai_client=injected_openai,
        storage_root=tmp_path,
    )
    injected_app = injected_remote.create_app()
    async with injected_app.router.lifespan_context(injected_app):
        pass

    assert injected_http.aclose_calls == 0
    assert injected_openai.close_calls == 0


@pytest.mark.asyncio
async def test_owned_client_shutdown_errors_are_sanitized_and_cleanup_continues(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    module = importlib.import_module("AgentRemote")
    events: list[str] = []

    class FailingHTTPClient(_OwnedHTTPClient):
        async def aclose(self) -> None:
            await super().aclose()
            events.append("close:http")
            raise RuntimeError("http-close-secret")

    class FailingOpenAIClient(_OwnedOpenAIClient):
        async def close(self) -> None:
            await super().close()
            events.append("close:openai")
            raise RuntimeError("openai-close-secret")

    monkeypatch.setattr(module.httpx, "AsyncClient", FailingHTTPClient)
    monkeypatch.setattr(module, "AsyncOpenAI", FailingOpenAIClient, raising=False)
    remote = AgentRemote(make_agent_config(), storage_root=tmp_path)
    app = remote.create_app()

    with pytest.raises(ConfigError, match="客户端关闭失败") as exc_info:
        async with app.router.lifespan_context(app):
            pass

    assert events == ["close:openai", "close:http"]
    assert "secret" not in str(exc_info.value)
    assert exc_info.value.__cause__ is None
    assert exc_info.value.__context__ is None


@pytest.mark.asyncio
async def test_reentering_lifespan_rebuilds_runtime_owned_clients(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    module = importlib.import_module("AgentRemote")
    created_http: list[_OwnedHTTPClient] = []
    created_openai: list[_OwnedOpenAIClient] = []

    def make_http_client(**kwargs: Any) -> _OwnedHTTPClient:
        client = _OwnedHTTPClient(**kwargs)
        created_http.append(client)
        return client

    def make_openai_client(**kwargs: Any) -> _OwnedOpenAIClient:
        client = _OwnedOpenAIClient(**kwargs)
        created_openai.append(client)
        return client

    monkeypatch.setattr(module.httpx, "AsyncClient", make_http_client)
    monkeypatch.setattr(module, "AsyncOpenAI", make_openai_client, raising=False)
    remote = AgentRemote(make_agent_config(), storage_root=tmp_path)
    app = remote.create_app()

    first_http = remote._http_client
    first_openai = remote._openai_client
    async with app.router.lifespan_context(app):
        assert remote._http_client is first_http
        assert remote._openai_client is first_openai

    async with app.router.lifespan_context(app):
        assert remote._http_client is not first_http
        assert remote._openai_client is not first_openai

    assert len(created_http) == 2
    assert len(created_openai) == 2
    assert [client.aclose_calls for client in created_http] == [1, 1]
    assert [client.close_calls for client in created_openai] == [1, 1]


@pytest.mark.asyncio
async def test_owned_client_recreation_errors_are_sanitized_without_chains(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    module = importlib.import_module("AgentRemote")
    created_http: list[_OwnedHTTPClient] = []
    openai_creations = 0

    def make_http_client(**kwargs: Any) -> _OwnedHTTPClient:
        client = _OwnedHTTPClient(**kwargs)
        created_http.append(client)
        return client

    def make_openai_client(**kwargs: Any) -> _OwnedOpenAIClient:
        nonlocal openai_creations
        openai_creations += 1
        if openai_creations == 2:
            raise RuntimeError("openai-recreation-secret")
        return _OwnedOpenAIClient(**kwargs)

    monkeypatch.setattr(module.httpx, "AsyncClient", make_http_client)
    monkeypatch.setattr(module, "AsyncOpenAI", make_openai_client, raising=False)
    remote = AgentRemote(make_agent_config(), storage_root=tmp_path)
    app = remote.create_app()

    async with app.router.lifespan_context(app):
        pass

    with pytest.raises(ConfigError, match="客户端启动失败") as exc_info:
        async with app.router.lifespan_context(app):
            pass

    assert len(created_http) == 2
    assert created_http[1].aclose_calls == 1
    assert "secret" not in str(exc_info.value)
    assert exc_info.value.__cause__ is None
    assert exc_info.value.__context__ is None


@pytest.mark.asyncio
async def test_tool_startup_failure_rolls_back_and_closes_owned_clients(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    module = importlib.import_module("AgentRemote")
    events: list[str] = []

    class RecordingHTTPClient(_OwnedHTTPClient):
        async def aclose(self) -> None:
            await super().aclose()
            events.append("close:http")

    class RecordingOpenAIClient(_OwnedOpenAIClient):
        async def close(self) -> None:
            await super().close()
            events.append("close:openai")

    monkeypatch.setattr(module.httpx, "AsyncClient", RecordingHTTPClient)
    monkeypatch.setattr(module, "AsyncOpenAI", RecordingOpenAIClient, raising=False)
    first = _lifecycle_tool("first", events)
    second = _lifecycle_tool("second", events, fail_startup=True)
    remote = AgentRemote(
        make_agent_config(tools={"extensions": ["first", "second"]}),
        extension_tool_classes=(first, second),
        storage_root=tmp_path,
    )
    app = remote.create_app()

    with pytest.raises(ConfigError, match="second") as exc_info:
        async with app.router.lifespan_context(app):
            pass

    assert "startup-secret" not in str(exc_info.value)
    assert events == [
        "start:first",
        "start:second",
        "stop:first",
        "close:openai",
        "close:http",
    ]


@pytest.mark.asyncio
async def test_tool_shutdown_error_does_not_skip_other_tools_or_clients(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    module = importlib.import_module("AgentRemote")
    events: list[str] = []

    class RecordingHTTPClient(_OwnedHTTPClient):
        async def aclose(self) -> None:
            await super().aclose()
            events.append("close:http")

    class RecordingOpenAIClient(_OwnedOpenAIClient):
        async def close(self) -> None:
            await super().close()
            events.append("close:openai")

    monkeypatch.setattr(module.httpx, "AsyncClient", RecordingHTTPClient)
    monkeypatch.setattr(module, "AsyncOpenAI", RecordingOpenAIClient, raising=False)
    first = _lifecycle_tool("first", events)
    second = _lifecycle_tool("second", events, fail_shutdown=True)
    remote = AgentRemote(
        make_agent_config(tools={"extensions": ["first", "second"]}),
        extension_tool_classes=(first, second),
        storage_root=tmp_path,
    )
    app = remote.create_app()

    with pytest.raises(ConfigError, match="second") as exc_info:
        async with app.router.lifespan_context(app):
            pass

    assert "shutdown-secret" not in str(exc_info.value)
    assert events == [
        "start:first",
        "start:second",
        "stop:second",
        "stop:first",
        "close:openai",
        "close:http",
    ]


@pytest.mark.asyncio
async def test_owned_runtime_uses_peer_ca_specific_client_and_closes_it(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    module = importlib.import_module("AgentRemote")
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
    remote = AgentRemote(
        make_agent_config(agents=[peer]),
        openai_client=FakeOpenAIClient([]),
        storage_root=tmp_path,
    )

    result = await remote.send("安全请求", "secure-peer", CONVERSATION_ID)
    app = remote.create_app()
    async with app.router.lifespan_context(app):
        pass

    assert result == {
        "ok": True,
        "to_id": "secure-peer",
        "message": "CA 请求成功",
    }
    assert ca_calls == [str(ca_file)]
    assert len(created_http) == 2
    assert created_http[0].kwargs == {"timeout": 60.0, "verify": True}
    assert created_http[0].post_calls == []
    assert created_http[1].kwargs == {"timeout": 60.0, "verify": ca_context}
    assert len(created_http[1].post_calls) == 1
    assert [client.aclose_calls for client in created_http] == [1, 1]


@pytest.mark.asyncio
async def test_invalid_peer_ca_becomes_safe_downstream_error(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    module = importlib.import_module("AgentRemote")

    def make_http_client(**kwargs: Any) -> _OwnedHTTPClient:
        return _OwnedHTTPClient(**kwargs)

    def fail_ca_context(*, cafile: str) -> object:
        raise OSError(f"cannot load {cafile} with peer-secret")

    monkeypatch.setattr(module.httpx, "AsyncClient", make_http_client)
    monkeypatch.setattr(module.ssl, "create_default_context", fail_ca_context)
    peer = PeerConfig(
        id="secure-peer",
        protocol="https",
        port=9443,
        key="peer-secret",
        ca_file=tmp_path / "missing-ca.pem",
    )
    remote = AgentRemote(
        make_agent_config(agents=[peer]),
        openai_client=FakeOpenAIClient([]),
        storage_root=tmp_path,
    )

    result = await remote.send("消息", "secure-peer", CONVERSATION_ID)
    app = remote.create_app()
    async with app.router.lifespan_context(app):
        pass

    assert result == {
        "ok": False,
        "code": "DOWNSTREAM_UNAVAILABLE",
        "message": "目标 Agent 暂时不可达",
        "to_id": "secure-peer",
    }
    assert "peer-secret" not in json.dumps(result)
    assert "missing-ca" not in json.dumps(result)
