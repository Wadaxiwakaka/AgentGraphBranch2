from __future__ import annotations

import asyncio
import json
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Any
from uuid import uuid4

import httpx
import pytest

from AgentRemote import AgentRemote
from core import AgentConfig, AgentGraphError, ConversationKey, PeerConfig
from tests.helpers import (
    FakeFunctionCallItem,
    FakeMessageItem,
    FakeOpenAIClient,
    FakeOutputText,
    FakeResponse,
    HostRoutingASGITransport,
)


def _config(
    *,
    agent_id: str = "worker",
    port: int = 9100,
    key: str = "worker-secret",
    peers: list[PeerConfig] | None = None,
) -> AgentConfig:
    return AgentConfig(
        id=agent_id,
        introduction="测试会话隔离",
        port=port,
        key=key,
        openai_baseurl="http://model.test/v1",
        openai_key="model-secret",
        model="test-model",
        agents=peers or [],
    )


def _final_response(text: str, item_id: str) -> FakeResponse:
    return FakeResponse(
        output=[
            FakeMessageItem(
                id=item_id,
                content=[FakeOutputText(text=text)],
            )
        ],
        output_text=text,
    )


@asynccontextmanager
async def _started_tool_registries(*remotes: AgentRemote):
    """Run tool lifecycles without taking ownership of injected clients."""

    started: list[AgentRemote] = []
    try:
        for remote in remotes:
            await remote.tool_registry.startup()
            started.append(remote)
        yield
    finally:
        for remote in reversed(started):
            await remote.tool_registry.shutdown()


@pytest.mark.asyncio
async def test_same_caller_conversations_are_isolated_and_close_is_branch_local(
    tmp_path: Path,
) -> None:
    first_conversation_id = str(uuid4())
    second_conversation_id = str(uuid4())
    openai_client = FakeOpenAIClient(
        [
            _final_response("第一分支回复", "msg-first"),
            _final_response("第二分支回复", "msg-second"),
        ]
    )

    async with httpx.AsyncClient(
        transport=httpx.MockTransport(lambda _request: httpx.Response(500))
    ) as http_client:
        remote = AgentRemote(
            _config(),
            http_client=http_client,
            openai_client=openai_client,
            storage_root=tmp_path,
        )

        await remote.response(
            "第一分支问题",
            from_id="Agent3",
            conversation_id=first_conversation_id,
            request_id="req-first",
        )
        await remote.response(
            "第二分支问题",
            from_id="Agent3",
            conversation_id=second_conversation_id,
            request_id="req-second",
        )

        keys = {
            (key.from_id, key.conversation_id): key for key in remote.chat_spaces
        }
        assert set(keys) == {
            ("Agent3", first_conversation_id),
            ("Agent3", second_conversation_id),
        }
        first_chat = remote.chat_spaces[keys[("Agent3", first_conversation_id)]]
        second_chat = remote.chat_spaces[keys[("Agent3", second_conversation_id)]]
        assert first_chat.messages == [
            {"role": "user", "content": "第一分支问题"},
            {"role": "assistant", "content": "第一分支回复"},
        ]
        assert second_chat.messages == [
            {"role": "user", "content": "第二分支问题"},
            {"role": "assistant", "content": "第二分支回复"},
        ]
        assert first_chat.conversation_id != second_chat.conversation_id

        result = await remote.close_incoming(
            from_id="Agent3",
            conversation_id=first_conversation_id,
            request_id="close-first",
        )

    assert result == {"closed": True, "saved": True}
    assert keys[("Agent3", first_conversation_id)] not in remote.chat_spaces
    assert remote.chat_spaces[keys[("Agent3", second_conversation_id)]] is second_chat
    assert (
        tmp_path
        / "worker"
        / "Agent3"
        / f"{first_chat.conversation_id}.json"
    ).is_file()


class _BlockingResponsesClient:
    def __init__(self) -> None:
        self.entered = asyncio.Event()
        self.release = asyncio.Event()

    async def create(self, **_kwargs: Any) -> FakeResponse:
        self.entered.set()
        await self.release.wait()
        return _final_response("完成", "msg-blocking")


class _BlockingOpenAIClient:
    def __init__(self) -> None:
        self.responses = _BlockingResponsesClient()


@pytest.mark.asyncio
async def test_same_caller_different_conversation_is_immediately_busy(
    tmp_path: Path,
) -> None:
    openai_client = _BlockingOpenAIClient()
    first_conversation_id = str(uuid4())
    second_conversation_id = str(uuid4())

    async with httpx.AsyncClient(
        transport=httpx.MockTransport(lambda _request: httpx.Response(500))
    ) as http_client:
        remote = AgentRemote(
            _config(),
            http_client=http_client,
            openai_client=openai_client,
            storage_root=tmp_path,
        )
        active = asyncio.create_task(
            remote.response(
                "占用第一分支",
                from_id="Agent3",
                conversation_id=first_conversation_id,
                request_id="req-active",
            )
        )
        await openai_client.responses.entered.wait()

        with pytest.raises(AgentGraphError) as caught:
            await asyncio.wait_for(
                remote.response(
                    "第二分支不能排队",
                    from_id="Agent3",
                    conversation_id=second_conversation_id,
                    request_id="req-busy",
                ),
                timeout=0.2,
            )

        openai_client.responses.release.set()
        assert await active == "完成"

    assert caught.value.code == "AGENT_BUSY"
    assert caught.value.status_code == 409


@pytest.mark.asyncio
async def test_tool_runtime_forwards_local_conversation_id_for_send_and_close(
    tmp_path: Path,
) -> None:
    peer = PeerConfig(id="Agent4", port=9204, key="agent4-secret")
    upstream_conversation_id = str(uuid4())
    observed_payloads: dict[str, dict[str, Any]] = {}

    async def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path.endswith("/profile"):
            return httpx.Response(
                200,
                json={"data": {"id": "Agent4", "introduction": "下游节点"}},
            )
        payload = json.loads(request.content)
        if request.url.path.endswith("/messages"):
            observed_payloads["send"] = payload
            return httpx.Response(200, json={"data": "Agent4回复"})
        if request.url.path.endswith("/conversations/close"):
            observed_payloads["close"] = payload
            return httpx.Response(
                200,
                json={"data": {"closed": True, "saved": True}},
            )
        return httpx.Response(404)

    openai_client = FakeOpenAIClient(
        [
            FakeResponse(
                output=[
                    FakeFunctionCallItem(
                        id="fc-send",
                        call_id="call-send",
                        name="send",
                        arguments='{"msg":"问题","to_id":"Agent4"}',
                    )
                ],
                output_text="",
            ),
            FakeResponse(
                output=[
                    FakeFunctionCallItem(
                        id="fc-close",
                        call_id="call-close",
                        name="close",
                        arguments='{"to_id":"Agent4"}',
                    )
                ],
                output_text="",
            ),
            _final_response("完成", "msg-final"),
        ]
    )

    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
        remote = AgentRemote(
            _config(peers=[peer]),
            http_client=client,
            openai_client=openai_client,
            storage_root=tmp_path,
        )
        async with _started_tool_registries(remote):
            await remote.response(
                "请询问并关闭下游",
                from_id="Agent1",
                conversation_id=upstream_conversation_id,
                request_id="req-tools",
            )

    chat = next(iter(remote.chat_spaces.values()))
    assert observed_payloads["send"]["from_id"] == "worker"
    assert observed_payloads["close"]["from_id"] == "worker"
    assert observed_payloads["send"]["conversation_id"] == chat.conversation_id
    assert observed_payloads["close"]["conversation_id"] == chat.conversation_id
    assert chat.conversation_id != upstream_conversation_id
    send_parameters = next(
        tool["parameters"] for tool in chat.tools if tool["name"] == "send"
    )
    close_parameters = next(
        tool["parameters"] for tool in chat.tools if tool["name"] == "close"
    )
    assert set(send_parameters["properties"]) == {"msg", "to_id"}
    assert set(close_parameters["properties"]) == {"to_id"}


class _ForwardingResponsesClient:
    def __init__(self, target_id: str) -> None:
        self.target_id = target_id
        self.create_calls: list[dict[str, Any]] = []

    async def create(self, **kwargs: Any) -> FakeResponse:
        self.create_calls.append(kwargs)
        response_input = kwargs["input"]
        last_item = response_input[-1]
        call_number = len(self.create_calls)
        if last_item.get("type") == "message" and last_item.get("role") == "user":
            message = last_item["content"]
            if "关闭" in message:
                name = "close"
                arguments = json.dumps(
                    {"to_id": self.target_id},
                    ensure_ascii=False,
                )
            else:
                name = "send"
                arguments = json.dumps(
                    {"msg": f"转发:{message}", "to_id": self.target_id},
                    ensure_ascii=False,
                )
            return FakeResponse(
                output=[
                    FakeFunctionCallItem(
                        id=f"fc-{self.target_id}-{call_number}",
                        call_id=f"call-{self.target_id}-{call_number}",
                        name=name,
                        arguments=arguments,
                    )
                ],
                output_text="",
            )

        assert last_item.get("type") == "function_call_output"
        result = json.loads(last_item["output"])
        text = f"{self.target_id}结果:{result.get('message', result.get('closed'))}"
        return _final_response(text, f"msg-{self.target_id}-{call_number}")


class _ForwardingOpenAIClient:
    def __init__(self, target_id: str) -> None:
        self.responses = _ForwardingResponsesClient(target_id)


class _LeafResponsesClient:
    def __init__(self) -> None:
        self.create_calls: list[dict[str, Any]] = []

    async def create(self, **kwargs: Any) -> FakeResponse:
        self.create_calls.append(kwargs)
        user_messages = [
            item["content"]
            for item in kwargs["input"]
            if item.get("type") == "message" and item.get("role") == "user"
        ]
        assert len(user_messages) == 1
        text = f"Agent4回复:{user_messages[0]}"
        return _final_response(text, f"msg-leaf-{len(self.create_calls)}")


class _LeafOpenAIClient:
    def __init__(self) -> None:
        self.responses = _LeafResponsesClient()


@pytest.mark.asyncio
async def test_converged_four_agent_branches_stay_isolated_and_close_exactly(
    tmp_path: Path,
) -> None:
    transport = HostRoutingASGITransport()
    agent3_peer = PeerConfig(
        id="Agent3",
        ip="127.0.0.1",
        port=9303,
        key="agent3-secret",
    )
    agent4_peer = PeerConfig(
        id="Agent4",
        ip="127.0.0.1",
        port=9304,
        key="agent4-secret",
    )
    first_upstream_id = str(uuid4())
    second_upstream_id = str(uuid4())

    async with httpx.AsyncClient(transport=transport) as shared_http:
        agent1 = AgentRemote(
            _config(
                agent_id="Agent1",
                port=9301,
                key="agent1-secret",
                peers=[agent3_peer],
            ),
            http_client=shared_http,
            openai_client=_ForwardingOpenAIClient("Agent3"),
            storage_root=tmp_path,
        )
        agent2 = AgentRemote(
            _config(
                agent_id="Agent2",
                port=9302,
                key="agent2-secret",
                peers=[agent3_peer],
            ),
            http_client=shared_http,
            openai_client=_ForwardingOpenAIClient("Agent3"),
            storage_root=tmp_path,
        )
        agent3 = AgentRemote(
            _config(
                agent_id="Agent3",
                port=9303,
                key="agent3-secret",
                peers=[agent4_peer],
            ),
            http_client=shared_http,
            openai_client=_ForwardingOpenAIClient("Agent4"),
            storage_root=tmp_path,
        )
        agent4 = AgentRemote(
            _config(
                agent_id="Agent4",
                port=9304,
                key="agent4-secret",
            ),
            http_client=shared_http,
            openai_client=_LeafOpenAIClient(),
            storage_root=tmp_path,
        )
        transport.register("127.0.0.1", 9301, agent1.create_app())
        transport.register("127.0.0.1", 9302, agent2.create_app())
        transport.register("127.0.0.1", 9303, agent3.create_app())
        transport.register("127.0.0.1", 9304, agent4.create_app())

        async with _started_tool_registries(agent1, agent2, agent3, agent4):
            for target_id, port, key, message, conversation_id in (
                ("Agent1", 9301, "agent1-secret", "Agent1问题", first_upstream_id),
                ("Agent2", 9302, "agent2-secret", "Agent2问题", second_upstream_id),
            ):
                response = await shared_http.post(
                    f"http://127.0.0.1:{port}/v1/agents/{target_id}/messages",
                    headers={"Authorization": f"Bearer {key}"},
                    json={
                        "from_id": "root",
                        "conversation_id": conversation_id,
                        "message": message,
                        "request_id": str(uuid4()),
                    },
                )
                assert response.status_code == 200

            agent1_chat = agent1.chat_spaces[
                ConversationKey("root", first_upstream_id)
            ]
            agent2_chat = agent2.chat_spaces[
                ConversationKey("root", second_upstream_id)
            ]
            agent3_first_chat = agent3.chat_spaces[
                ConversationKey("Agent1", agent1_chat.conversation_id)
            ]
            agent3_second_chat = agent3.chat_spaces[
                ConversationKey("Agent2", agent2_chat.conversation_id)
            ]
            agent4_first_key = ConversationKey(
                "Agent3",
                agent3_first_chat.conversation_id,
            )
            agent4_second_key = ConversationKey(
                "Agent3",
                agent3_second_chat.conversation_id,
            )
            assert set(agent4.chat_spaces) == {agent4_first_key, agent4_second_key}
            assert agent4.chat_spaces[agent4_first_key].messages[0]["content"] == (
                "转发:转发:Agent1问题"
            )
            assert agent4.chat_spaces[agent4_second_key].messages[0]["content"] == (
                "转发:转发:Agent2问题"
            )

            close_response = await shared_http.post(
                "http://127.0.0.1:9303/v1/agents/Agent3/messages",
                headers={"Authorization": "Bearer agent3-secret"},
                json={
                    "from_id": "Agent1",
                    "conversation_id": agent1_chat.conversation_id,
                    "message": "关闭Agent4当前分支",
                    "request_id": str(uuid4()),
                },
            )
            assert close_response.status_code == 200

            assert agent4_first_key not in agent4.chat_spaces
            assert agent4_second_key in agent4.chat_spaces
