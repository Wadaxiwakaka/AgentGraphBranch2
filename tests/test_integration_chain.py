from __future__ import annotations

import json
from contextlib import AsyncExitStack
from pathlib import Path
from typing import Any
from uuid import uuid4

import httpx
import pytest

from AgentRemote import AgentRemote
from User import User
from core import AgentConfig, ConversationKey, load_agent_config
from tests.helpers import HostRoutingASGITransport, ThreeNodeFakeOpenAIClient


PROJECT_ROOT = Path(__file__).resolve().parents[1]
CONFIG_ROOT = PROJECT_ROOT / "agents_setting"
USER_QUESTION = "请询问Agent2在干嘛"
AGENT2_QUESTION = "你在干嘛？"
AGENT2_REPLY = "我正在整理今天的任务。"
FINAL_REPLY = "Agent2说它正在整理今天的任务。"
SEND_CALL_ID = "call_agent2_status"


def _load_example_configs(monkeypatch: pytest.MonkeyPatch) -> dict[str, AgentConfig]:
    """加载三个公开示例配置，并用测试占位值完成环境变量展开。"""

    monkeypatch.setenv("OPENAI_API_KEY", "integration-placeholder-key")
    return {
        agent_id: load_agent_config(CONFIG_ROOT / f"{agent_id}.json")
        for agent_id in ("root", "Agent1", "Agent2")
    }


def _assert_example_config_contract(configs: dict[str, AgentConfig]) -> None:
    """断言示例只形成 root→Agent1→Agent2 的本地有向链。"""

    root = configs["root"]
    agent1 = configs["Agent1"]
    agent2 = configs["Agent2"]

    assert (root.id, root.host, root.port) == ("root", "127.0.0.1", 9860)
    assert [(peer.id, peer.ip, peer.port) for peer in root.agents] == [
        ("Agent1", "127.0.0.1", 9861)
    ]
    assert root.agents[0].key.get_secret_value() == "dev-agent1-key"

    assert (agent1.id, agent1.host, agent1.port) == (
        "Agent1",
        "127.0.0.1",
        9861,
    )
    assert agent1.key is not None
    assert agent1.key.get_secret_value() == "dev-agent1-key"
    assert [(peer.id, peer.ip, peer.port) for peer in agent1.agents] == [
        ("Agent2", "127.0.0.1", 9862)
    ]
    assert agent1.agents[0].key.get_secret_value() == "dev-agent2-key"

    assert (agent2.id, agent2.host, agent2.port) == (
        "Agent2",
        "127.0.0.1",
        9862,
    )
    assert agent2.key is not None
    assert agent2.key.get_secret_value() == "dev-agent2-key"
    assert agent2.agents == []

    for config in (agent1, agent2):
        assert config.openai_baseurl == "http://localhost:20128/v1"
        assert config.openai_key is not None
        assert config.openai_key.get_secret_value() == "integration-placeholder-key"
        assert config.model == "gpt-5.6-luna"
        source = (CONFIG_ROOT / f"{config.id}.json").read_text(encoding="utf-8")
        assert '"openai_key": "${OPENAI_API_KEY}"' in source
        assert "integration-placeholder-key" not in source


def _context_types(context: list[dict[str, Any]]) -> list[str]:
    """提取 Responses 上下文的 item 类型，便于断言跨节点顺序。"""

    return [item["type"] for item in context]


@pytest.mark.asyncio
async def test_root_agent1_agent2_chain_uses_real_http_contracts_and_closes_cleanly(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    configs = _load_example_configs(monkeypatch)
    _assert_example_config_contract(configs)

    transport = HostRoutingASGITransport()
    agent1_openai = ThreeNodeFakeOpenAIClient("Agent1")
    agent2_openai = ThreeNodeFakeOpenAIClient("Agent2")

    async with AsyncExitStack() as stack:
        shared_http = await stack.enter_async_context(
            httpx.AsyncClient(transport=transport)
        )
        root = User(
            configs["root"],
            http_client=shared_http,
            storage_root=tmp_path,
        )
        agent1 = AgentRemote(
            configs["Agent1"],
            http_client=shared_http,
            openai_client=agent1_openai,
            storage_root=tmp_path,
        )
        agent2 = AgentRemote(
            configs["Agent2"],
            http_client=shared_http,
            openai_client=agent2_openai,
            storage_root=tmp_path,
        )
        root_app = root.create_app()
        agent1_app = agent1.create_app()
        agent2_app = agent2.create_app()
        await stack.enter_async_context(root_app.router.lifespan_context(root_app))
        await stack.enter_async_context(
            agent1_app.router.lifespan_context(agent1_app)
        )
        await stack.enter_async_context(
            agent2_app.router.lifespan_context(agent2_app)
        )
        transport.register("127.0.0.1", 9860, root_app)
        transport.register("127.0.0.1", 9861, agent1_app)
        transport.register("127.0.0.1", 9862, agent2_app)

        message_response = await shared_http.post(
            "http://127.0.0.1:9860/v1/user/chats/Agent1/messages",
            json={"message": USER_QUESTION, "request_id": str(uuid4())},
        )

        assert message_response.status_code == 200
        assert message_response.json() == {"data": FINAL_REPLY}
        assert {
            "method": "POST",
            "host": "127.0.0.1",
            "port": 9861,
            "path": "/v1/agents/Agent1/messages",
        } in transport.calls
        assert {
            "method": "POST",
            "host": "127.0.0.1",
            "port": 9862,
            "path": "/v1/agents/Agent2/messages",
        } in transport.calls
        assert all(
            set(call) == {"method", "host", "port", "path"}
            for call in transport.calls
        )

        assert len(agent1_openai.responses.create_calls) == 2
        second_input = agent1_openai.responses.create_calls[1]["input"]
        tool_output = next(
            item
            for item in second_input
            if item.get("type") == "function_call_output"
        )
        assert tool_output["call_id"] == SEND_CALL_ID
        assert AGENT2_REPLY in tool_output["output"]
        assert json.loads(tool_output["output"]) == {
            "ok": True,
            "to_id": "Agent2",
            "message": AGENT2_REPLY,
        }

        root_chat = root.chat_spaces["Agent1"]
        agent1_key = ConversationKey("root", root_chat.conversation_id)
        agent1_chat = agent1.chat_spaces[agent1_key]
        agent2_key = ConversationKey("Agent1", agent1_chat.conversation_id)
        agent2_chat = agent2.chat_spaces[agent2_key]
        assert root_chat.messages == [
            {"role": "user", "content": USER_QUESTION},
            {"role": "assistant", "content": FINAL_REPLY},
        ]
        assert _context_types(root_chat.context_items) == ["message", "message"]
        assert [item["role"] for item in root_chat.context_items] == [
            "user",
            "assistant",
        ]
        assert agent1_chat.messages == [
            {"role": "user", "content": USER_QUESTION},
            {"role": "assistant", "content": FINAL_REPLY},
        ]
        assert _context_types(agent1_chat.context_items) == [
            "message",
            "function_call",
            "function_call_output",
            "message",
        ]
        assert agent1_chat.context_items[0]["role"] == "user"
        assert agent1_chat.context_items[1]["call_id"] == SEND_CALL_ID
        assert agent1_chat.context_items[2]["call_id"] == SEND_CALL_ID
        assert agent1_chat.context_items[3]["role"] == "assistant"
        assert agent2_chat.messages == [
            {"role": "user", "content": AGENT2_QUESTION},
            {"role": "assistant", "content": AGENT2_REPLY},
        ]
        assert _context_types(agent2_chat.context_items) == ["message", "message"]
        assert [item["role"] for item in agent2_chat.context_items] == [
            "user",
            "assistant",
        ]
        assert agent1.currentChatSpace is None
        assert agent2.currentChatSpace is None

        topology_response = await shared_http.get(
            "http://127.0.0.1:9860/v1/user/topology"
        )
        assert topology_response.status_code == 200
        topology = topology_response.json()["data"]
        assert {node["id"] for node in topology["nodes"]} == {
            "root",
            "Agent1",
            "Agent2",
        }
        assert {
            (edge["from_id"], edge["to_id"])
            for edge in topology["edges"]
        } == {("root", "Agent1"), ("Agent1", "Agent2")}
        serialized_topology = json.dumps(topology, ensure_ascii=False)
        for forbidden in (
            "key",
            "openai_key",
            "dev-agent1-key",
            "dev-agent2-key",
        ):
            assert forbidden not in serialized_topology

        root_conversation_id = root_chat.conversation_id
        agent1_conversation_id = agent1_chat.conversation_id
        close_response = await shared_http.delete(
            "http://127.0.0.1:9860/v1/user/chats/Agent1"
        )

        assert close_response.status_code == 200
        assert close_response.json() == {
            "data": {"closed": True, "saved": True}
        }
        assert "Agent1" not in root.chat_spaces
        assert agent1_key not in agent1.chat_spaces

    saved_paths = {
        "root": (
            "Agent1",
            root_conversation_id,
            tmp_path
            / "root"
            / "Agent1"
            / f"{root_conversation_id}.json",
        ),
        "Agent1": (
            "root",
            agent1_conversation_id,
            tmp_path
            / "Agent1"
            / "root"
            / f"{agent1_conversation_id}.json",
        ),
    }
    for owner_id, (peer_id, conversation_id, saved_path) in saved_paths.items():
        payload = json.loads(saved_path.read_text(encoding="utf-8"))
        assert payload["owner_id"] == owner_id
        assert payload["peer_id"] == peer_id
        assert payload["conversation_id"] == conversation_id
