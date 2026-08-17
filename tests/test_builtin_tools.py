from __future__ import annotations

from types import SimpleNamespace
from typing import Any

import pytest

from tool_system.builtin_tools.close import CloseArguments, CloseTool
from tool_system.builtin_tools.send import SendArguments, SendTool
from tool_system.contract import ToolStateError


class _FakeAgent:
    def __init__(self, peer_ids: list[str]) -> None:
        self._peers = {peer_id: object() for peer_id in peer_ids}
        self.currentChatSpace: Any | None = None
        self.calls: list[tuple[Any, ...]] = []

    async def send(
        self,
        msg: str,
        to_id: str,
        conversation_id: str,
    ) -> dict[str, Any]:
        self.calls.append(("send", msg, to_id, conversation_id))
        return {"ok": True, "to_id": to_id, "message": "sent"}

    async def close(
        self,
        to_id: str,
        conversation_id: str,
    ) -> dict[str, Any]:
        self.calls.append(("close", to_id, conversation_id))
        return {"ok": True, "to_id": to_id, "closed": True, "saved": True}


def test_builtin_tools_hide_when_agent_has_no_peers() -> None:
    agent = _FakeAgent([])

    assert SendTool(agent).is_available() is False
    assert CloseTool(agent).is_available() is False


def test_send_tool_preserves_existing_parameters_schema_exactly() -> None:
    agent = _FakeAgent(["peer-a", "peer-b"])

    assert SendTool(agent).parameters_schema() == {
        "type": "object",
        "properties": {
            "msg": {"type": "string", "minLength": 1},
            "to_id": {"type": "string", "enum": ["peer-a", "peer-b"]},
        },
        "required": ["msg", "to_id"],
        "additionalProperties": False,
    }


def test_close_tool_preserves_existing_parameters_schema_exactly() -> None:
    agent = _FakeAgent(["peer-a", "peer-b"])

    assert CloseTool(agent).parameters_schema() == {
        "type": "object",
        "properties": {
            "to_id": {"type": "string", "enum": ["peer-a", "peer-b"]},
        },
        "required": ["to_id"],
        "additionalProperties": False,
    }


@pytest.mark.asyncio
async def test_send_tool_uses_current_chat_conversation_id() -> None:
    agent = _FakeAgent(["peer-a"])
    tool = SendTool(agent)
    agent.currentChatSpace = SimpleNamespace(conversation_id="conversation-local")

    result = await tool.execute(SendArguments(msg="hello", to_id="peer-a"))

    assert result == {"ok": True, "to_id": "peer-a", "message": "sent"}
    assert agent.calls == [("send", "hello", "peer-a", "conversation-local")]


@pytest.mark.asyncio
async def test_close_tool_uses_current_chat_conversation_id() -> None:
    agent = _FakeAgent(["peer-a"])
    tool = CloseTool(agent)
    agent.currentChatSpace = SimpleNamespace(conversation_id="conversation-local")

    result = await tool.execute(CloseArguments(to_id="peer-a"))

    assert result == {
        "ok": True,
        "to_id": "peer-a",
        "closed": True,
        "saved": True,
    }
    assert agent.calls == [("close", "peer-a", "conversation-local")]


@pytest.mark.asyncio
async def test_builtin_tool_requires_an_active_chat() -> None:
    agent = _FakeAgent(["peer-a"])

    with pytest.raises(ToolStateError, match="活动会话"):
        await SendTool(agent).execute(SendArguments(msg="hello", to_id="peer-a"))
