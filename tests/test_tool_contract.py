from __future__ import annotations

from dataclasses import FrozenInstanceError
from types import SimpleNamespace
from typing import Any

import pytest
from pydantic import ValidationError

from tool_system.contract import AgentTool, ToolArguments, ToolSpec, ToolStateError


class ExampleArguments(ToolArguments):
    count: int


class ExampleTool(AgentTool):
    spec = ToolSpec(
        name="example",
        description="Example tool.",
        arguments_model=ExampleArguments,
    )

    async def execute(self, arguments: ExampleArguments) -> dict[str, Any]:
        return {"count": arguments.count}


def test_tool_arguments_forbid_extra_fields() -> None:
    with pytest.raises(ValidationError):
        ExampleArguments.model_validate({"count": 1, "extra": True})


def test_tool_spec_is_frozen_and_uses_slots() -> None:
    spec = ExampleTool.spec

    assert not hasattr(spec, "__dict__")
    with pytest.raises(FrozenInstanceError):
        spec.name = "changed"  # type: ignore[misc]


@pytest.mark.asyncio
async def test_agent_tool_defaults_and_current_chat_helper() -> None:
    agent = SimpleNamespace(currentChatSpace=object())
    tool = ExampleTool(agent)

    assert tool.agent is agent
    assert tool.is_available() is True
    assert tool.parameters_schema() == ExampleArguments.model_json_schema()
    assert tool.require_current_chat() is agent.currentChatSpace
    assert await tool.startup() is None
    assert await tool.shutdown() is None


def test_require_current_chat_raises_stable_state_error() -> None:
    tool = ExampleTool(SimpleNamespace(currentChatSpace=None))

    with pytest.raises(ToolStateError, match="活动会话"):
        tool.require_current_chat()
