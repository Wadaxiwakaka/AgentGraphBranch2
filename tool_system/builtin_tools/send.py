"""Built-in adapter for sending a message to a visible peer Agent."""

from __future__ import annotations

from typing import Any

from pydantic import Field

from tool_system.contract import AgentTool, ToolArguments, ToolSpec


class SendArguments(ToolArguments):
    msg: str = Field(min_length=1)
    to_id: str


class SendTool(AgentTool):
    spec = ToolSpec(
        name="send",
        description="向一个直接可见的 Agent 发送消息。",
        arguments_model=SendArguments,
    )

    def is_available(self) -> bool:
        return bool(self.agent._peers)

    def parameters_schema(self) -> dict[str, Any]:
        return {
            "type": "object",
            "properties": {
                "msg": {"type": "string", "minLength": 1},
                "to_id": {
                    "type": "string",
                    "enum": list(self.agent._peers),
                },
            },
            "required": ["msg", "to_id"],
            "additionalProperties": False,
        }

    async def execute(self, arguments: SendArguments) -> dict[str, Any]:
        chat = self.require_current_chat()
        return await self.agent.send(
            arguments.msg,
            arguments.to_id,
            chat.conversation_id,
        )
