"""Built-in adapter for closing a conversation with a visible peer Agent."""

from __future__ import annotations

from typing import Any

from tool_system.contract import AgentTool, ToolArguments, ToolSpec


class CloseArguments(ToolArguments):
    to_id: str


class CloseTool(AgentTool):
    spec = ToolSpec(
        name="close",
        description="仅关闭当前 Agent 与一个直接可见 Agent 的远端会话。",
        arguments_model=CloseArguments,
    )

    def is_available(self) -> bool:
        return bool(self.agent._peers)

    def parameters_schema(self) -> dict[str, Any]:
        return {
            "type": "object",
            "properties": {
                "to_id": {
                    "type": "string",
                    "enum": list(self.agent._peers),
                },
            },
            "required": ["to_id"],
            "additionalProperties": False,
        }

    async def execute(self, arguments: CloseArguments) -> dict[str, Any]:
        chat = self.require_current_chat()
        return await self.agent.close(
            arguments.to_id,
            chat.conversation_id,
        )
