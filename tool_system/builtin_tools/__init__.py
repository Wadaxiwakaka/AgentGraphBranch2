"""Explicit catalog of model tools built into every ordinary Agent."""

from tool_system.contract import AgentTool

from .close import CloseTool
from .send import SendTool

BUILTIN_TOOLS: tuple[type[AgentTool], ...] = (
    SendTool,
    CloseTool,
)

__all__ = ["BUILTIN_TOOLS", "CloseTool", "SendTool"]
