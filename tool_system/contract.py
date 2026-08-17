"""Extensible tool contracts shared by AgentGraph tool implementations."""

from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, ClassVar

from pydantic import BaseModel, ConfigDict

if TYPE_CHECKING:
    from AgentRemote import AgentRemote
    from core import ChatSpace


class ToolStateError(RuntimeError):
    """Raised when a tool or registry is used in an invalid runtime state."""


class ToolArguments(BaseModel):
    """Base model for tool arguments; undeclared model input is forbidden."""

    model_config = ConfigDict(extra="forbid")


@dataclass(frozen=True, slots=True)
class ToolSpec:
    """Immutable declaration of a model-visible tool."""

    name: str
    description: str
    arguments_model: type[ToolArguments]


class AgentTool(ABC):
    """Base class for a tool instance bound to one Agent instance."""

    spec: ClassVar[ToolSpec]

    def __init__(self, agent: AgentRemote) -> None:
        self.agent = agent

    def is_available(self) -> bool:
        return True

    def parameters_schema(self) -> dict[str, Any]:
        return self.spec.arguments_model.model_json_schema()

    async def startup(self) -> None:
        return None

    async def shutdown(self) -> None:
        return None

    def require_current_chat(self) -> ChatSpace:
        chat = self.agent.currentChatSpace
        if chat is None:
            raise ToolStateError("工具执行时不存在活动会话")
        return chat

    @abstractmethod
    async def execute(self, arguments: ToolArguments) -> dict[str, Any]:
        raise NotImplementedError
