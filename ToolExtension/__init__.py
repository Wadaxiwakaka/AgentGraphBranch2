"""Explicit catalog of trusted local extension tool classes.

This module is a trusted, explicit directory rather than a plugin discovery
mechanism.  Its tuple order determines the stable schema order presented to a
model; registration here does not enable a tool, because runtime configuration
still selects enabled extensions.  The root Agent/runtime startup path must
not import the ``ToolExtension`` package, avoiding accidental side effects and
circular dependencies; ordinary Agent entry points import this package, whose
explicit imports below define the approved implementation catalog.

Extensions that own resources should acquire them in ``startup`` and release
them in ``shutdown``.  They must not create clients, connections, or other
resources at import time, when no Agent lifecycle exists to clean them up.
"""

from tool_system.contract import AgentTool

from .agent_info import AgentInfoTool
from .get_weather import GetWeatherTool
from .search_knowledge import SearchKnowledgeTool
from .text_stats import TextStatsTool

EXTENSION_TOOLS: tuple[type[AgentTool], ...] = (
    TextStatsTool,
    AgentInfoTool,
    GetWeatherTool,
    SearchKnowledgeTool,
)

__all__ = ["EXTENSION_TOOLS"]
