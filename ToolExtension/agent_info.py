"""A teaching extension that returns a deliberately minimal Agent identity.

The empty ``ToolArguments`` model makes the public call shape explicit while
still rejecting undeclared JSON fields.  ``ToolSpec`` publishes that schema,
and the registry constructs this ``AgentTool`` once per Agent, rather than
sharing Agent state across instances.  The result is composed only of
JSON-safe primitives.

``AgentRemote.get_profile`` already establishes a redaction boundary, but this
tool applies a second, positive whitelist before exposing anything externally.
It must never return a whole ``model_dump`` of configuration or profile data:
that could disclose keys, model URLs, peers, clients, sessions, or future
sensitive fields added to those objects.
"""

from __future__ import annotations

from typing import Any

from tool_system.contract import AgentTool, ToolArguments, ToolSpec


class AgentInfoArguments(ToolArguments):
    """No caller-provided values are needed for the public identity summary."""


class AgentInfoTool(AgentTool):
    """Expose only the public identity fields intentionally selected below."""

    spec = ToolSpec(
        name="agent_info",
        description="返回当前 Agent 的公开身份摘要。",
        arguments_model=AgentInfoArguments,
    )

    async def execute(self, arguments: AgentInfoArguments) -> dict[str, Any]:
        """Return the explicit safe subset of the current Agent profile."""

        profile = self.agent.get_profile()
        return {
            "ok": True,
            "agent_id": profile["id"],
            "introduction": profile["introduction"],
        }
