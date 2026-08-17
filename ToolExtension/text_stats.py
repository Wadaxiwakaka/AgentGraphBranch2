"""A teaching extension that returns JSON-safe statistics for supplied text.

This module demonstrates the complete shape of a small extension: a strict
``ToolArguments`` parameter model, a model-visible ``ToolSpec``, and an
``AgentTool`` instance that is created separately for each Agent.  Its result
contains only JSON-safe primitive values, so the registry can return it
directly to the model.

``character_count`` intentionally uses Python ``len(text)``: it counts Unicode
code points, not user-perceived grapheme clusters or UTF-8 bytes.  Words
intentionally mean whitespace-separated tokens, matching ``str.split``. Lines
intentionally use ``str.splitlines``; therefore a trailing newline does not
create an additional empty line.  The tool owns no network connection, file,
or other resource, so it needs no lifecycle hooks beyond AgentTool's default
``startup`` and ``shutdown`` implementations.
"""

from __future__ import annotations

from pydantic import Field

from tool_system.contract import AgentTool, ToolArguments, ToolSpec


class TextStatsArguments(ToolArguments):
    """Strict input accepted by the text statistics tool."""

    text: str = Field(min_length=1, max_length=10_000)


class TextStatsTool(AgentTool):
    """Compute deterministic, local statistics for one supplied text value."""

    spec = ToolSpec(
        name="text_stats",
        description="统计文本的字符、非空白字符、空白分词和行段数量。",
        arguments_model=TextStatsArguments,
    )

    async def execute(self, arguments: TextStatsArguments) -> dict[str, int | bool]:
        """Return primitive counts that remain safe to serialize as JSON."""

        text = arguments.text
        return {
            "ok": True,
            "character_count": len(text),
            "non_whitespace_character_count": sum(
                not character.isspace() for character in text
            ),
            "word_count": len(text.split()),
            "line_count": len(text.splitlines()),
        }
