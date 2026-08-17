# Example Extension Tools Implementation Plan

> [!IMPORTANT]
> **历史计划说明（2026-07-19）：** 本文记录最初实现 `text_stats` 与 `agent_info`
> 两个无网络教学工具时的执行计划，不代表当前扩展目录的完整现状。当前实现已经追加
> 资源型网络教学工具 `get_weather`；最新工具目录、生命周期、精确 schema 和测试要求以
> `docs/superpowers/specs/2026-07-19-example-extension-tools-design.md` 及当前代码为准。

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add fully commented `text_stats` and `agent_info` teaching extensions that remain disabled unless a normal Agent explicitly selects them.

**Architecture:** Each example lives in its own `ToolExtension` module and implements the existing `AgentTool` contract. `ToolExtension.__init__` remains the only registration point; `ToolRegistry` continues to own selection, strict validation, execution, and lifecycle.

**Tech Stack:** Python 3.12+, Pydantic 2, pytest, pytest-asyncio, existing `tool_system` contracts.

## Global Constraints

- Add no dependencies and do not change `uv.lock`.
- Keep both examples deterministic and free of network, file, subprocess, and module-import side effects.
- Existing configs that omit `tools` or use `[]`/`"none"` must not expose either example.
- `agent_info` may return only the current Agent's id and introduction; never serialize config/runtime objects.
- Tool names remain case-sensitive and must match `[A-Za-z0-9_-]{1,64}`.
- Do not modify or reveal existing user changes under `agents_setting`.

---

### Task 1: Lock the teaching contracts with failing tests

**Files:**
- Create: `tests/test_example_extension_tools.py`

**Interfaces:**
- Consumes: `ToolRegistry.build(...)`, `ToolRegistry.dispatch(...)`, `ToolArguments`, `AgentTool`.
- Produces: executable expectations for `TextStatsTool`, `AgentInfoTool`, and `EXTENSION_TOOLS`.

- [ ] **Step 1: Write catalog and selection tests**

Create tests that import the not-yet-created classes and assert:

```python
assert EXTENSION_TOOLS == (TextStatsTool, AgentInfoTool)
assert schema_names(enabled="none") == []
assert schema_names(enabled=["agent_info", "text_stats"]) == [
    "text_stats",
    "agent_info",
]
assert schema_names(enabled="all") == ["text_stats", "agent_info"]
```

- [ ] **Step 2: Write strict schema and execution tests**

Use a fake Agent whose `get_profile()` contains extra public fields and whose attributes contain conspicuous secret markers. Assert:

```python
assert await dispatch("text_stats", '{"text":"你好 world\nnext"}') == {
    "ok": True,
    "character_count": 13,
    "non_whitespace_character_count": 11,
    "word_count": 3,
    "line_count": 2,
}
assert await dispatch("agent_info", "{}") == {
    "ok": True,
    "agent_id": "teacher",
    "introduction": "教学 Agent",
}
```

Also assert empty text, non-string text, and extra fields return `INVALID_TOOL_ARGUMENTS`, and serialized `agent_info` output contains none of the fake keys, URLs, host/port, conversation id, or peer data.

- [ ] **Step 3: Run the new tests and verify RED**

Run:

```powershell
.\.venv\Scripts\python.exe -m pytest tests\test_example_extension_tools.py -q -p no:cacheprovider
```

Expected: collection fails because `ToolExtension.text_stats` and `ToolExtension.agent_info` do not exist.

---

### Task 2: Implement and register both examples

**Files:**
- Create: `ToolExtension/text_stats.py`
- Create: `ToolExtension/agent_info.py`
- Modify: `ToolExtension/__init__.py`
- Test: `tests/test_example_extension_tools.py`

**Interfaces:**
- Produces: `TextStatsArguments`, `TextStatsTool`, `AgentInfoArguments`, `AgentInfoTool`, and ordered `EXTENSION_TOOLS`.

- [ ] **Step 1: Implement `text_stats` with teaching comments**

Use this behavior:

```python
class TextStatsArguments(ToolArguments):
    text: str = Field(min_length=1, max_length=10_000)


class TextStatsTool(AgentTool):
    spec = ToolSpec(
        name="text_stats",
        description="统计文本的字符、非空白字符、空白分词和行段数量。",
        arguments_model=TextStatsArguments,
    )

    async def execute(self, arguments: TextStatsArguments) -> dict[str, Any]:
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
```

Comments must explain the parameter model, required fields, JSON-safe return values, whitespace word semantics, `splitlines()` trailing-newline behavior, and why this tool needs no lifecycle hooks.

- [ ] **Step 2: Implement `agent_info` with an explicit whitelist**

```python
class AgentInfoArguments(ToolArguments):
    pass


class AgentInfoTool(AgentTool):
    spec = ToolSpec(
        name="agent_info",
        description="返回当前 Agent 的公开身份摘要。",
        arguments_model=AgentInfoArguments,
    )

    async def execute(self, arguments: AgentInfoArguments) -> dict[str, Any]:
        del arguments
        profile = self.agent.get_profile()
        return {
            "ok": True,
            "agent_id": profile["id"],
            "introduction": profile["introduction"],
        }
```

Comments must explain per-Agent binding, use of the existing sanitized profile boundary, the second explicit whitelist, and forbidden fields such as keys, model URLs, peers, clients, sessions, and full config dumps.

- [ ] **Step 3: Register classes in stable order**

`ToolExtension/__init__.py` must explicitly import both classes and define:

```python
EXTENSION_TOOLS: tuple[type[AgentTool], ...] = (
    TextStatsTool,
    AgentInfoTool,
)
```

Add catalog comments explaining trust, ordering, default-disabled selection, root isolation, and `startup()`/`shutdown()` ownership for future resource-backed tools.

- [ ] **Step 4: Run tests and verify GREEN**

Run the Task 1 command. Expected: all tests in `test_example_extension_tools.py` pass.

---

### Task 3: Update teaching documentation and verify the repository

**Files:**
- Modify: `README.md`
- Modify: `docs/中文入门教程.md`
- Modify: `HANDOFF.md`
- Modify: `docs/superpowers/specs/2026-07-19-extensible-agent-tools-design.md`
- Modify: `docs/superpowers/specs/2026-07-19-example-extension-tools-design.md`

**Interfaces:**
- Consumes: registered names `text_stats` and `agent_info`.
- Produces: complete copy-register-config-test instructions for future extensions.

- [ ] **Step 1: Document usage and extension workflow**

Add this configuration example:

```json
{
  "tools": {
    "extensions": ["text_stats", "agent_info"]
  }
}
```

Explain each sample, that registration does not enable it, how to copy one module, define a strict `ToolArguments` model, add a `ToolSpec`, return a JSON dictionary, register the class, enable it in JSON, and add tests. Replace wording that says `ToolExtension` is empty.

- [ ] **Step 2: Run focused tests**

```powershell
.\.venv\Scripts\python.exe -m pytest tests\test_example_extension_tools.py tests\test_tool_registry.py tests\test_agent_entry.py tests\test_agent_remote.py -q -p no:cacheprovider
```

Expected: all focused tests pass.

- [ ] **Step 3: Run final verification**

```powershell
uv lock --check
$testTemp = Join-Path $env:TEMP ('agentgraph-example-tools-' + [guid]::NewGuid())
.\.venv\Scripts\python.exe -m pytest -q -p no:cacheprovider --basetemp=$testTemp
.\.venv\Scripts\python.exe -m compileall -q Agent.py AgentRemote.py User.py core.py tool_system ToolExtension
git diff --check
```

Expected: lock, compilation, and diff checks pass. The only permitted full-suite failure is the already-known example-config placeholder assertion caused by untouched local `agents_setting` changes; report it without revealing credential values.
