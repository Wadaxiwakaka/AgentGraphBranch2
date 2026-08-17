from __future__ import annotations

import asyncio
from copy import deepcopy
from types import MappingProxyType, SimpleNamespace
from typing import Any, ClassVar

import pytest
from pydantic import ConfigDict, model_validator

from core import ConfigError
from tool_system.contract import AgentTool, ToolArguments, ToolSpec, ToolStateError
from tool_system.registry import ToolRegistry, ToolRegistryState


class NoArguments(ToolArguments):
    pass


class CountArguments(ToolArguments):
    count: int


class EchoTool(AgentTool):
    spec = ToolSpec("echo", "Echo a count.", CountArguments)
    instances: ClassVar[list["EchoTool"]] = []

    def __init__(self, agent: Any) -> None:
        super().__init__(agent)
        self.availability_checks = 0
        self.schema_checks = 0
        self.__class__.instances.append(self)

    def is_available(self) -> bool:
        self.availability_checks += 1
        return True

    def parameters_schema(self) -> dict[str, Any]:
        self.schema_checks += 1
        return super().parameters_schema()

    async def execute(self, arguments: CountArguments) -> dict[str, Any]:
        return {"ok": True, "count": arguments.count, "agent": self.agent.name}


class HiddenTool(AgentTool):
    spec = ToolSpec("hidden", "Unavailable.", NoArguments)

    def __init__(self, agent: Any) -> None:
        super().__init__(agent)
        self.availability_checks = 0

    def is_available(self) -> bool:
        self.availability_checks += 1
        return False

    async def execute(self, arguments: NoArguments) -> dict[str, Any]:
        raise AssertionError("unavailable tools must not execute")


class AlphaExtension(AgentTool):
    spec = ToolSpec("alpha", "Alpha extension.", NoArguments)
    constructed: ClassVar[int] = 0

    def __init__(self, agent: Any) -> None:
        super().__init__(agent)
        self.__class__.constructed += 1

    async def execute(self, arguments: NoArguments) -> dict[str, Any]:
        return {"ok": True, "name": "alpha"}


class BetaExtension(AgentTool):
    spec = ToolSpec("beta", "Beta extension.", NoArguments)
    constructed: ClassVar[int] = 0

    def __init__(self, agent: Any) -> None:
        super().__init__(agent)
        self.__class__.constructed += 1

    async def execute(self, arguments: NoArguments) -> dict[str, Any]:
        return {"ok": True, "name": "beta"}


@pytest.fixture(autouse=True)
def reset_tool_counters() -> None:
    EchoTool.instances.clear()
    AlphaExtension.constructed = 0
    BetaExtension.constructed = 0


def build_registry(
    *,
    agent: Any | None = None,
    builtin: tuple[type[AgentTool], ...] = (EchoTool,),
    extensions: tuple[type[AgentTool], ...] = (),
    enabled: str | list[str] = "none",
) -> ToolRegistry:
    return ToolRegistry.build(
        agent=agent or SimpleNamespace(name="agent", currentChatSpace=None),
        builtin_tool_classes=builtin,
        extension_tool_classes=extensions,
        enabled_extensions=enabled,
    )


def test_build_selects_catalog_order_and_supports_all_none_and_list() -> None:
    extensions = (AlphaExtension, BetaExtension)

    none_registry = build_registry(extensions=extensions, enabled="none")
    assert [schema["name"] for schema in none_registry.schemas()] == ["echo"]
    assert AlphaExtension.constructed == 0
    assert BetaExtension.constructed == 0

    list_registry = build_registry(extensions=extensions, enabled=["beta", "alpha"])
    assert [schema["name"] for schema in list_registry.schemas()] == [
        "echo",
        "alpha",
        "beta",
    ]

    all_registry = build_registry(extensions=extensions, enabled="all")
    assert [schema["name"] for schema in all_registry.schemas()] == [
        "echo",
        "alpha",
        "beta",
    ]


def test_build_requires_class_tuples_and_validates_every_catalog_item() -> None:
    with pytest.raises(ConfigError, match="目录"):
        ToolRegistry.build(
            agent=SimpleNamespace(),
            builtin_tool_classes=[EchoTool],  # type: ignore[arg-type]
            extension_tool_classes=(),
            enabled_extensions="none",
        )

    with pytest.raises(ConfigError, match="目录"):
        build_registry(extensions=(AlphaExtension, object), enabled="none")  # type: ignore[arg-type]


def test_build_validates_specs_and_all_duplicate_names_before_selection() -> None:
    class InvalidSpecTool(AgentTool):
        spec = ToolSpec("bad name", "Invalid.", NoArguments)

        async def execute(self, arguments: NoArguments) -> dict[str, Any]:
            return {}

    class DuplicateDisabledTool(AgentTool):
        spec = ToolSpec("echo", "Duplicate.", NoArguments)

        async def execute(self, arguments: NoArguments) -> dict[str, Any]:
            return {}

    with pytest.raises(ConfigError, match="声明"):
        build_registry(extensions=(InvalidSpecTool,), enabled="none")

    with pytest.raises(ConfigError, match="重复"):
        build_registry(extensions=(DuplicateDisabledTool,), enabled="none")


def test_build_rejects_argument_models_that_reenable_extra_fields() -> None:
    class PermissiveArguments(ToolArguments):
        model_config = ConfigDict(extra="allow")

    class PermissiveTool(AgentTool):
        spec = ToolSpec("permissive", "Permissive arguments.", PermissiveArguments)

        def parameters_schema(self) -> dict[str, Any]:
            return {
                "type": "object",
                "properties": {},
                "required": [],
                "additionalProperties": False,
            }

        async def execute(self, arguments: PermissiveArguments) -> dict[str, Any]:
            return {"extra": arguments.model_extra}

    with pytest.raises(ConfigError, match="声明"):
        build_registry(builtin=(PermissiveTool,))


@pytest.mark.parametrize(
    "enabled",
    [["alpha", "alpha"], ["missing"], "invalid"],
)
def test_build_rejects_invalid_extension_selection(enabled: Any) -> None:
    with pytest.raises(ConfigError, match="扩展"):
        build_registry(extensions=(AlphaExtension,), enabled=enabled)


def test_build_creates_per_agent_instances_and_calls_snapshots_once() -> None:
    first_agent = SimpleNamespace(name="first", currentChatSpace=None)
    second_agent = SimpleNamespace(name="second", currentChatSpace=None)

    first = build_registry(agent=first_agent, builtin=(EchoTool, HiddenTool))
    second = build_registry(agent=second_agent, builtin=(EchoTool, HiddenTool))

    assert EchoTool.instances[0] is not EchoTool.instances[1]
    assert EchoTool.instances[0].agent is first_agent
    assert EchoTool.instances[1].agent is second_agent
    assert EchoTool.instances[0].availability_checks == 1
    assert EchoTool.instances[0].schema_checks == 1
    assert [schema["name"] for schema in first.schemas()] == ["echo"]
    assert [schema["name"] for schema in second.schemas()] == ["echo"]


def test_registry_freezes_mapping_and_returns_deep_copied_schema_snapshots() -> None:
    shared_schema = {
        "type": "object",
        "properties": {"count": {"type": "integer"}},
        "required": ["count"],
        "additionalProperties": False,
    }

    class SharedSchemaTool(EchoTool):
        spec = ToolSpec("shared", "Shared schema.", CountArguments)

        def parameters_schema(self) -> dict[str, Any]:
            return shared_schema

    registry = build_registry(builtin=(SharedSchemaTool,))
    first = registry.schemas()
    expected = deepcopy(first)

    assert isinstance(registry._tools, MappingProxyType)
    with pytest.raises(TypeError):
        registry._tools["other"] = object()  # type: ignore[index]
    first[0]["parameters"]["properties"]["count"]["type"] = "string"
    shared_schema["properties"]["count"]["type"] = "number"
    SharedSchemaTool.spec = ToolSpec("changed", "Changed.", NoArguments)

    assert registry.schemas() == expected


def test_build_accepts_recursive_strict_object_schema() -> None:
    class ChildArguments(ToolArguments):
        value: int

    class ParentArguments(ToolArguments):
        child: ChildArguments
        children: list[ChildArguments]

    class NestedTool(AgentTool):
        spec = ToolSpec("nested", "Nested schema.", ParentArguments)

        async def execute(self, arguments: ParentArguments) -> dict[str, Any]:
            return {"ok": True}

    registry = build_registry(builtin=(NestedTool,))

    assert registry.schemas()[0]["parameters"]["additionalProperties"] is False


def test_build_accepts_productive_recursive_local_references() -> None:
    class RecursiveNode(ToolArguments):
        value: int
        children: list[RecursiveNode]

    class RecursiveArguments(ToolArguments):
        root: RecursiveNode

    class RecursiveTool(AgentTool):
        spec = ToolSpec("recursive", "Recursive schema.", RecursiveArguments)

        async def execute(self, arguments: RecursiveArguments) -> dict[str, Any]:
            return {"ok": True}

    registry = build_registry(builtin=(RecursiveTool,))

    assert registry.schemas()[0]["name"] == "recursive"


def test_schema_validation_does_not_treat_default_data_as_a_subschema() -> None:
    class DefaultDataTool(AgentTool):
        spec = ToolSpec("default_data", "Default data.", NoArguments)

        def parameters_schema(self) -> dict[str, Any]:
            return {
                "type": "object",
                "properties": {
                    "note": {
                        "type": "string",
                        "default": {"type": "object"},
                    }
                },
                "required": ["note"],
                "additionalProperties": False,
            }

        async def execute(self, arguments: NoArguments) -> dict[str, Any]:
            return {"ok": True}

    registry = build_registry(builtin=(DefaultDataTool,))

    assert registry.schemas()[0]["name"] == "default_data"


def test_build_rejects_schema_that_is_not_json_serializable() -> None:
    class UnserializableSchemaTool(AgentTool):
        spec = ToolSpec("unserializable_schema", "Bad schema.", NoArguments)

        def parameters_schema(self) -> dict[str, Any]:
            return {
                "type": "object",
                "properties": {
                    "note": {"type": "string", "default": object()},
                },
                "required": ["note"],
                "additionalProperties": False,
            }

        async def execute(self, arguments: NoArguments) -> dict[str, Any]:
            return {"ok": True}

    with pytest.raises(ConfigError, match="schema") as exc_info:
        build_registry(builtin=(UnserializableSchemaTool,))

    assert exc_info.value.__cause__ is None
    assert exc_info.value.__context__ is None


@pytest.mark.parametrize(
    "schema",
    [
        {"type": "array", "items": {"type": "string"}},
        {"type": "object", "properties": {}, "required": []},
        {
            "type": "object",
            "properties": {"child": {"type": "object", "properties": {}}},
            "required": ["child"],
            "additionalProperties": False,
        },
        {
            "type": "object",
            "properties": {"one": {"type": "string"}},
            "required": [],
            "additionalProperties": False,
        },
        {
            "type": "object",
            "properties": {
                "child": {
                    "type": ["object", "null"],
                    "properties": {"value": {"type": "string"}},
                    "required": [],
                    "additionalProperties": False,
                }
            },
            "required": ["child"],
            "additionalProperties": False,
        },
        {
            "type": "object",
            "properties": {
                "child": {
                    "properties": {"value": {"type": "string"}},
                }
            },
            "required": ["child"],
            "additionalProperties": False,
        },
        {
            "type": "object",
            "properties": {
                "child": {"$ref": "https://example.invalid/schema.json"},
            },
            "required": ["child"],
            "additionalProperties": False,
        },
        {
            "type": "object",
            "properties": {
                "child": {"$ref": "#/$defs/missing"},
            },
            "required": ["child"],
            "additionalProperties": False,
        },
        {
            "type": "object",
            "properties": {},
            "required": [],
            "additionalProperties": False,
            "patternProperties": {".*": {"type": "string"}},
        },
        {
            "type": "object",
            "properties": {
                "child": {"$dynamicRef": "#node"},
            },
            "required": ["child"],
            "additionalProperties": False,
        },
        {
            "type": "object",
            "properties": {
                "child": {"$ref": "#/properties/child"},
            },
            "required": ["child"],
            "additionalProperties": False,
        },
        {
            "$defs": {
                "first": {"$ref": "#/$defs/second"},
                "second": {"$ref": "#/$defs/first"},
            },
            "type": "object",
            "properties": {
                "child": {"$ref": "#/$defs/first"},
            },
            "required": ["child"],
            "additionalProperties": False,
        },
    ],
)
def test_build_rejects_non_strict_schema_recursively(schema: dict[str, Any]) -> None:
    class BadSchemaTool(AgentTool):
        spec = ToolSpec("bad_schema", "Bad schema.", NoArguments)

        def parameters_schema(self) -> dict[str, Any]:
            return schema

        async def execute(self, arguments: NoArguments) -> dict[str, Any]:
            return {}

    with pytest.raises(ConfigError, match="schema"):
        build_registry(builtin=(BadSchemaTool,))


def test_build_errors_are_safe_config_errors_without_exception_chains() -> None:
    class ExplodingTool(AgentTool):
        spec = ToolSpec("explode", "Explodes.", NoArguments)

        def __init__(self, agent: Any) -> None:
            raise RuntimeError("secret-token")

        async def execute(self, arguments: NoArguments) -> dict[str, Any]:
            return {}

    with pytest.raises(ConfigError) as exc_info:
        build_registry(builtin=(ExplodingTool,))

    assert "secret-token" not in str(exc_info.value)
    assert exc_info.value.__cause__ is None
    assert exc_info.value.__context__ is None


@pytest.mark.asyncio
async def test_dispatch_requires_started_state_and_strictly_validates_arguments() -> None:
    registry = build_registry()

    with pytest.raises(ToolStateError, match="STARTED"):
        await registry.dispatch("echo", '{"count":1}')

    await registry.startup()
    assert await registry.dispatch("echo", '{"count":1}') == {
        "ok": True,
        "count": 1,
        "agent": "agent",
    }
    for raw_arguments in (
        '{"count":"1"}',
        '{"count":1,"extra":true}',
        "[]",
        "not-json",
        {"count": 1},
    ):
        result = await registry.dispatch("echo", raw_arguments)
        assert result["code"] == "INVALID_TOOL_ARGUMENTS"


@pytest.mark.asyncio
async def test_dispatch_returns_stable_unknown_tool_error() -> None:
    registry = build_registry()
    await registry.startup()

    for name in ("missing", None, 1):
        assert await registry.dispatch(name, "{}") == {
            "ok": False,
            "code": "UNKNOWN_TOOL",
            "message": "模型请求了未开放的工具",
        }


@pytest.mark.asyncio
async def test_dispatch_sanitizes_argument_validator_exceptions() -> None:
    class ExplodingArguments(ToolArguments):
        value: int

        @model_validator(mode="after")
        def explode(self) -> "ExplodingArguments":
            raise RuntimeError("validator-secret")

    class ValidatorTool(AgentTool):
        spec = ToolSpec("validator", "Validator.", ExplodingArguments)

        async def execute(self, arguments: ExplodingArguments) -> dict[str, Any]:
            raise AssertionError("invalid arguments must not execute")

    registry = build_registry(builtin=(ValidatorTool,))
    await registry.startup()

    result = await registry.dispatch("validator", '{"value":1}')

    assert result["code"] == "INVALID_TOOL_ARGUMENTS"
    assert "validator-secret" not in str(result)


@pytest.mark.asyncio
async def test_dispatch_sanitizes_exceptions_bad_results_and_nan(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class BadResultTool(AgentTool):
        spec = ToolSpec("bad_result", "Bad result.", NoArguments)
        mode: ClassVar[str] = "exception"

        async def execute(self, arguments: NoArguments) -> dict[str, Any]:
            if self.mode == "exception":
                raise RuntimeError("secret-token")
            if self.mode == "not_dict":
                return ["bad"]  # type: ignore[return-value]
            if self.mode == "nan":
                return {"value": float("nan")}
            return {"value": object()}

    registry = build_registry(builtin=(BadResultTool,))
    await registry.startup()
    expected = {
        "ok": False,
        "code": "TOOL_EXECUTION_ERROR",
        "message": "工具执行失败",
    }

    for mode in ("exception", "not_dict", "nan", "unserializable"):
        BadResultTool.mode = mode
        assert await registry.dispatch("bad_result", "{}") == expected

    BadResultTool.mode = "valid"

    def explode_json(*args: Any, **kwargs: Any) -> str:
        del args, kwargs
        raise RuntimeError("serializer-secret")

    monkeypatch.setattr("tool_system.registry.json.dumps", explode_json)
    assert await registry.dispatch("bad_result", "{}") == expected


@pytest.mark.asyncio
async def test_dispatch_propagates_cancellation() -> None:
    class CancelledTool(AgentTool):
        spec = ToolSpec("cancel", "Cancelled.", NoArguments)

        async def execute(self, arguments: NoArguments) -> dict[str, Any]:
            raise asyncio.CancelledError

    registry = build_registry(builtin=(CancelledTool,))
    await registry.startup()

    with pytest.raises(asyncio.CancelledError):
        await registry.dispatch("cancel", "{}")


def lifecycle_tool(
    name: str,
    events: list[str],
    *,
    startup_error: bool = False,
    shutdown_error: bool = False,
    shutdown_cancelled: bool = False,
) -> type[AgentTool]:
    class LifecycleTool(AgentTool):
        spec = ToolSpec(name, f"{name} lifecycle.", NoArguments)

        async def startup(self) -> None:
            events.append(f"start:{name}")
            if startup_error:
                raise RuntimeError(f"startup-secret:{name}")

        async def shutdown(self) -> None:
            events.append(f"stop:{name}")
            if shutdown_error:
                raise RuntimeError(f"shutdown-secret:{name}")
            if shutdown_cancelled:
                raise asyncio.CancelledError

        async def execute(self, arguments: NoArguments) -> dict[str, Any]:
            return {"ok": True}

    return LifecycleTool


@pytest.mark.asyncio
async def test_lifecycle_orders_hooks_and_tracks_states() -> None:
    events: list[str] = []
    first = lifecycle_tool("first", events)
    second = lifecycle_tool("second", events)
    registry = build_registry(builtin=(first, second))

    assert registry.state is ToolRegistryState.BUILT
    await registry.startup()
    assert registry.state is ToolRegistryState.STARTED
    await registry.startup()
    assert events == ["start:first", "start:second"]
    await registry.shutdown()
    assert registry.state is ToolRegistryState.STOPPED
    assert events == ["start:first", "start:second", "stop:second", "stop:first"]
    await registry.shutdown()
    assert events == ["start:first", "start:second", "stop:second", "stop:first"]

    await registry.startup()
    assert registry.state is ToolRegistryState.STARTED
    await registry.shutdown()
    assert events == [
        "start:first",
        "start:second",
        "stop:second",
        "stop:first",
        "start:first",
        "start:second",
        "stop:second",
        "stop:first",
    ]


@pytest.mark.asyncio
async def test_startup_failure_rolls_back_started_tools_and_stops_registry() -> None:
    events: list[str] = []
    first = lifecycle_tool("first", events)
    second = lifecycle_tool("second", events, startup_error=True)
    third = lifecycle_tool("third", events)
    registry = build_registry(builtin=(first, second, third))

    with pytest.raises(ConfigError) as exc_info:
        await registry.startup()

    assert registry.state is ToolRegistryState.STOPPED
    assert events == ["start:first", "start:second", "stop:first"]
    assert "second" in str(exc_info.value)
    assert "startup-secret" not in str(exc_info.value)
    assert exc_info.value.__cause__ is None
    assert exc_info.value.__context__ is None


@pytest.mark.asyncio
async def test_startup_failure_takes_priority_over_rollback_cancellation() -> None:
    events: list[str] = []

    class RollbackCancelledTool(AgentTool):
        spec = ToolSpec("rollback_cancel", "Rollback cancellation.", NoArguments)

        async def startup(self) -> None:
            events.append("start:rollback")

        async def shutdown(self) -> None:
            events.append("stop:rollback")
            raise asyncio.CancelledError

        async def execute(self, arguments: NoArguments) -> dict[str, Any]:
            return {"ok": True}

    failing = lifecycle_tool("failing", events, startup_error=True)
    registry = build_registry(builtin=(RollbackCancelledTool, failing))

    with pytest.raises(ConfigError, match="启动"):
        await registry.startup()

    assert registry.state is ToolRegistryState.STOPPED
    assert events == ["start:rollback", "start:failing", "stop:rollback"]


@pytest.mark.asyncio
async def test_shutdown_continues_after_errors_then_raises_safe_first_error() -> None:
    events: list[str] = []
    first = lifecycle_tool("first", events, shutdown_error=True)
    second = lifecycle_tool("second", events, shutdown_error=True)
    registry = build_registry(builtin=(first, second))
    await registry.startup()

    with pytest.raises(ConfigError) as exc_info:
        await registry.shutdown()

    assert registry.state is ToolRegistryState.STOPPED
    assert events == ["start:first", "start:second", "stop:second", "stop:first"]
    assert "second" in str(exc_info.value)
    assert "first" not in str(exc_info.value)
    assert "shutdown-secret" not in str(exc_info.value)
    assert exc_info.value.__cause__ is None
    assert exc_info.value.__context__ is None


@pytest.mark.asyncio
async def test_shutdown_preserves_earlier_error_when_later_cleanup_is_cancelled() -> None:
    events: list[str] = []
    first = lifecycle_tool("first", events, shutdown_cancelled=True)
    second = lifecycle_tool("second", events, shutdown_error=True)
    registry = build_registry(builtin=(first, second))
    await registry.startup()

    with pytest.raises(ConfigError) as exc_info:
        await registry.shutdown()

    assert registry.state is ToolRegistryState.STOPPED
    assert events == ["start:first", "start:second", "stop:second", "stop:first"]
    assert "second" in str(exc_info.value)
    assert exc_info.value.__cause__ is None
    assert exc_info.value.__context__ is None


@pytest.mark.asyncio
async def test_built_registry_can_stop_without_running_hooks_then_restart() -> None:
    registry = build_registry()
    await registry.shutdown()
    assert registry.state is ToolRegistryState.STOPPED

    await registry.startup()
    assert registry.state is ToolRegistryState.STARTED
