"""Validated, immutable-at-runtime registry for Agent-bound tools."""

from __future__ import annotations

import asyncio
import json
import re
from copy import deepcopy
from dataclasses import dataclass
from enum import Enum
from types import MappingProxyType
from typing import Any, Literal, Mapping

from core import ConfigError

from .contract import AgentTool, ToolArguments, ToolSpec, ToolStateError


_TOOL_NAME = re.compile(r"^[A-Za-z0-9_-]{1,64}$")
_UNKNOWN_TOOL = {
    "ok": False,
    "code": "UNKNOWN_TOOL",
    "message": "模型请求了未开放的工具",
}
_EXECUTION_ERROR = {
    "ok": False,
    "code": "TOOL_EXECUTION_ERROR",
    "message": "工具执行失败",
}
_SCHEMA_CONSTRAINT_KEYWORDS = frozenset(
    {
        "additionalProperties",
        "allOf",
        "anyOf",
        "const",
        "contains",
        "dependentRequired",
        "dependentSchemas",
        "else",
        "enum",
        "exclusiveMaximum",
        "exclusiveMinimum",
        "if",
        "items",
        "maxContains",
        "maxItems",
        "maxLength",
        "maxProperties",
        "maximum",
        "minContains",
        "minItems",
        "minLength",
        "minProperties",
        "minimum",
        "multipleOf",
        "not",
        "oneOf",
        "pattern",
        "patternProperties",
        "prefixItems",
        "properties",
        "propertyNames",
        "required",
        "then",
        "type",
        "unevaluatedItems",
        "unevaluatedProperties",
        "uniqueItems",
    }
)


class ToolRegistryState(str, Enum):
    BUILT = "BUILT"
    STARTING = "STARTING"
    STARTED = "STARTED"
    SHUTTING_DOWN = "SHUTTING_DOWN"
    STOPPED = "STOPPED"


@dataclass(frozen=True, slots=True)
class _CatalogEntry:
    tool_class: type[AgentTool]
    spec: ToolSpec


@dataclass(frozen=True, slots=True)
class _BoundTool:
    instance: AgentTool
    spec: ToolSpec
    arguments_model: type[ToolArguments]
    schema: dict[str, Any]


class ToolRegistry:
    """A fixed collection of validated tools bound to one Agent instance."""

    def __init__(
        self,
        *,
        tools: dict[str, _BoundTool],
        schemas: list[dict[str, Any]],
    ) -> None:
        self._tools: Mapping[str, _BoundTool] = MappingProxyType(dict(tools))
        self._schemas = tuple(deepcopy(schemas))
        self._ordered_tools = tuple(tools.values())
        self._started_tools: tuple[_BoundTool, ...] = ()
        self._state = ToolRegistryState.BUILT

    @property
    def state(self) -> ToolRegistryState:
        return self._state

    @classmethod
    def build(
        cls,
        *,
        agent: Any,
        builtin_tool_classes: tuple[type[AgentTool], ...],
        extension_tool_classes: tuple[type[AgentTool], ...],
        enabled_extensions: Literal["all", "none"] | list[str],
    ) -> ToolRegistry:
        builtin_entries = _validate_catalog("内置工具目录", builtin_tool_classes)
        extension_entries = _validate_catalog("扩展工具目录", extension_tool_classes)
        _validate_unique_names((*builtin_entries, *extension_entries))
        enabled_names = _select_extensions(extension_entries, enabled_extensions)

        selected = [
            *builtin_entries,
            *(entry for entry in extension_entries if entry.spec.name in enabled_names),
        ]
        bound_tools: dict[str, _BoundTool] = {}
        schemas: list[dict[str, Any]] = []
        for entry in selected:
            instance, construction_failed = _construct_tool(entry.tool_class, agent)
            if construction_failed:
                raise ConfigError("工具实例构建失败")

            available, availability_failed = _check_availability(instance)
            if availability_failed:
                raise ConfigError("工具可用性检查失败")
            if not isinstance(available, bool):
                raise ConfigError("工具可用性检查必须返回布尔值")
            if not available:
                continue

            parameters, schema_failed = _generate_schema(instance)
            if schema_failed:
                raise ConfigError("工具 schema 生成失败")
            if not _is_strict_object_schema(parameters):
                raise ConfigError("工具 schema 必须是递归严格 object schema")
            if not _is_json_serializable(parameters):
                raise ConfigError("工具 schema 必须可 JSON 序列化")

            parameters_snapshot = deepcopy(parameters)
            spec_snapshot = ToolSpec(
                name=entry.spec.name,
                description=entry.spec.description,
                arguments_model=entry.spec.arguments_model,
            )
            function_schema = {
                "type": "function",
                "name": spec_snapshot.name,
                "description": spec_snapshot.description,
                "parameters": deepcopy(parameters_snapshot),
                "strict": True,
            }
            bound_tools[spec_snapshot.name] = _BoundTool(
                instance=instance,
                spec=spec_snapshot,
                arguments_model=spec_snapshot.arguments_model,
                schema=parameters_snapshot,
            )
            schemas.append(function_schema)

        return cls(tools=bound_tools, schemas=schemas)

    def schemas(self) -> list[dict[str, Any]]:
        return deepcopy(list(self._schemas))

    async def dispatch(self, name: Any, raw_arguments: Any) -> dict[str, Any]:
        if self._state is not ToolRegistryState.STARTED:
            raise ToolStateError("工具注册表仅能在 STARTED 状态分发调用")

        if not isinstance(name, str):
            return dict(_UNKNOWN_TOOL)
        bound = self._tools.get(name)
        if bound is None:
            return dict(_UNKNOWN_TOOL)
        if not isinstance(raw_arguments, str):
            return _invalid_arguments("工具参数必须是 JSON 对象字符串")

        try:
            arguments = bound.arguments_model.model_validate_json(
                raw_arguments,
                strict=True,
            )
        except Exception:
            return _invalid_arguments("工具参数不是有效的 JSON 对象")

        try:
            result = await bound.instance.execute(arguments)
        except asyncio.CancelledError:
            raise
        except Exception:
            return dict(_EXECUTION_ERROR)

        if not isinstance(result, dict):
            return dict(_EXECUTION_ERROR)
        try:
            json.dumps(result, ensure_ascii=False, allow_nan=False)
        except Exception:
            return dict(_EXECUTION_ERROR)
        return result

    async def startup(self) -> None:
        if self._state is ToolRegistryState.STARTED:
            return
        if self._state not in (ToolRegistryState.BUILT, ToolRegistryState.STOPPED):
            raise ToolStateError(
                f"工具注册表不能从 {self._state.value} 状态启动"
            )

        self._state = ToolRegistryState.STARTING
        started: list[_BoundTool] = []
        failure: BaseException | None = None
        failure_name: str | None = None
        for bound in self._ordered_tools:
            try:
                await bound.instance.startup()
            except BaseException as error:
                failure = error
                failure_name = bound.spec.name
                break
            started.append(bound)

        if failure is None:
            self._started_tools = tuple(started)
            self._state = ToolRegistryState.STARTED
            return

        for bound in reversed(started):
            try:
                await bound.instance.shutdown()
            except BaseException:
                continue
        self._started_tools = ()
        self._state = ToolRegistryState.STOPPED

        if isinstance(failure, asyncio.CancelledError):
            raise failure
        if isinstance(failure, Exception):
            raise ConfigError(f"工具 '{failure_name}' 启动失败")
        raise failure

    async def shutdown(self) -> None:
        if self._state is ToolRegistryState.STOPPED:
            return
        if self._state is ToolRegistryState.BUILT:
            self._state = ToolRegistryState.STOPPED
            return
        if self._state is not ToolRegistryState.STARTED:
            raise ToolStateError(
                f"工具注册表不能从 {self._state.value} 状态关闭"
        )

        self._state = ToolRegistryState.SHUTTING_DOWN
        failure: BaseException | None = None
        failure_name: str | None = None
        for bound in reversed(self._started_tools):
            try:
                await bound.instance.shutdown()
            except BaseException as error:
                if failure is None:
                    failure = error
                    failure_name = bound.spec.name
        self._started_tools = ()
        self._state = ToolRegistryState.STOPPED

        if isinstance(failure, asyncio.CancelledError):
            raise failure
        if isinstance(failure, Exception):
            raise ConfigError(f"工具 '{failure_name}' 关闭失败")
        if failure is not None:
            raise failure


def _validate_catalog(
    label: str,
    catalog: Any,
) -> tuple[_CatalogEntry, ...]:
    if not isinstance(catalog, tuple):
        raise ConfigError(f"{label}必须是工具类元组")

    entries: list[_CatalogEntry] = []
    for item in catalog:
        if (
            not isinstance(item, type)
            or not issubclass(item, AgentTool)
            or item is AgentTool
        ):
            raise ConfigError(f"{label}包含无效工具类")
        spec = getattr(item, "spec", None)
        if not _is_valid_spec(spec):
            raise ConfigError(f"{label}包含无效工具声明")
        entries.append(
            _CatalogEntry(
                tool_class=item,
                spec=ToolSpec(
                    name=spec.name,
                    description=spec.description,
                    arguments_model=spec.arguments_model,
                ),
            )
        )
    return tuple(entries)


def _is_valid_spec(spec: Any) -> bool:
    if not isinstance(spec, ToolSpec):
        return False
    if not isinstance(spec.name, str) or _TOOL_NAME.fullmatch(spec.name) is None:
        return False
    if not isinstance(spec.description, str) or not spec.description.strip():
        return False
    model = spec.arguments_model
    return (
        isinstance(model, type)
        and issubclass(model, ToolArguments)
        and model.model_config.get("extra") == "forbid"
    )


def _validate_unique_names(entries: tuple[_CatalogEntry, ...]) -> None:
    seen: set[str] = set()
    for entry in entries:
        if entry.spec.name in seen:
            raise ConfigError("工具目录包含重复名称")
        seen.add(entry.spec.name)


def _select_extensions(
    entries: tuple[_CatalogEntry, ...],
    enabled_extensions: Any,
) -> set[str]:
    available = {entry.spec.name for entry in entries}
    if enabled_extensions == "all":
        return available
    if enabled_extensions == "none":
        return set()
    if not isinstance(enabled_extensions, list):
        raise ConfigError("扩展工具选择必须是 'all'、'none' 或名称列表")
    if any(not isinstance(name, str) for name in enabled_extensions):
        raise ConfigError("扩展工具名称必须是字符串")
    if len(enabled_extensions) != len(set(enabled_extensions)):
        raise ConfigError("扩展工具名称不能重复")
    selected = set(enabled_extensions)
    if not selected.issubset(available):
        raise ConfigError("配置包含未知扩展工具")
    return selected


def _construct_tool(
    tool_class: type[AgentTool],
    agent: Any,
) -> tuple[AgentTool | None, bool]:
    try:
        return tool_class(agent), False
    except Exception:
        return None, True


def _check_availability(tool: AgentTool | None) -> tuple[Any, bool]:
    if tool is None:
        return None, True
    try:
        return tool.is_available(), False
    except Exception:
        return None, True


def _generate_schema(tool: AgentTool | None) -> tuple[Any, bool]:
    if tool is None:
        return None, True
    try:
        return deepcopy(tool.parameters_schema()), False
    except Exception:
        return None, True


def _is_strict_object_schema(schema: Any) -> bool:
    if not isinstance(schema, dict) or schema.get("type") != "object":
        return False
    return _validate_schema_node(schema, root=schema, reference_stack=[])


def _validate_schema_node(
    node: Any,
    *,
    root: dict[str, Any],
    reference_stack: list[tuple[str, bool]],
) -> bool:
    if not isinstance(node, dict):
        return False

    if "$dynamicRef" in node or "$recursiveRef" in node:
        return False

    reference = node.get("$ref")
    if reference is not None:
        target = _resolve_local_reference(root, reference)
        if target is None:
            return False
        cycle_start = next(
            (
                index
                for index, (active_reference, _) in enumerate(reference_stack)
                if active_reference == reference
            ),
            None,
        )
        if cycle_start is not None:
            if not any(
                productive for _, productive in reference_stack[cycle_start:]
            ):
                return False
        else:
            reference_stack.append((reference, _has_schema_constraints(target)))
            try:
                if not _validate_schema_node(
                    target,
                    root=root,
                    reference_stack=reference_stack,
                ):
                    return False
            finally:
                reference_stack.pop()

    node_type = node.get("type")
    declares_object = node_type == "object" or (
        isinstance(node_type, list) and "object" in node_type
    )
    object_keywords = (
        "additionalProperties",
        "dependentRequired",
        "dependentSchemas",
        "maxProperties",
        "minProperties",
        "patternProperties",
        "properties",
        "propertyNames",
        "required",
        "unevaluatedProperties",
    )
    has_object_keywords = any(keyword in node for keyword in object_keywords)
    if has_object_keywords and not declares_object:
        return False

    if declares_object:
        properties = node.get("properties")
        if not isinstance(properties, dict):
            return False
        if not all(isinstance(name, str) for name in properties):
            return False
        required = node.get("required", [])
        if not isinstance(required, list) or not all(
            isinstance(name, str) for name in required
        ):
            return False
        if len(required) != len(set(required)):
            return False
        if set(required) != set(properties):
            return False
        if node.get("additionalProperties") is not False:
            return False
        if "patternProperties" in node:
            return False

    mapping_keywords = (
        "$defs",
        "definitions",
        "dependentSchemas",
        "patternProperties",
        "properties",
    )
    for keyword in mapping_keywords:
        children = node.get(keyword)
        if children is None:
            continue
        if not isinstance(children, dict) or not all(
            _validate_schema_node(
                child,
                root=root,
                reference_stack=reference_stack,
            )
            for child in children.values()
        ):
            return False

    single_keywords = (
        "additionalProperties",
        "contains",
        "else",
        "if",
        "items",
        "not",
        "propertyNames",
        "then",
        "unevaluatedItems",
        "unevaluatedProperties",
    )
    for keyword in single_keywords:
        child = node.get(keyword)
        if child is None or isinstance(child, bool):
            continue
        if isinstance(child, list):
            if not all(
                _validate_schema_node(
                    item,
                    root=root,
                    reference_stack=reference_stack,
                )
                for item in child
            ):
                return False
        elif not _validate_schema_node(
            child,
            root=root,
            reference_stack=reference_stack,
        ):
            return False

    sequence_keywords = ("allOf", "anyOf", "oneOf", "prefixItems")
    for keyword in sequence_keywords:
        children = node.get(keyword)
        if children is None:
            continue
        if not isinstance(children, list) or not all(
            _validate_schema_node(
                child,
                root=root,
                reference_stack=reference_stack,
            )
            for child in children
        ):
            return False
    return True


def _has_schema_constraints(node: dict[str, Any]) -> bool:
    return any(keyword in node for keyword in _SCHEMA_CONSTRAINT_KEYWORDS)


def _resolve_local_reference(
    root: dict[str, Any],
    reference: Any,
) -> dict[str, Any] | None:
    if not isinstance(reference, str) or not reference.startswith("#/"):
        return None

    current: Any = root
    for raw_token in reference[2:].split("/"):
        if re.search(r"~(?:[^01]|$)", raw_token):
            return None
        token = raw_token.replace("~1", "/").replace("~0", "~")
        if not isinstance(current, dict) or token not in current:
            return None
        current = current[token]
    return current if isinstance(current, dict) else None


def _is_json_serializable(value: Any) -> bool:
    try:
        json.dumps(value, ensure_ascii=False, allow_nan=False)
    except Exception:
        return False
    return True


def _invalid_arguments(message: str) -> dict[str, Any]:
    return {
        "ok": False,
        "code": "INVALID_TOOL_ARGUMENTS",
        "message": message,
    }
