from __future__ import annotations

import json
from typing import Any

import httpx
import pytest
from openai import BadRequestError, OpenAI, pydantic_function_tool

import ToolExtension.get_weather as get_weather_module
from ToolExtension import EXTENSION_TOOLS
from ToolExtension.agent_info import AgentInfoTool
from ToolExtension.get_weather import GetWeatherArguments, GetWeatherTool
from ToolExtension.search_knowledge import SearchKnowledgeTool
from ToolExtension.text_stats import TextStatsTool
from tool_system.contract import AgentTool, ToolStateError
from tool_system.registry import ToolRegistry


EXPECTED_GET_WEATHER_TOOL_SCHEMA = {
    "type": "function",
    "name": "get_weather",
    "description": "Retrieves current weather for the given location.",
    "parameters": {
        "type": "object",
        "properties": {
            "location": {
                "type": "string",
                "description": "City and country e.g. Bogotá, Colombia",
            },
            "units": {
                "type": "string",
                "enum": ["celsius", "fahrenheit"],
                "description": "Units the temperature will be returned in.",
            },
        },
        "required": ["location", "units"],
        "additionalProperties": False,
    },
    "strict": True,
}

EXPECTED_GET_WEATHER_DEFAULT_TOOL_SCHEMA = {
    "type": "function",
    "name": "get_weather",
    "description": "Retrieves current weather for the given location.",
    "parameters": {
        "additionalProperties": False,
        "description": "Parameters used to retrieve current weather.",
        "properties": {
            "location": {
                "description": "City and country e.g. Bogotá, Colombia",
                "title": "Weather Location",
                "type": "string",
            },
            "units": {
                "description": "Units the temperature will be returned in.",
                "enum": ["celsius", "fahrenheit"],
                "title": "Temperature Units",
                "type": "string",
            },
        },
        "required": ["location", "units"],
        "title": "WeatherQueryParameters",
        "type": "object",
    },
    "strict": True,
}


class _FakeAgent:
    def __init__(
        self,
        agent_id: str = "teacher",
        introduction: str = "教学 Agent",
    ) -> None:
        self.agent_id = agent_id
        self.introduction = introduction

    def get_profile(self) -> dict[str, Any]:
        return {
            "id": self.agent_id,
            "introduction": self.introduction,
            "host": "127.0.0.1",
            "port": 9137,
            "keys": "KEYS_SECRET_MARKER",
            "protocol": "PROTOCOL_SECRET_MARKER",
            "protocol_version": "v999",
            "inbound": "INBOUND_SECRET_MARKER",
            "model": "MODEL_SECRET_MARKER",
            "peer": "PEER_SECRET_MARKER",
            "session": "SESSION_SECRET_MARKER",
        }


def _build_registry(
    enabled_extensions: str | list[str],
    agent: _FakeAgent | None = None,
) -> ToolRegistry:
    return ToolRegistry.build(
        agent=agent or _FakeAgent(),
        builtin_tool_classes=(),
        extension_tool_classes=EXTENSION_TOOLS,
        enabled_extensions=enabled_extensions,
    )


def _mock_weather_clients(
    monkeypatch: pytest.MonkeyPatch,
    handler: Any,
) -> list[httpx.AsyncClient]:
    real_async_client = httpx.AsyncClient
    created_clients: list[httpx.AsyncClient] = []

    def create_client(*args: Any, **kwargs: Any) -> httpx.AsyncClient:
        client = real_async_client(
            *args,
            transport=httpx.MockTransport(handler),
            **kwargs,
        )
        created_clients.append(client)
        return client

    monkeypatch.setattr(get_weather_module.httpx, "AsyncClient", create_client)
    return created_clients


def test_extension_catalog_has_the_three_examples_in_stable_order() -> None:
    assert EXTENSION_TOOLS == (
        TextStatsTool,
        AgentInfoTool,
        GetWeatherTool,
        SearchKnowledgeTool,
    )


def test_extension_selection_supports_none_empty_list_catalog_order_and_all() -> None:
    assert _build_registry("none").schemas() == []
    assert _build_registry([]).schemas() == []
    selected_names = [
        schema["name"]
        for schema in _build_registry(
            ["get_weather", "agent_info", "text_stats"]
        ).schemas()
    ]
    assert selected_names == ["text_stats", "agent_info", "get_weather"]
    assert [schema["name"] for schema in _build_registry("all").schemas()] == [
        "text_stats",
        "agent_info",
        "get_weather",
    ]


def test_get_weather_generated_tool_schema_matches_openai_example_exactly() -> None:
    assert _build_registry(["get_weather"]).schemas() == [
        EXPECTED_GET_WEATHER_TOOL_SCHEMA
    ]


def test_get_weather_default_schema_without_override_matches_documented_json() -> None:
    tool = GetWeatherTool(_FakeAgent())
    default_tool_schema = {
        "type": "function",
        "name": tool.spec.name,
        "description": tool.spec.description,
        "parameters": AgentTool.parameters_schema(tool),
        "strict": True,
    }

    assert default_tool_schema == EXPECTED_GET_WEATHER_DEFAULT_TOOL_SCHEMA


def test_openai_pydantic_helper_keeps_default_schema_annotations() -> None:
    helper_tool = pydantic_function_tool(
        GetWeatherArguments,
        name="get_weather",
        description="Retrieves current weather for the given location.",
    )

    assert helper_tool["function"]["parameters"] == (
        EXPECTED_GET_WEATHER_DEFAULT_TOOL_SCHEMA["parameters"]
    )


def test_openai_responses_sdk_sends_default_schema_annotations_unchanged() -> None:
    captured_body: dict[str, Any] = {}

    def handler(request: httpx.Request) -> httpx.Response:
        captured_body.update(json.loads(request.content))
        return httpx.Response(
            400,
            request=request,
            json={
                "error": {
                    "message": "停止模拟请求",
                    "type": "invalid_request_error",
                    "param": None,
                    "code": None,
                }
            },
        )

    tool_schema = EXPECTED_GET_WEATHER_DEFAULT_TOOL_SCHEMA
    with OpenAI(
        api_key="test-key",
        base_url="http://openai-sdk-capture.invalid/v1",
        max_retries=0,
        http_client=httpx.Client(transport=httpx.MockTransport(handler)),
    ) as client:
        with pytest.raises(BadRequestError):
            client.responses.create(
                model="test-model",
                tools=[tool_schema],  # type: ignore[list-item]
                input="test input",
            )

    assert captured_body["tools"] == [tool_schema]


def test_text_stats_schema_is_strict_and_constrains_text() -> None:
    schema = TextStatsTool(_FakeAgent()).parameters_schema()

    assert schema["type"] == "object"
    assert schema["additionalProperties"] is False
    assert schema["required"] == ["text"]
    assert schema["properties"]["text"] == {
        "type": "string",
        "minLength": 1,
        "maxLength": 10000,
        "title": "Text",
    }


@pytest.mark.asyncio
async def test_text_stats_dispatches_exact_counts_and_rejects_invalid_arguments() -> None:
    registry = _build_registry(["text_stats"])
    await registry.startup()
    try:
        result = await registry.dispatch(
            "text_stats",
            json.dumps({"text": "你好 world\nnext"}, ensure_ascii=False),
        )
        assert result == {
            "ok": True,
            "character_count": 13,
            "non_whitespace_character_count": 11,
            "word_count": 3,
            "line_count": 2,
        }

        for raw_arguments in (
            "{}",
            '{"text":""}',
            '{"text":1}',
            '{"text":"valid","extra":true}',
            json.dumps({"text": "x" * 10001}),
        ):
            assert (await registry.dispatch("text_stats", raw_arguments))["code"] == (
                "INVALID_TOOL_ARGUMENTS"
            )
    finally:
        await registry.shutdown()


@pytest.mark.asyncio
async def test_text_stats_counts_unicode_code_points_not_graphemes_or_utf8_bytes() -> None:
    registry = _build_registry(["text_stats"])
    await registry.startup()
    try:
        result = await registry.dispatch(
            "text_stats",
            json.dumps({"text": "e\u0301 \U0001f469\u200d\U0001f4bb"}),
        )
        assert result == {
            "ok": True,
            "character_count": 6,
            "non_whitespace_character_count": 5,
            "word_count": 2,
            "line_count": 1,
        }
    finally:
        await registry.shutdown()


@pytest.mark.asyncio
async def test_agent_info_exposes_only_safe_profile_fields_and_rejects_extra_fields() -> None:
    registry = _build_registry(["agent_info"])
    schema = next(
        item["parameters"]
        for item in registry.schemas()
        if item["name"] == "agent_info"
    )
    assert schema["type"] == "object"
    assert schema["properties"] == {}
    assert schema["additionalProperties"] is False
    await registry.startup()
    try:
        result = await registry.dispatch("agent_info", "{}")
        assert result == {
            "ok": True,
            "agent_id": "teacher",
            "introduction": "教学 Agent",
        }
        serialized = json.dumps(result, ensure_ascii=False, allow_nan=False)
        for marker in (
            "INBOUND_SECRET_MARKER",
            "KEYS_SECRET_MARKER",
            "MODEL_SECRET_MARKER",
            "PEER_SECRET_MARKER",
            "PROTOCOL_SECRET_MARKER",
            "SESSION_SECRET_MARKER",
            "127.0.0.1",
            "9137",
            "v999",
        ):
            assert marker not in serialized

        assert (await registry.dispatch("agent_info", '{"extra":true}'))["code"] == (
            "INVALID_TOOL_ARGUMENTS"
        )
    finally:
        await registry.shutdown()


@pytest.mark.asyncio
async def test_agent_info_tools_bind_to_their_own_agents_without_shared_cache() -> None:
    first_registry = _build_registry(
        ["agent_info"],
        _FakeAgent("first", "第一个 Agent"),
    )
    second_registry = _build_registry(
        ["agent_info"],
        _FakeAgent("second", "第二个 Agent"),
    )
    await first_registry.startup()
    await second_registry.startup()
    try:
        assert await first_registry.dispatch("agent_info", "{}") == {
            "ok": True,
            "agent_id": "first",
            "introduction": "第一个 Agent",
        }
        assert await second_registry.dispatch("agent_info", "{}") == {
            "ok": True,
            "agent_id": "second",
            "introduction": "第二个 Agent",
        }
    finally:
        await second_registry.shutdown()
        await first_registry.shutdown()


@pytest.mark.asyncio
async def test_get_weather_owns_one_http_client_per_started_lifespan(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    async def unexpected_request(request: httpx.Request) -> httpx.Response:
        raise AssertionError(f"生命周期测试不应发送请求: {request.url}")

    created_clients = _mock_weather_clients(monkeypatch, unexpected_request)
    registry = _build_registry(["get_weather"])

    assert created_clients == []

    await registry.startup()
    first_client = created_clients[0]
    assert first_client.is_closed is False
    await registry.shutdown()
    assert first_client.is_closed is True

    await registry.startup()
    second_client = created_clients[1]
    assert second_client is not first_client
    assert second_client.is_closed is False
    await registry.shutdown()
    assert second_client.is_closed is True


@pytest.mark.asyncio
async def test_get_weather_rejects_invalid_arguments_without_network_access(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    requests: list[httpx.Request] = []

    async def handler(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        return httpx.Response(500, request=request)

    _mock_weather_clients(monkeypatch, handler)
    registry = _build_registry(["get_weather"])
    await registry.startup()
    try:
        for raw_arguments in (
            "{}",
            '{"location":"Bogotá, Colombia"}',
            '{"location":"Bogotá, Colombia","units":"kelvin"}',
            '{"location":1,"units":"celsius"}',
            '{"location":"Bogotá, Colombia","units":"celsius","extra":true}',
        ):
            assert (await registry.dispatch("get_weather", raw_arguments))["code"] == (
                "INVALID_TOOL_ARGUMENTS"
            )
        assert requests == []
    finally:
        await registry.shutdown()


@pytest.mark.asyncio
async def test_get_weather_queries_geocoding_then_current_weather(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    requests: list[httpx.Request] = []

    async def handler(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        if request.url.host == "geocoding-api.open-meteo.com":
            assert request.url.path == "/v1/search"
            assert request.url.params["name"] == "Bogotá, Colombia"
            assert request.url.params["count"] == "1"
            assert request.url.params["format"] == "json"
            return httpx.Response(
                200,
                request=request,
                json={
                    "results": [
                        {
                            "name": "Bogotá",
                            "country": "Colombia",
                            "latitude": 4.711,
                            "longitude": -74.0721,
                        }
                    ]
                },
            )

        assert request.url.host == "api.open-meteo.com"
        assert request.url.path == "/v1/forecast"
        assert request.url.params["latitude"] == "4.711"
        assert request.url.params["longitude"] == "-74.0721"
        assert request.url.params["current"] == "temperature_2m"
        assert request.url.params["temperature_unit"] == "fahrenheit"
        assert request.url.params["timezone"] == "auto"
        return httpx.Response(
            200,
            request=request,
            json={
                "current": {
                    "time": "2026-07-19T12:00",
                    "temperature_2m": 64.4,
                },
                "current_units": {"temperature_2m": "°F"},
            },
        )

    created_clients = _mock_weather_clients(monkeypatch, handler)
    registry = _build_registry(["get_weather"])
    await registry.startup()
    try:
        result = await registry.dispatch(
            "get_weather",
            json.dumps(
                {"location": "Bogotá, Colombia", "units": "fahrenheit"},
                ensure_ascii=False,
            ),
        )
        assert result == {
            "ok": True,
            "location": "Bogotá",
            "country": "Colombia",
            "temperature": 64.4,
            "unit": "°F",
            "observed_at": "2026-07-19T12:00",
            "source": "Open-Meteo",
        }
        assert [request.url.host for request in requests] == [
            "geocoding-api.open-meteo.com",
            "api.open-meteo.com",
        ]
    finally:
        await registry.shutdown()

    assert created_clients[0].is_closed is True


@pytest.mark.asyncio
async def test_get_weather_returns_not_found_without_forecast_request(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    requests: list[httpx.Request] = []

    async def handler(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        return httpx.Response(200, request=request, json={"results": []})

    _mock_weather_clients(monkeypatch, handler)
    registry = _build_registry(["get_weather"])
    await registry.startup()
    try:
        assert await registry.dispatch(
            "get_weather",
            '{"location":"Missing Place","units":"celsius"}',
        ) == {
            "ok": False,
            "code": "LOCATION_NOT_FOUND",
            "message": "未找到对应地点",
        }
        assert len(requests) == 1
        assert requests[0].url.host == "geocoding-api.open-meteo.com"
    finally:
        await registry.shutdown()


@pytest.mark.asyncio
async def test_get_weather_converts_http_failures_to_safe_execution_error(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    async def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(
            503,
            request=request,
            text="UPSTREAM_SECRET_RESPONSE_MARKER",
        )

    _mock_weather_clients(monkeypatch, handler)
    registry = _build_registry(["get_weather"])
    await registry.startup()
    try:
        result = await registry.dispatch(
            "get_weather",
            '{"location":"Bogotá, Colombia","units":"celsius"}',
        )
        assert result == {
            "ok": False,
            "code": "TOOL_EXECUTION_ERROR",
            "message": "工具执行失败",
        }
        assert "UPSTREAM_SECRET_RESPONSE_MARKER" not in json.dumps(
            result,
            ensure_ascii=False,
        )
        assert "open-meteo.com" not in json.dumps(result, ensure_ascii=False)
    finally:
        await registry.shutdown()


@pytest.mark.asyncio
async def test_get_weather_rejects_non_numeric_coordinates_before_forecast(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    requests: list[httpx.Request] = []

    async def handler(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        if request.url.host == "api.open-meteo.com":
            return httpx.Response(
                200,
                request=request,
                json={
                    "current": {
                        "time": "2026-07-19T12:00",
                        "temperature_2m": 18.0,
                    },
                    "current_units": {"temperature_2m": "°C"},
                },
            )
        return httpx.Response(
            200,
            request=request,
            json={
                "results": [
                    {
                        "name": "Bogotá",
                        "country": "Colombia",
                        "latitude": "4.711",
                        "longitude": -74.0721,
                    }
                ]
            },
        )

    _mock_weather_clients(monkeypatch, handler)
    registry = _build_registry(["get_weather"])
    await registry.startup()
    try:
        assert await registry.dispatch(
            "get_weather",
            '{"location":"Bogotá, Colombia","units":"celsius"}',
        ) == {
            "ok": False,
            "code": "TOOL_EXECUTION_ERROR",
            "message": "工具执行失败",
        }
        assert len(requests) == 1
    finally:
        await registry.shutdown()


@pytest.mark.asyncio
async def test_get_weather_rejects_non_primitive_result_fields(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    async def handler(request: httpx.Request) -> httpx.Response:
        if request.url.host == "geocoding-api.open-meteo.com":
            return httpx.Response(
                200,
                request=request,
                json={
                    "results": [
                        {
                            "name": "Bogotá",
                            "country": "Colombia",
                            "latitude": 4.711,
                            "longitude": -74.0721,
                        }
                    ]
                },
            )
        return httpx.Response(
            200,
            request=request,
            json={
                "current": {
                    "time": "2026-07-19T12:00",
                    "temperature_2m": {"unexpected": "object"},
                },
                "current_units": {"temperature_2m": "°C"},
            },
        )

    _mock_weather_clients(monkeypatch, handler)
    registry = _build_registry(["get_weather"])
    await registry.startup()
    try:
        assert await registry.dispatch(
            "get_weather",
            '{"location":"Bogotá, Colombia","units":"celsius"}',
        ) == {
            "ok": False,
            "code": "TOOL_EXECUTION_ERROR",
            "message": "工具执行失败",
        }
    finally:
        await registry.shutdown()


@pytest.mark.asyncio
async def test_get_weather_requires_startup_before_direct_execution() -> None:
    tool = GetWeatherTool(_FakeAgent())

    with pytest.raises(ToolStateError, match="尚未启动"):
        await tool.execute(
            GetWeatherArguments(
                location="Bogotá, Colombia",
                units="celsius",
            )
        )
