from __future__ import annotations

import json
import traceback
from uuid import UUID, uuid4

import pytest
from pydantic import ValidationError

import core


def test_peer_config_uses_loopback_http_defaults() -> None:
    peer = core.PeerConfig(id="peer_1", port=8080, key="peer-secret")

    assert peer.ip == "127.0.0.1"
    assert peer.protocol == "http"
    assert peer.key.get_secret_value() == "peer-secret"


@pytest.mark.parametrize("bad_id", ["", "contains space", "a" * 65, "peer.dot"])
def test_peer_config_rejects_invalid_ids(bad_id: str) -> None:
    with pytest.raises(ValidationError):
        core.PeerConfig(id=bad_id, port=8080, key="peer-secret")


@pytest.mark.parametrize("bad_port", [0, 65536])
def test_peer_config_rejects_ports_outside_tcp_range(bad_port: int) -> None:
    with pytest.raises(ValidationError):
        core.PeerConfig(id="peer", port=bad_port, key="peer-secret")


@pytest.mark.parametrize("empty_key", ["", "   "])
def test_peer_config_rejects_empty_keys(empty_key: str) -> None:
    with pytest.raises(ValidationError):
        core.PeerConfig(id="peer", port=8080, key=empty_key)


def _valid_agent_data() -> dict[str, object]:
    return {
        "id": "worker",
        "introduction": "负责处理测试任务。",
        "port": 9000,
        "key": "agent-secret",
        "openai_baseurl": "https://api.example.test/v1",
        "openai_key": "openai-secret",
        "model": "test-model",
    }


def test_root_agent_can_omit_agent_and_openai_credentials() -> None:
    config = core.AgentConfig(id="root", introduction="根节点", port=8000)

    assert config.host == "127.0.0.1"
    assert config.key is None
    assert config.openai_baseurl is None
    assert config.openai_key is None
    assert config.model is None
    assert config.agents == []


def test_agent_lists_are_not_shared_between_config_instances() -> None:
    first = core.AgentConfig(id="root", introduction="第一个根节点", port=8000)
    second = core.AgentConfig(id="root", introduction="第二个根节点", port=8001)

    first.agents.append(
        core.PeerConfig(id="peer", port=9000, key="peer-secret")
    )

    assert second.agents == []


def test_agent_config_defaults_to_no_extension_tools() -> None:
    first = core.AgentConfig(**_valid_agent_data())
    second = core.AgentConfig(**_valid_agent_data())

    assert first.tools.extensions == []
    first.tools.extensions.append("memory")
    assert second.tools.extensions == []


@pytest.mark.parametrize(
    "selection",
    ["all", "none", [], ["memory"], ["memory", "set_state"]],
)
def test_non_root_agent_accepts_extension_tool_selection(
    selection: str | list[str],
) -> None:
    data = _valid_agent_data()
    data["tools"] = {"extensions": selection}

    config = core.AgentConfig(**data)

    assert config.tools.extensions == selection


@pytest.mark.parametrize(
    "selection",
    ["memory", "ALL", [""], ["contains space"], ["tool.name"], ["x" * 65]],
)
def test_agent_config_rejects_invalid_extension_selection(
    selection: str | list[str],
) -> None:
    data = _valid_agent_data()
    data["tools"] = {"extensions": selection}

    with pytest.raises(ValidationError):
        core.AgentConfig(**data)


def test_agent_config_rejects_duplicate_extension_names() -> None:
    data = _valid_agent_data()
    data["tools"] = {"extensions": ["memory", "memory"]}

    with pytest.raises(ValidationError, match="重复"):
        core.AgentConfig(**data)


@pytest.mark.parametrize("selection", ["all", ["memory"]])
def test_root_agent_rejects_enabled_extension_tools(
    selection: str | list[str],
) -> None:
    with pytest.raises(ValidationError, match="root"):
        core.AgentConfig(
            id="root",
            introduction="根节点",
            port=8000,
            tools={"extensions": selection},
        )


@pytest.mark.parametrize("selection", ["none", []])
def test_root_agent_accepts_explicitly_disabled_extension_tools(
    selection: str | list[str],
) -> None:
    config = core.AgentConfig(
        id="root",
        introduction="根节点",
        port=8000,
        tools={"extensions": selection},
    )

    assert config.tools.extensions == selection


@pytest.mark.parametrize("missing_field", ["key", "openai_baseurl", "openai_key", "model"])
def test_non_root_agent_requires_all_credentials(missing_field: str) -> None:
    data = _valid_agent_data()
    del data[missing_field]

    with pytest.raises(ValidationError):
        core.AgentConfig(**data)


@pytest.mark.parametrize(
    ("empty_field", "empty_value"),
    [
        ("key", " "),
        ("openai_baseurl", ""),
        ("openai_key", " "),
        ("model", ""),
    ],
)
def test_non_root_agent_rejects_empty_credentials(
    empty_field: str, empty_value: str
) -> None:
    data = _valid_agent_data()
    data[empty_field] = empty_value

    with pytest.raises(ValidationError):
        core.AgentConfig(**data)


def test_non_root_agent_accepts_complete_credentials() -> None:
    config = core.AgentConfig(**_valid_agent_data())

    assert config.key is not None
    assert config.key.get_secret_value() == "agent-secret"
    assert config.openai_key is not None
    assert config.openai_key.get_secret_value() == "openai-secret"


def test_agent_config_rejects_self_reference() -> None:
    data = _valid_agent_data()
    data["agents"] = [
        {"id": "worker", "port": 9001, "key": "peer-secret"},
    ]

    with pytest.raises(ValidationError):
        core.AgentConfig(**data)


def test_agent_config_rejects_duplicate_peer_ids() -> None:
    data = _valid_agent_data()
    data["agents"] = [
        {"id": "peer", "port": 9001, "key": "first-secret"},
        {"id": "peer", "port": 9002, "key": "second-secret"},
    ]

    with pytest.raises(ValidationError):
        core.AgentConfig(**data)


@pytest.mark.parametrize(
    ("certfile", "keyfile"),
    [("certificate.pem", None), (None, "private-key.pem")],
)
def test_agent_config_requires_tls_files_as_a_pair(
    certfile: str | None, keyfile: str | None
) -> None:
    data = _valid_agent_data()
    data["ssl_certfile"] = certfile
    data["ssl_keyfile"] = keyfile

    with pytest.raises(ValidationError):
        core.AgentConfig(**data)


@pytest.mark.parametrize("introduction", ["", "   ", "x" * 2001])
def test_agent_config_rejects_invalid_introductions(introduction: str) -> None:
    data = _valid_agent_data()
    data["introduction"] = introduction

    with pytest.raises(ValidationError):
        core.AgentConfig(**data)


@pytest.mark.parametrize(
    "field_name",
    [
        "http_timeout_seconds",
        "openai_timeout_seconds",
        "max_tool_calls_per_turn",
        "max_response_steps_per_turn",
        "topology_max_nodes",
        "topology_max_depth",
    ],
)
def test_agent_config_requires_positive_limits_and_timeouts(field_name: str) -> None:
    data = _valid_agent_data()
    data[field_name] = 0

    with pytest.raises(ValidationError):
        core.AgentConfig(**data)


def test_agent_config_max_context_chars_defaults_to_none_and_accepts_positive(
) -> None:
    data = _valid_agent_data()

    assert core.AgentConfig(**data).max_context_chars is None

    data["max_context_chars"] = 4096
    assert core.AgentConfig(**data).max_context_chars == 4096


@pytest.mark.parametrize("bad_value", [0, -1])
def test_agent_config_rejects_non_positive_max_context_chars(
    bad_value: int,
) -> None:
    data = _valid_agent_data()
    data["max_context_chars"] = bad_value

    with pytest.raises(ValidationError):
        core.AgentConfig(**data)


@pytest.mark.parametrize("bad_port", [0, 65536])
def test_agent_config_rejects_ports_outside_tcp_range(bad_port: int) -> None:
    data = _valid_agent_data()
    data["port"] = bad_port

    with pytest.raises(ValidationError):
        core.AgentConfig(**data)


def test_load_agent_config_reads_utf8_and_expands_environment_recursively(
    tmp_path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("INTRO", "环境变量")
    monkeypatch.setenv("AGENT_KEY", "agent-secret")
    monkeypatch.setenv("OPENAI_URL", "https://api.example.test/v1")
    monkeypatch.setenv("OPENAI_KEY", "openai-secret")
    monkeypatch.setenv("MODEL", "test-model")
    monkeypatch.setenv("PEER_HOST", "10.0.0.8")
    monkeypatch.setenv("PEER_KEY", "peer-secret")
    config_path = tmp_path / "agent.json"
    config_path.write_text(
        json.dumps(
            {
                "id": "worker",
                "introduction": "中文-${INTRO}",
                "port": 9000,
                "key": "${AGENT_KEY}",
                "openai_baseurl": "${OPENAI_URL}",
                "openai_key": "${OPENAI_KEY}",
                "model": "prefix-${MODEL}",
                "agents": [
                    {
                        "id": "peer",
                        "ip": "${PEER_HOST}",
                        "port": 9001,
                        "key": "${PEER_KEY}",
                    }
                ],
            },
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )

    config = core.load_agent_config(str(config_path))

    assert config.introduction == "中文-环境变量"
    assert config.key is not None
    assert config.key.get_secret_value() == "agent-secret"
    assert config.model == "prefix-test-model"
    assert config.agents[0].ip == "10.0.0.8"
    assert config.agents[0].key.get_secret_value() == "peer-secret"


def test_load_agent_config_reports_missing_environment_name_without_secret(
    tmp_path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("EXISTING_SECRET", "do-not-leak-this")
    config_path = tmp_path / "missing-env.json"
    config_path.write_text(
        json.dumps(
            {
                "id": "root",
                "introduction": "根节点",
                "port": 8000,
                "key": "${EXISTING_SECRET}-${MISSING_SECRET}",
            },
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )

    with pytest.raises(core.ConfigError) as exc_info:
        core.load_agent_config(config_path)

    assert "MISSING_SECRET" in str(exc_info.value)
    assert "do-not-leak-this" not in str(exc_info.value)


def test_load_agent_config_converts_invalid_json_to_config_error(tmp_path) -> None:
    config_path = tmp_path / "invalid.json"
    config_path.write_text("{not-json", encoding="utf-8")

    with pytest.raises(core.ConfigError, match="JSON"):
        core.load_agent_config(config_path)


def test_load_agent_config_converts_file_errors_to_config_error(tmp_path) -> None:
    missing_path = tmp_path / "does-not-exist.json"

    with pytest.raises(core.ConfigError, match="配置文件"):
        core.load_agent_config(missing_path)


def test_load_agent_config_sanitizes_pydantic_errors(tmp_path) -> None:
    config_path = tmp_path / "invalid-config.json"
    config_path.write_text(
        json.dumps(
            {
                "id": "worker",
                "introduction": "普通节点",
                "port": 0,
                "key": "agent-super-secret",
                "openai_baseurl": "https://api.example.test/v1",
                "openai_key": "openai-super-secret",
                "model": "test-model",
            },
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )

    with pytest.raises(core.ConfigError) as exc_info:
        core.load_agent_config(config_path)

    error_message = str(exc_info.value)
    assert "配置校验失败" in error_message
    assert "agent-super-secret" not in error_message
    assert "openai-super-secret" not in error_message


def test_load_agent_config_drops_secret_bearing_validation_exception_chain(
    tmp_path,
) -> None:
    sentinel_secret = "S3CR3T7F5D"
    config_path = tmp_path / "invalid-secret-config.json"
    config_path.write_text(
        json.dumps(
            {
                "key": sentinel_secret,
                "id": "worker",
                "introduction": "普通节点",
                "port": 9000,
                "openai_baseurl": "https://api.example.test/v1",
                "openai_key": "openai-secret",
                "model": "test-model",
                "agents": [
                    {"id": "peer", "port": 9001, "key": "first-secret"},
                    {"id": "peer", "port": 9002, "key": "second-secret"},
                ],
            },
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )

    with pytest.raises(core.ConfigError) as exc_info:
        core.load_agent_config(config_path)

    error = exc_info.value
    rendered_traceback = "".join(traceback.format_exception(error))
    assert sentinel_secret not in rendered_traceback
    assert error.__cause__ is None
    assert error.__context__ is None


def test_load_agent_config_drops_invalid_json_exception_chain(tmp_path) -> None:
    sentinel_secret = "JSON-CHAIN-SENTINEL-a91c"
    config_path = tmp_path / "invalid-secret-json.json"
    config_path.write_text(
        '{"id":"root","key":"'
        + sentinel_secret
        + '","introduction":"root","port":8000,}',
        encoding="utf-8",
    )

    with pytest.raises(core.ConfigError) as exc_info:
        core.load_agent_config(config_path)

    error = exc_info.value
    rendered_traceback = "".join(traceback.format_exception(error))
    assert sentinel_secret not in rendered_traceback
    assert error.__cause__ is None
    assert error.__context__ is None


@pytest.mark.parametrize("message", ["x", "x" * 65536], ids=["minimum", "maximum"])
def test_message_request_accepts_message_length_boundaries(message: str) -> None:
    conversation_id = uuid4()
    request_id = uuid4()

    request = core.MessageRequest(
        from_id="sender",
        conversation_id=str(conversation_id),
        message=message,
        request_id=str(request_id),
    )

    assert request.conversation_id == conversation_id
    assert isinstance(request.conversation_id, UUID)
    assert request.request_id == request_id
    assert isinstance(request.request_id, UUID)


@pytest.mark.parametrize(
    "message", ["", "x" * 65537], ids=["empty", "too-long"]
)
def test_message_request_rejects_messages_outside_length_limits(message: str) -> None:
    with pytest.raises(ValidationError):
        core.MessageRequest(
            from_id="sender",
            conversation_id=uuid4(),
            message=message,
            request_id=uuid4(),
        )


def test_message_request_requires_a_uuid_conversation_id() -> None:
    with pytest.raises(ValidationError):
        core.MessageRequest(
            from_id="sender",
            conversation_id="not-a-uuid",
            message="消息",
            request_id=uuid4(),
        )


def test_close_request_requires_a_uuid_request_id() -> None:
    with pytest.raises(ValidationError):
        core.CloseRequest(
            from_id="sender",
            conversation_id=uuid4(),
            request_id="not-a-uuid",
        )


def test_close_request_requires_a_uuid_conversation_id() -> None:
    with pytest.raises(ValidationError):
        core.CloseRequest(
            from_id="sender",
            conversation_id="not-a-uuid",
            request_id=uuid4(),
        )


def test_topology_request_preserves_traversal_limits() -> None:
    request = core.TopologyRequest(
        visited_ids=["root", "peer"],
        depth=2,
        max_depth=64,
        max_nodes=1000,
    )

    assert request.visited_ids == ["root", "peer"]
    assert request.depth == 2
    assert request.max_depth == 64
    assert request.max_nodes == 1000


def test_user_message_request_has_optional_uuid_and_message_limits() -> None:
    request = core.UserMessageRequest(message="你好")

    assert request.request_id is None
    with pytest.raises(ValidationError):
        core.UserMessageRequest(message="", request_id=uuid4())
    with pytest.raises(ValidationError):
        core.UserMessageRequest(message="你好", request_id="not-a-uuid")


def test_api_error_detail_defaults_optional_fields_to_none() -> None:
    detail = core.APIErrorDetail(code="invalid_request", message="请求无效")

    assert detail.details is None
    assert detail.retry_after_seconds is None


def test_agent_graph_error_exposes_transport_metadata() -> None:
    error = core.AgentGraphError(
        code="peer_unavailable",
        message="对等节点不可用",
        status_code=503,
        details={"peer_id": "peer"},
        retry_after_seconds=1.5,
    )

    assert str(error) == "对等节点不可用"
    assert error.code == "peer_unavailable"
    assert error.message == "对等节点不可用"
    assert error.status_code == 503
    assert error.details == {"peer_id": "peer"}
    assert error.retry_after_seconds == 1.5


def test_agent_config_defaults_to_no_skills() -> None:
    """省略 skills 时默认空列表，行为与实现前零差异。"""

    config = core.AgentConfig(**_valid_agent_data())
    assert config.skills == []


def test_agent_config_rejects_duplicate_skill_names() -> None:
    """skills 列表不得包含重复名字，与 extensions 去重语义一致。"""

    data = _valid_agent_data()
    data["skills"] = ["research", "research"]
    with pytest.raises(ValidationError):
        core.AgentConfig(**data)


def test_agent_config_rejects_more_than_eight_skills() -> None:
    """启用数量上限 8：封住 instructions 不计入上下文预算的盲区。"""

    data = _valid_agent_data()
    data["skills"] = [f"skill_{index}" for index in range(9)]
    with pytest.raises(ValidationError):
        core.AgentConfig(**data)


def test_root_agent_rejects_enabled_skills() -> None:
    """root 无模型无 instructions，不允许启用 skill。"""

    with pytest.raises(ValidationError):
        core.AgentConfig(
            id="root",
            introduction="根节点",
            port=8000,
            skills=["research"],
        )


def test_root_agent_accepts_explicitly_empty_skills() -> None:
    """root 显式传空 skills 与省略等价，不影响既有配置。"""

    config = core.AgentConfig(
        id="root",
        introduction="根节点",
        port=8000,
        skills=[],
    )
    assert config.skills == []
