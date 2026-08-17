from __future__ import annotations

import json
from datetime import datetime
from pathlib import Path
from uuid import UUID

import pytest
from pydantic import BaseModel

import core


class _SDKMessageItem(BaseModel):
    type: str
    role: str
    content: list[dict[str, object]]
    status: str | None = None


def test_chat_space_initializes_public_compatibility_state(tmp_path: Path) -> None:
    tools = [{"type": "function", "name": "lookup"}]

    chat = core.ChatSpace(
        owner_id="owner",
        peer_id="peer",
        instructions="只回答事实。",
        tools=tools,
        storage_root=tmp_path,
    )

    assert chat.id == "peer"
    assert chat.messages == []
    assert chat.instructions == "只回答事实。"
    assert chat.tools == tools
    assert chat.context_items == []
    assert str(UUID(chat.conversation_id)) == chat.conversation_id


def test_add_msg_updates_readable_and_responses_api_views(tmp_path: Path) -> None:
    chat = core.ChatSpace(owner_id="owner", peer_id="peer", storage_root=tmp_path)

    chat.add_msg("用户问题", "user")
    chat.add_msg("直接回答", "assistant")
    chat.add_msg("代理回答", "agent")

    assert chat.messages == [
        {"role": "user", "content": "用户问题"},
        {"role": "assistant", "content": "直接回答"},
        {"role": "assistant", "content": "代理回答"},
    ]
    assert chat.context_items == [
        {"type": "message", "role": "user", "content": "用户问题"},
        {"type": "message", "role": "assistant", "content": "直接回答"},
        {"type": "message", "role": "assistant", "content": "代理回答"},
    ]


def test_add_msg_rejects_unknown_roles_without_mutating_state(tmp_path: Path) -> None:
    chat = core.ChatSpace(owner_id="owner", peer_id="peer", storage_root=tmp_path)

    with pytest.raises(ValueError, match="role"):
        chat.add_msg("系统消息", "system")

    assert chat.messages == []
    assert chat.context_items == []


def test_append_response_items_preserves_full_items_and_extracts_text_once(
    tmp_path: Path,
) -> None:
    chat = core.ChatSpace(owner_id="owner", peer_id="peer", storage_root=tmp_path)
    function_call = {
        "type": "function_call",
        "id": "fc_1",
        "call_id": "call_1",
        "name": "lookup",
        "arguments": '{"query":"AgentGraph"}',
        "status": "completed",
    }
    reasoning = {
        "type": "reasoning",
        "id": "rs_1",
        "summary": [{"type": "summary_text", "text": "检查事实"}],
        "encrypted_content": "encrypted-reasoning",
        "status": "completed",
    }
    message = _SDKMessageItem(
        type="message",
        role="assistant",
        content=[
            {"type": "output_text", "text": "第一段"},
            {"type": "output_text", "text": "第二段"},
        ],
    )

    chat.append_response_items([function_call, reasoning, message])

    assert chat.context_items == [
        function_call,
        reasoning,
        {
            "type": "message",
            "role": "assistant",
            "content": [
                {"type": "output_text", "text": "第一段"},
                {"type": "output_text", "text": "第二段"},
            ],
        },
    ]
    assert chat.messages == [{"role": "assistant", "content": "第一段第二段"}]
    assert len(chat.context_items) == 3


def test_append_tool_output_adds_responses_api_item(tmp_path: Path) -> None:
    chat = core.ChatSpace(owner_id="owner", peer_id="peer", storage_root=tmp_path)

    chat.append_tool_output("call_1", {"result": "成功"})

    assert chat.context_items == [
        {
            "type": "function_call_output",
            "call_id": "call_1",
            "output": {"result": "成功"},
        }
    ]
    assert chat.messages == []


def test_get_context_messages_returns_a_deep_copy(tmp_path: Path) -> None:
    chat = core.ChatSpace(owner_id="owner", peer_id="peer", storage_root=tmp_path)
    chat.append_response_items(
        [
            {
                "type": "function_call",
                "call_id": "call_1",
                "name": "lookup",
                "arguments": {"nested": {"query": "original"}},
            }
        ]
    )

    returned_context = chat.get_context_messages()
    returned_context[0]["arguments"]["nested"]["query"] = "tampered"

    assert chat.context_items[0]["arguments"]["nested"]["query"] == "original"


def test_save_writes_complete_utf8_json_atomically_to_expected_path(
    tmp_path: Path,
) -> None:
    storage_root = tmp_path / "chat_history"
    tools = [{"type": "function", "name": "查询"}]
    chat = core.ChatSpace(
        owner_id="owner",
        peer_id="peer",
        instructions="请使用中文回答。",
        tools=tools,
        storage_root=storage_root,
    )
    chat.add_msg("你好，世界", "user")
    chat.append_response_items(
        [
            {
                "type": "reasoning",
                "id": "rs_1",
                "encrypted_content": "encrypted-reasoning",
            },
            {
                "type": "message",
                "role": "assistant",
                "content": [{"type": "output_text", "text": "你好！"}],
            },
        ]
    )

    saved_path = chat.save()

    expected_path = (
        storage_root / "owner" / "peer" / f"{chat.conversation_id}.json"
    )
    assert saved_path == expected_path
    raw_bytes = saved_path.read_bytes()
    assert "你好，世界".encode("utf-8") in raw_bytes
    assert "请使用中文回答。".encode("utf-8") in raw_bytes

    payload = json.loads(raw_bytes.decode("utf-8"))
    assert payload["schema_version"] == 1
    assert payload["owner_id"] == "owner"
    assert payload["peer_id"] == "peer"
    assert payload["id"] == "peer"
    assert payload["conversation_id"] == chat.conversation_id
    assert datetime.fromisoformat(payload["saved_at"])
    assert payload["instructions"] == "请使用中文回答。"
    assert payload["tools"] == tools
    assert payload["messages"] == chat.messages
    assert payload["context_items"] == chat.context_items
    assert list(saved_path.parent.glob("*.tmp")) == []

    assert chat.save() == saved_path
    assert list(saved_path.parent.glob("*.tmp")) == []


def test_save_preserves_old_file_and_cleans_temp_when_replace_fails(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    storage_root = tmp_path / "chat_history"
    chat = core.ChatSpace(
        owner_id="owner",
        peer_id="peer",
        storage_root=storage_root,
    )
    target_path = (
        storage_root / "owner" / "peer" / f"{chat.conversation_id}.json"
    )
    target_path.parent.mkdir(parents=True)
    old_contents = b'{"old":"complete"}\n'
    target_path.write_bytes(old_contents)

    def fail_replace(source, destination) -> None:
        assert Path(source).parent == target_path.parent
        assert Path(source).suffix == ".tmp"
        assert Path(source).is_file()
        assert Path(destination) == target_path
        raise OSError("injected replace failure")

    monkeypatch.setattr(core.os, "replace", fail_replace)

    with pytest.raises(OSError, match="injected replace failure"):
        chat.save()

    assert target_path.read_bytes() == old_contents
    assert list(target_path.parent.glob("*.tmp")) == []


def test_save_uses_same_directory_os_replace_on_success(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    storage_root = tmp_path / "chat_history"
    chat = core.ChatSpace(
        owner_id="owner",
        peer_id="peer",
        storage_root=storage_root,
    )
    real_replace = core.os.replace
    replace_calls: list[tuple[Path, Path]] = []

    def record_replace(source, destination) -> None:
        source_path = Path(source)
        destination_path = Path(destination)
        replace_calls.append((source_path, destination_path))
        real_replace(source_path, destination_path)

    monkeypatch.setattr(core.os, "replace", record_replace)

    saved_path = chat.save()

    assert len(replace_calls) == 1
    temporary_path, destination_path = replace_calls[0]
    assert temporary_path.parent == saved_path.parent
    assert temporary_path.suffix == ".tmp"
    assert destination_path == saved_path
    assert not temporary_path.exists()
    assert json.loads(saved_path.read_text(encoding="utf-8"))["schema_version"] == 1


def test_clear_only_empties_conversation_content(tmp_path: Path) -> None:
    tools = [{"type": "function", "name": "lookup"}]
    chat = core.ChatSpace(
        owner_id="owner",
        peer_id="peer",
        instructions="保持简洁。",
        tools=tools,
        storage_root=tmp_path,
    )
    conversation_id = chat.conversation_id
    chat.add_msg("问题", "user")
    chat.append_tool_output("call_1", "结果")

    chat.clear()

    assert chat.messages == []
    assert chat.context_items == []
    assert chat.id == "peer"
    assert chat.conversation_id == conversation_id
    assert chat.instructions == "保持简洁。"
    assert chat.tools == tools
