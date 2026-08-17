from __future__ import annotations

import asyncio
import builtins
import importlib
import json
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pytest

from tool_system.contract import AgentTool, ToolArguments, ToolSpec


def _load_agent_module() -> Any:
    """延迟导入统一入口，让 RED 清晰报告模块尚未实现。"""

    try:
        return importlib.import_module("Agent")
    except ModuleNotFoundError:
        pytest.fail("Agent 统一入口模块尚未实现")


def _write_config(path: Path, values: dict[str, Any]) -> Path:
    """把测试配置写成 UTF-8 JSON 并返回路径。"""

    path.write_text(json.dumps(values, ensure_ascii=False), encoding="utf-8")
    return path


class _EntryToolArguments(ToolArguments):
    value: str


class _EntryTool(AgentTool):
    spec = ToolSpec(
        name="entry_tool",
        description="Test Agent entry extension injection.",
        arguments_model=_EntryToolArguments,
    )

    async def execute(self, arguments: _EntryToolArguments) -> dict[str, Any]:
        return {"ok": True, "value": arguments.value}


@pytest.mark.asyncio
async def test_agent_factory_selects_user_or_agent_remote_and_exposes_app(
    tmp_path: Path,
) -> None:
    module = _load_agent_module()
    root_path = _write_config(
        tmp_path / "root.json",
        {
            "id": "root",
            "introduction": "本地入口",
            "host": "127.0.0.1",
            "port": 9000,
        },
    )
    worker_path = _write_config(
        tmp_path / "worker.json",
        {
            "id": "worker",
            "introduction": "普通 Agent",
            "host": "127.0.0.1",
            "port": 9100,
            "key": "worker-secret",
            "openai_baseurl": "https://models.example.test/v1",
            "openai_key": "model-secret",
            "model": "test-model",
        },
    )

    root_entry = module.Agent(root_path)
    worker_entry = module.Agent(worker_path)

    assert isinstance(root_entry.runtime, module.User)
    assert root_entry.config.id == "root"
    assert root_entry.app is root_entry.create_app()
    assert isinstance(worker_entry.runtime, module.AgentRemote)
    assert worker_entry.config.id == "worker"
    assert worker_entry.app is worker_entry.create_app()

    async with root_entry.app.router.lifespan_context(root_entry.app):
        pass
    async with worker_entry.app.router.lifespan_context(worker_entry.app):
        pass


@pytest.mark.asyncio
async def test_agent_factory_injects_explicit_extension_catalog_for_workers(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    module = _load_agent_module()
    extension_module = importlib.import_module("ToolExtension")
    monkeypatch.setattr(extension_module, "EXTENSION_TOOLS", (_EntryTool,))
    worker_path = _write_config(
        tmp_path / "worker-tools.json",
        {
            "id": "worker",
            "introduction": "普通 Agent",
            "port": 9100,
            "key": "worker-secret",
            "openai_baseurl": "https://models.example.test/v1",
            "openai_key": "model-secret",
            "model": "test-model",
            "tools": {"extensions": ["entry_tool"]},
        },
    )

    entry = module.Agent(worker_path)

    assert [
        schema["name"] for schema in entry.runtime.tool_registry.schemas()
    ] == ["entry_tool"]
    async with entry.app.router.lifespan_context(entry.app):
        pass


@pytest.mark.asyncio
async def test_root_skips_extension_import_and_worker_import_errors_are_safe(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    module = _load_agent_module()
    root_path = _write_config(
        tmp_path / "root-no-extension-import.json",
        {
            "id": "root",
            "introduction": "本地入口",
            "port": 9000,
        },
    )
    worker_path = _write_config(
        tmp_path / "worker-extension-import-error.json",
        {
            "id": "worker",
            "introduction": "普通 Agent",
            "port": 9100,
            "key": "worker-secret",
            "openai_baseurl": "https://models.example.test/v1",
            "openai_key": "model-secret",
            "model": "test-model",
        },
    )
    original_import = builtins.__import__

    def fail_extension_import(name: str, *args: Any, **kwargs: Any) -> Any:
        if name == "ToolExtension":
            raise RuntimeError("extension-import-secret")
        return original_import(name, *args, **kwargs)

    monkeypatch.setattr(builtins, "__import__", fail_extension_import)

    root_entry = module.Agent(root_path)
    async with root_entry.app.router.lifespan_context(root_entry.app):
        pass

    with pytest.raises(module.ConfigError) as exc_info:
        module.Agent(worker_path)

    assert str(exc_info.value) == "扩展工具目录加载失败"
    assert exc_info.value.__cause__ is None
    assert exc_info.value.__context__ is None


def test_argument_parser_requires_config_and_supports_mutually_exclusive_flags() -> None:
    module = _load_agent_module()
    parser = module._build_argument_parser()

    with pytest.raises(SystemExit) as missing:
        parser.parse_args([])
    assert missing.value.code == 2

    default = parser.parse_args(["--config", "root.json"])
    enabled = parser.parse_args(["--config", "root.json", "--interactive"])
    disabled = parser.parse_args(["--config", "root.json", "--no-interactive"])
    with pytest.raises(SystemExit) as conflicting:
        parser.parse_args(
            ["--config", "root.json", "--interactive", "--no-interactive"]
        )

    assert default.config == "root.json"
    assert default.interactive is None
    assert enabled.interactive is True
    assert disabled.interactive is False
    assert conflicting.value.code == 2


class _FakeStdin:
    """提供可控 isatty 结果的最小 stdin 替身。"""

    def __init__(self, is_tty: bool) -> None:
        self._is_tty = is_tty

    def isatty(self) -> bool:
        return self._is_tty


@pytest.mark.parametrize(
    ("agent_id", "explicit", "is_tty", "expected"),
    [
        ("root", None, True, True),
        ("root", None, False, False),
        ("root", True, False, True),
        ("root", False, True, False),
        ("worker", True, True, False),
        ("worker", None, True, False),
    ],
)
def test_interactive_default_only_enables_root_tty(
    agent_id: str,
    explicit: bool | None,
    is_tty: bool,
    expected: bool,
) -> None:
    module = _load_agent_module()
    config = SimpleNamespace(id=agent_id)

    assert module._should_run_interactive(
        config,
        explicit,
        _FakeStdin(is_tty),
    ) is expected


def test_build_uvicorn_server_uses_host_port_and_tls_from_config() -> None:
    module = _load_agent_module()
    app = object()
    entry = SimpleNamespace(
        app=app,
        config=SimpleNamespace(
            host="0.0.0.0",
            port=9443,
            ssl_certfile=Path("server-cert.pem"),
            ssl_keyfile=Path("server-key.pem"),
        ),
    )

    server = module._build_uvicorn_server(entry)

    assert server.config.app is app
    assert server.config.host == "0.0.0.0"
    assert server.config.port == 9443
    assert server.config.ssl_certfile == Path("server-cert.pem")
    assert server.config.ssl_keyfile == Path("server-key.pem")


class _FakeRootRuntime:
    """记录交互 CLI 调用，不替代被测命令解析逻辑。"""

    def __init__(self) -> None:
        self.calls: list[tuple[Any, ...]] = []

    async def talk_to(
        self,
        message: str,
        to_id: str,
        request_id: str | None = None,
    ) -> str:
        self.calls.append(("talk", to_id, message, request_id))
        return f"回复 {to_id}: {message}"

    async def discover_topology(self) -> dict[str, list]:
        self.calls.append(("topology",))
        return {
            "nodes": [{"id": "root"}],
            "edges": [],
            "errors": [],
        }

    def get_history(self, to_id: str) -> list[dict[str, str]]:
        self.calls.append(("history", to_id))
        return [{"role": "assistant", "content": "历史"}]

    async def close_chat(
        self,
        to_id: str,
        request_id: str | None = None,
    ) -> dict[str, bool]:
        self.calls.append(("close", to_id, request_id))
        return {"closed": True, "saved": True}


class _FakeServer:
    """提供 should_exit 的最小 Uvicorn Server 替身。"""

    def __init__(self) -> None:
        self.should_exit = False


@pytest.mark.asyncio
async def test_interactive_cli_parses_commands_and_reads_input_in_thread(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    module = _load_agent_module()
    runtime = _FakeRootRuntime()
    server = _FakeServer()
    commands = iter(
        [
            "/talk peer-a 包含 空格 的消息",
            "/topology",
            "/history peer-a",
            "/close peer-a",
            "/unknown",
            "/quit",
        ]
    )
    thread_calls: list[tuple[Any, tuple[Any, ...]]] = []
    outputs: list[str] = []

    def input_fn(_prompt: str) -> str:
        return next(commands)

    async def fake_to_thread(function: Any, *args: Any) -> Any:
        thread_calls.append((function, args))
        return function(*args)

    monkeypatch.setattr(module.asyncio, "to_thread", fake_to_thread)

    await module._interactive_loop(
        runtime,
        server,
        input_fn=input_fn,
        print_fn=outputs.append,
    )

    assert runtime.calls == [
        ("talk", "peer-a", "包含 空格 的消息", None),
        ("topology",),
        ("history", "peer-a"),
        ("close", "peer-a", None),
    ]
    assert len(thread_calls) == 6
    assert all(function is input_fn for function, _args in thread_calls)
    assert all(args == ("root> ",) for _function, args in thread_calls)
    assert outputs[0] == "回复 peer-a: 包含 空格 的消息"
    assert json.loads(outputs[1])["nodes"] == [{"id": "root"}]
    assert json.loads(outputs[2]) == [{"role": "assistant", "content": "历史"}]
    assert json.loads(outputs[3]) == {"closed": True, "saved": True}
    assert outputs[4] == "未知命令；可用命令: /talk /topology /history /close /quit"
    assert server.should_exit is True


@pytest.mark.asyncio
async def test_interactive_cli_requires_exact_command_tokens() -> None:
    module = _load_agent_module()
    runtime = _FakeRootRuntime()
    server = _FakeServer()
    commands = iter(
        [
            "/talkative peer-a 消息",
            "/historyx peer-a",
            "/closex peer-a",
            "/quit",
        ]
    )
    outputs: list[str] = []

    await module._interactive_loop(
        runtime,
        server,
        input_fn=lambda _prompt: next(commands),
        print_fn=outputs.append,
    )

    assert runtime.calls == []
    assert outputs == [
        "未知命令；可用命令: /talk /topology /history /close /quit",
        "未知命令；可用命令: /talk /topology /history /close /quit",
        "未知命令；可用命令: /talk /topology /history /close /quit",
    ]
    assert server.should_exit is True


class _GracefulFakeServer:
    """模拟一直运行到 should_exit，并记录是否完成清理。"""

    def __init__(self) -> None:
        self.should_exit = False
        self.started = asyncio.Event()
        self.stopped = asyncio.Event()

    async def serve(self) -> None:
        self.started.set()
        while not self.should_exit:
            await asyncio.sleep(0)
        self.stopped.set()


@pytest.mark.asyncio
async def test_run_agent_waits_for_server_to_finish_after_quit(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    module = _load_agent_module()
    server = _GracefulFakeServer()
    runtime = _FakeRootRuntime()
    entry = SimpleNamespace(runtime=runtime)
    commands = iter(["/quit"])

    monkeypatch.setattr(module, "_build_uvicorn_server", lambda _entry: server)

    await asyncio.wait_for(
        module._run_agent(
            entry,
            interactive=True,
            input_fn=lambda _prompt: next(commands),
            print_fn=lambda _value: None,
        ),
        timeout=1,
    )

    assert server.started.is_set()
    assert server.should_exit is True
    assert server.stopped.is_set()
