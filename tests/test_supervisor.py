"""AgentSupervisor 的确定性单元测试：注入 spawn/probe，不启动真实进程。"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pytest

from core import AgentGraphError
from supervisor import AgentSupervisor


class FakeProc:
    """最小进程替身：poll/terminate/wait 语义与 asyncio 子进程一致。"""

    def __init__(self, pid: int = 4242) -> None:
        self.pid = pid
        self._alive = True
        self.returncode: int | None = None
        self.terminated = False
        self.killed = False

    def poll(self) -> int | None:
        return None if self._alive else (0 if self.returncode is None else self.returncode)

    def terminate(self) -> None:
        self.terminated = True
        self._alive = False
        self.returncode = 0

    def kill(self) -> None:
        self.killed = True
        self._alive = False
        self.returncode = -9

    async def wait(self) -> int | None:
        return self.returncode

    def crash(self, code: int = 3) -> None:
        self._alive = False
        self.returncode = code


def _write_config(directory: Path, name: str, port: int) -> None:
    (directory / name).write_text(
        json.dumps(
            {
                "id": name.removesuffix(".json"),
                "introduction": f"intro {name}",
                "host": "127.0.0.1",
                "port": port,
            }
        ),
        encoding="utf-8",
    )


@pytest.fixture
def rig(tmp_path: Path) -> dict[str, Any]:
    agents_dir = tmp_path / "agents_setting"
    agents_dir.mkdir()
    _write_config(agents_dir, "Agent1.json", 9861)
    _write_config(agents_dir, "Agent2.json", 9862)
    (agents_dir / "broken.json").write_text("{", encoding="utf-8")
    (agents_dir / "root.json").write_text(
        json.dumps({"id": "root", "introduction": "r", "port": 9860}),
        encoding="utf-8",
    )

    spawned_argv: list[list[str]] = []
    running_ports: set[int] = set()
    procs: dict[str, FakeProc] = {}

    async def fake_spawn(argv: list[str], **kwargs: Any) -> FakeProc:
        config_path = argv[argv.index("--config") + 1]
        proc = FakeProc(pid=1000 + len(spawned_argv))
        spawned_argv.append(list(argv))
        procs[config_path] = proc
        return proc

    async def fake_probe(host: str, port: int) -> bool:
        return port in running_ports

    supervisor = AgentSupervisor(
        agents_dir,
        repo_root=tmp_path / "repo",
        log_dir=tmp_path / "logs",
        spawn=fake_spawn,
        probe=fake_probe,
    )
    return {
        "supervisor": supervisor,
        "agents_dir": agents_dir,
        "argvs": spawned_argv,
        "running_ports": running_ports,
        "procs": procs,
    }


async def test_roster_excludes_root_and_broken_configs(rig: dict[str, Any]) -> None:
    agents = await rig["supervisor"].status()
    assert [a["id"] for a in agents] == ["Agent1", "Agent2"]
    assert all(a["state"] == "stopped" and a["ours"] is False for a in agents)


async def test_external_running_agent_is_running_not_ours(rig: dict[str, Any]) -> None:
    rig["running_ports"].add(9861)
    agents = await rig["supervisor"].status()
    by_id = {a["id"]: a for a in agents}
    assert by_id["Agent1"]["state"] == "running"
    assert by_id["Agent1"]["ours"] is False
    assert by_id["Agent2"]["state"] == "stopped"


async def test_start_spawns_agent_py_with_config(rig: dict[str, Any]) -> None:
    result = await rig["supervisor"].start("Agent1")
    assert result["started"] is True
    assert result["state"] == "starting"
    argv = rig["argvs"][0]
    assert argv[1].endswith("Agent.py")
    assert argv[argv.index("--config") + 1].endswith("Agent1.json")
    assert "--no-interactive" in argv
    agents = await rig["supervisor"].status()
    assert {a["id"]: a["state"] for a in agents}["Agent1"] == "starting"


async def test_start_is_idempotent_while_alive_or_running(rig: dict[str, Any]) -> None:
    await rig["supervisor"].start("Agent1")
    await rig["supervisor"].start("Agent1")
    assert len(rig["argvs"]) == 1
    rig["running_ports"].add(9861)
    result = await rig["supervisor"].start("Agent1")
    assert result == {
        "started": False,
        "state": "running",
        "reason": "already_running",
    }
    assert len(rig["argvs"]) == 1


async def test_start_unknown_agent_raises_not_found(rig: dict[str, Any]) -> None:
    with pytest.raises(AgentGraphError) as exc_info:
        await rig["supervisor"].start("Agent9")
    assert exc_info.value.code == "AGENT_NOT_FOUND"
    assert exc_info.value.status_code == 404


async def test_crashed_agent_reports_exit_code_and_log_tail(
    rig: dict[str, Any],
) -> None:
    await rig["supervisor"].start("Agent1")
    rig["logs"] = None  # 明确日志目录由 fixture 提供
    (rig["supervisor"].log_dir / "Agent1.log").write_bytes(
        "配置错误: 缺少 OPENAI_API_KEY\n".encode("utf-8"),
    )
    proc = next(iter(rig["procs"].values()))
    proc.crash(code=2)
    agents = await rig["supervisor"].status()
    crashed = {a["id"]: a for a in agents}["Agent1"]
    assert crashed["state"] == "crashed"
    assert crashed["exit_code"] == 2
    assert "OPENAI_API_KEY" in crashed["last_log"]
    # 崩溃后可以再次拉起
    result = await rig["supervisor"].start("Agent1")
    assert result["started"] is True
    assert len(rig["argvs"]) == 2


async def test_stop_requires_ownership(rig: dict[str, Any]) -> None:
    rig["running_ports"].add(9861)
    with pytest.raises(AgentGraphError) as exc_info:
        await rig["supervisor"].stop("Agent1")
    assert exc_info.value.code == "AGENT_NOT_OWNED"
    assert exc_info.value.status_code == 409


async def test_stop_terminates_owned_process(rig: dict[str, Any]) -> None:
    await rig["supervisor"].start("Agent1")
    proc = next(iter(rig["procs"].values()))
    result = await rig["supervisor"].stop("Agent1")
    assert result == {"stopped": True, "state": "stopped"}
    assert proc.terminated is True
    # 已停止时幂等
    again = await rig["supervisor"].stop("Agent1")
    assert again == {"stopped": False, "state": "stopped"}


async def test_close_terminates_all_owned_processes(rig: dict[str, Any]) -> None:
    await rig["supervisor"].start("Agent1")
    await rig["supervisor"].start("Agent2")
    await rig["supervisor"].close()
    assert all(p.terminated for p in rig["procs"].values())


async def test_missing_agents_dir_yields_empty_roster(tmp_path: Path) -> None:
    supervisor = AgentSupervisor(
        tmp_path / "nope",
        log_dir=tmp_path / "logs",
        probe=_never_probe,
    )
    assert await supervisor.status() == []


async def _never_probe(host: str, port: int) -> bool:
    return False
