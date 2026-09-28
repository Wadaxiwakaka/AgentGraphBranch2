"""root 进程的本地 Agent 监督者：枚举配置目录并按需拉起/停止节点子进程。

安全边界（与 root API 无鉴权的前提一致，全部能力只应暴露在回环地址）：
- 只会执行 ``<repo>/Agent.py --config <agents_dir 内的 *.json> --no-interactive``，
  不接受任意路径、任意命令或环境注入；
- 只终止由本实例拉起的进程；外部手动启动的 Agent 只读探测，不可停止；
- 子进程继承 root 进程环境变量（含 ``OPENAI_API_KEY``），stdout/stderr 追加写入
  ``agent_logs/<id>.log`` 供失败诊断。
"""

from __future__ import annotations

import asyncio
import json
import os
import re
import subprocess
import sys
from pathlib import Path
from typing import Any, Awaitable, Callable

import httpx

from core import AgentGraphError

_AGENT_ID = re.compile(r"^[A-Za-z0-9_-]{1,64}$")
_LOG_TAIL_BYTES = 2000
_STOP_TIMEOUT_SECONDS = 10.0
_PROBE_TIMEOUT_SECONDS = 1.0

Probe = Callable[[str, int], Awaitable[bool]]
Spawn = Callable[..., Awaitable[Any]]


class AgentSupervisor:
    """扫描 ``agents_dir`` 下的节点 JSON，管理本机 Agent 子进程生命周期。

    参数:
        agents_dir: 存放各节点 JSON 配置的目录；root 自身配置也应位于其中，
            但 roster 会排除 ``id=root``。
        repo_root: ``Agent.py`` 所在目录，同时作为子进程 cwd；默认为本文件目录。
        log_dir: 子进程输出日志目录；默认 ``<repo_root>/agent_logs``。
        spawn: 可注入的子进程工厂，签名同 ``_default_spawn``；测试用。
        probe: 可注入的 healthz 探测函数 ``(host, port) -> bool``；测试用。

    返回值:
        构造后公开 ``agents_dir`` 与 ``log_dir`` 属性，其余状态私有。

    异常:
        构造本身不抛出；目录缺失时 roster 为空，不视为错误。

    状态变化:
        默认创建一个仅用于 healthz 探测的 ``httpx.AsyncClient``（注入 probe 时
        不创建），由 ``close()`` 统一关闭。
    """

    def __init__(
        self,
        agents_dir: Path,
        *,
        repo_root: Path | None = None,
        log_dir: Path | None = None,
        spawn: Spawn | None = None,
        probe: Probe | None = None,
    ) -> None:
        self.agents_dir = Path(agents_dir)
        self.repo_root = Path(repo_root) if repo_root else Path(__file__).resolve().parent
        self.log_dir = Path(log_dir) if log_dir else self.repo_root / "agent_logs"
        self._spawn = spawn or self._default_spawn
        self._probe = probe or self._default_probe
        self._spawned: dict[str, dict[str, Any]] = {}
        self._lock = asyncio.Lock()
        self._owns_client = probe is None
        self._client = httpx.AsyncClient(timeout=_PROBE_TIMEOUT_SECONDS) if self._owns_client else None

    async def _default_spawn(
        self,
        argv: list[str],
        *,
        cwd: str,
        stdout: Any,
        stderr: Any,
        creationflags: int,
    ) -> Any:
        return await asyncio.create_subprocess_exec(
            *argv,
            cwd=cwd,
            stdout=stdout,
            stderr=stderr,
            creationflags=creationflags,
        )

    async def _default_probe(self, host: str, port: int) -> bool:
        assert self._client is not None
        try:
            response = await self._client.get(f"http://{host}:{port}/healthz")
        except (httpx.HTTPError, OSError, ValueError):
            return False
        return response.status_code == 200

    def _roster(self) -> list[dict[str, Any]]:
        """读取 agents_dir 下的合法节点配置；坏文件与 root 自身被静默排除。"""

        roster: list[dict[str, Any]] = []
        if not self.agents_dir.is_dir():
            return roster
        for path in sorted(self.agents_dir.glob("*.json")):
            try:
                data = json.loads(path.read_text(encoding="utf-8"))
            except (OSError, ValueError):
                continue
            if not isinstance(data, dict):
                continue
            agent_id = data.get("id")
            port = data.get("port")
            host = data.get("host")
            if not isinstance(agent_id, str) or _AGENT_ID.fullmatch(agent_id) is None:
                continue
            if agent_id == "root":
                continue
            if (
                not isinstance(port, int)
                or isinstance(port, bool)
                or not 1 <= port <= 65535
            ):
                continue
            introduction = data.get("introduction")
            roster.append(
                {
                    "id": agent_id,
                    "host": host if isinstance(host, str) and host else "127.0.0.1",
                    "port": port,
                    "introduction": (
                        introduction if isinstance(introduction, str) else ""
                    ),
                    "config_path": str(path),
                },
            )
        return roster

    def _entry(self, agent_id: str) -> dict[str, Any] | None:
        return next((e for e in self._roster() if e["id"] == agent_id), None)

    def _log_tail(self, agent_id: str) -> str:
        try:
            with open(self.log_dir / f"{agent_id}.log", "rb") as handle:
                handle.seek(0, 2)
                size = handle.tell()
                handle.seek(max(0, size - _LOG_TAIL_BYTES))
                return handle.read().decode("utf-8", errors="replace")
        except OSError:
            return ""

    async def status(self) -> list[dict[str, Any]]:
        """返回 roster 中每个 Agent 的运行状态视图。

        参数:
            无。

        返回值:
            每项含 ``id/host/port/introduction/config_path/state/ours``；
            ``state`` 取 ``running``（healthz 可达）、``starting``（本实例已拉起、
            端口未就绪）、``crashed``（本实例拉起后退出，附 ``exit_code`` 与
            ``last_log``）或 ``stopped``。

        异常:
            roster 中的坏配置不抛出；探测异常按不可达处理。

        状态变化:
            不修改进程表；仅发起只读 healthz 请求。
        """

        roster = self._roster()
        alive_flags = await asyncio.gather(
            *(self._probe(entry["host"], entry["port"]) for entry in roster),
        )
        result: list[dict[str, Any]] = []
        for entry, reachable in zip(roster, alive_flags):
            record = self._spawned.get(entry["id"])
            proc = record["proc"] if record is not None else None
            if reachable:
                state, extra = "running", {}
            elif proc is not None and proc.poll() is None:
                state, extra = "starting", {}
            elif proc is not None:
                state = "crashed"
                extra = {
                    "exit_code": proc.returncode,
                    "last_log": self._log_tail(entry["id"]),
                }
            else:
                state, extra = "stopped", {}
            result.append(
                {
                    **entry,
                    "state": state,
                    "ours": record is not None and proc.poll() is None,
                    **extra,
                },
            )
        return result

    async def start(self, agent_id: str) -> dict[str, Any]:
        """拉起 roster 中的一个 Agent 子进程。

        参数:
            agent_id: 目标节点 id，必须存在于 ``agents_dir`` 的合法配置中。

        返回值:
            ``started`` 布尔、``state`` 与可选 ``pid``；目标已在运行时
            ``started=False`` 且 ``reason=already_running``，不重复拉起。

        异常:
            AgentGraphError: 目标不在 roster 时 404；子进程创建失败时 500。

        状态变化:
            写入 ``agent_logs/<id>.log`` 并登记进程表；已有崩溃记录会被覆盖。
            端口就绪前状态为 ``starting``，由 ``status()`` 轮询收敛。
        """

        async with self._lock:
            entry = self._entry(agent_id)
            if entry is None:
                raise AgentGraphError(
                    code="AGENT_NOT_FOUND",
                    message="配置目录中没有该 Agent",
                    status_code=404,
                )
            if await self._probe(entry["host"], entry["port"]):
                return {
                    "started": False,
                    "state": "running",
                    "reason": "already_running",
                }
            old = self._spawned.get(agent_id)
            if old is not None and old["proc"].poll() is None:
                return {"started": True, "state": "starting", "pid": old["proc"].pid}
            if old is not None:
                old["log_handle"].close()

            self.log_dir.mkdir(parents=True, exist_ok=True)
            log_handle = open(self.log_dir / f"{agent_id}.log", "ab", buffering=0)
            creationflags = (
                subprocess.CREATE_NO_WINDOW if os.name == "nt" else 0
            )
            argv = [
                sys.executable,
                str(self.repo_root / "Agent.py"),
                "--config",
                entry["config_path"],
                "--no-interactive",
            ]
            try:
                proc = await self._spawn(
                    argv,
                    cwd=str(self.repo_root),
                    stdout=log_handle,
                    stderr=subprocess.STDOUT,
                    creationflags=creationflags,
                )
            except Exception:
                log_handle.close()
                raise AgentGraphError(
                    code="AGENT_START_FAILED",
                    message="Agent 子进程启动失败",
                    status_code=500,
                ) from None
            self._spawned[agent_id] = {"proc": proc, "log_handle": log_handle}
            return {"started": True, "state": "starting", "pid": proc.pid}

    async def stop(self, agent_id: str) -> dict[str, Any]:
        """停止一个由本实例拉起的 Agent 子进程。

        参数:
            agent_id: 目标节点 id。

        返回值:
            ``stopped`` 布尔与 ``state``；目标本就未运行时 ``stopped=False``。

        异常:
            AgentGraphError: 目标不在 roster 时 404；目标在运行但不是本实例
            拉起（外部进程）时 409 ``AGENT_NOT_OWNED``，绝不猜测 PID。

        状态变化:
            terminate 后等待退出（超时则 kill），关闭日志句柄并移除进程表记录。
        """

        async with self._lock:
            entry = self._entry(agent_id)
            if entry is None:
                raise AgentGraphError(
                    code="AGENT_NOT_FOUND",
                    message="配置目录中没有该 Agent",
                    status_code=404,
                )
            record = self._spawned.get(agent_id)
            proc = record["proc"] if record is not None else None
            if proc is None or proc.poll() is not None:
                if await self._probe(entry["host"], entry["port"]):
                    raise AgentGraphError(
                        code="AGENT_NOT_OWNED",
                        message="该 Agent 不是由本 root 拉起，无法停止",
                        status_code=409,
                    )
                return {"stopped": False, "state": "stopped"}

            proc.terminate()
            try:
                await asyncio.wait_for(proc.wait(), timeout=_STOP_TIMEOUT_SECONDS)
            except asyncio.TimeoutError:
                proc.kill()
                await proc.wait()
            record["log_handle"].close()
            self._spawned.pop(agent_id, None)
            return {"stopped": True, "state": "stopped"}

    async def close(self) -> None:
        """root 关闭时统一终止本实例拉起的全部子进程并释放资源。"""

        for record in list(self._spawned.values()):
            proc = record["proc"]
            if proc.poll() is None:
                proc.terminate()
                try:
                    await asyncio.wait_for(
                        proc.wait(),
                        timeout=_STOP_TIMEOUT_SECONDS,
                    )
                except asyncio.TimeoutError:
                    proc.kill()
                    await proc.wait()
            record["log_handle"].close()
        self._spawned.clear()
        if self._owns_client and self._client is not None:
            await self._client.aclose()
