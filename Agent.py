"""AgentGraph 普通节点与 root 网关共用的进程入口。"""

from __future__ import annotations

import argparse
import asyncio
import json
import sys
from pathlib import Path
from typing import Any, Callable, Sequence, TextIO

import uvicorn
from fastapi import FastAPI

from AgentRemote import AgentRemote
from User import User
from core import AgentConfig, AgentGraphError, ConfigError, load_agent_config


class Agent:
    """从单个配置文件组合出 root 或普通 Agent 的统一运行对象。

    参数:
        config_path: UTF-8 JSON 配置文件路径，可传字符串或 ``Path``。

    返回值:
        构造完成后公开经过校验的 ``config``、按 id 选择的 ``runtime``，以及该
        runtime 创建的单一 ``app`` 实例。

    异常:
        ConfigError: 配置或普通 Agent 的扩展工具目录加载失败时抛出安全异常。
        ValueError: 已校验配置与所选运行时的身份约束冲突时传播。

    状态变化:
        读取配置文件；root 创建 ``User``，其它 id 创建 ``AgentRemote``，并立即
        构造对应 FastAPI 应用，但不会启动监听端口。
    """

    def __init__(self, config_path: str | Path) -> None:
        """加载配置并选择唯一的运行时实现。

        参数:
            config_path: 要交给 ``load_agent_config`` 的配置路径。

        返回值:
            ``None``；结果保存在 ``config``、``runtime`` 和 ``app`` 属性中。

        异常:
            ConfigError: 配置或扩展工具目录加载失败时传播。
            ValueError: 运行时拒绝不匹配的配置时传播。

        状态变化:
            普通 Agent 可能创建其自有 HTTP/OpenAI 客户端；root 只创建 HTTP 客户端。
            两类客户端均由各自 FastAPI lifespan 在服务退出时负责关闭。
        """

        self.config = load_agent_config(config_path)
        self.runtime: User | AgentRemote
        if self.config.id == "root":
            self.runtime = User(self.config)
        else:
            extension_import_failed = False
            try:
                from ToolExtension import EXTENSION_TOOLS
            except Exception:
                extension_import_failed = True
            if extension_import_failed:
                raise ConfigError("扩展工具目录加载失败")
            self.runtime = AgentRemote(
                self.config,
                extension_tool_classes=EXTENSION_TOOLS,
            )
        self.app = self.runtime.create_app()

    def create_app(self) -> FastAPI:
        """返回构造期间创建的 FastAPI 应用实例。

        参数:
            无。

        返回值:
            与公开 ``app`` 属性相同的稳定对象；重复调用不会重复注册路由或 lifespan。

        异常:
            本方法只返回现有对象，正常情况下不抛出异常。

        状态变化:
            无；不会新建客户端、应用或监听端口。
        """

        return self.app


def _build_argument_parser() -> argparse.ArgumentParser:
    """创建命令行参数解析器，并保留三态 interactive 选择。"""

    parser = argparse.ArgumentParser(description="启动 AgentGraph 节点")
    parser.add_argument("--config", required=True, help="Agent JSON 配置文件路径")
    interactive_group = parser.add_mutually_exclusive_group()
    interactive_group.add_argument(
        "--interactive",
        dest="interactive",
        action="store_true",
        help="为 root 显式启用交互 CLI",
    )
    interactive_group.add_argument(
        "--no-interactive",
        dest="interactive",
        action="store_false",
        help="显式禁用交互 CLI",
    )
    parser.set_defaults(interactive=None)
    return parser


def _should_run_interactive(
    config: AgentConfig,
    explicit: bool | None,
    stdin: TextIO,
) -> bool:
    """根据身份、显式参数和 TTY 状态计算是否进入 root CLI。"""

    if config.id != "root":
        return False
    if explicit is not None:
        return explicit
    return stdin.isatty()


def _build_uvicorn_server(agent: Agent) -> uvicorn.Server:
    """从 AgentConfig 原样构造 Uvicorn 配置，但不启动网络监听。"""

    config = uvicorn.Config(
        agent.app,
        host=agent.config.host,
        port=agent.config.port,
        ssl_certfile=agent.config.ssl_certfile,
        ssl_keyfile=agent.config.ssl_keyfile,
    )
    return uvicorn.Server(config)


def _json_text(value: Any) -> str:
    """把 CLI 结构化结果转换为易读且不转义中文的 JSON。"""

    return json.dumps(value, ensure_ascii=False, indent=2)


def _cli_error_payload(error: AgentGraphError) -> dict[str, Any]:
    """只挑选 AgentGraphError 中允许展示的稳定安全字段。"""

    payload: dict[str, Any] = {
        "code": error.code,
        "message": error.message,
    }
    if error.details is not None:
        payload["details"] = error.details
    if error.retry_after_seconds is not None:
        payload["retry_after_seconds"] = error.retry_after_seconds
    return {"error": payload}


async def _interactive_loop(
    runtime: User,
    server: uvicorn.Server,
    *,
    input_fn: Callable[[str], str] | None = None,
    print_fn: Callable[[str], Any] = print,
) -> None:
    """解析 root CLI 命令，直到用户退出或输入流结束。"""

    actual_input = input if input_fn is None else input_fn
    while not server.should_exit:
        try:
            # input 是同步阻塞调用，必须放在线程中，避免冻结 Uvicorn 事件循环和所有
            # 正在处理的 HTTP 请求；取消协程也不会把用户消息或密钥写入日志。
            raw_line = await asyncio.to_thread(actual_input, "root> ")
        except (EOFError, KeyboardInterrupt):
            server.should_exit = True
            return

        line = raw_line.strip()
        if not line:
            continue
        if line == "/quit":
            server.should_exit = True
            return

        try:
            command = line.split(maxsplit=1)[0]
            if command == "/talk":
                parts = line.split(maxsplit=2)
                if len(parts) != 3 or not parts[1] or not parts[2]:
                    print_fn("用法: /talk <to_id> <message>")
                    continue
                answer = await runtime.talk_to(parts[2], parts[1])
                print_fn(answer)
                continue

            if line == "/topology":
                print_fn(_json_text(await runtime.discover_topology()))
                continue

            if command == "/history":
                parts = line.split(maxsplit=1)
                if len(parts) != 2 or not parts[1]:
                    print_fn("用法: /history <to_id>")
                    continue
                print_fn(_json_text(runtime.get_history(parts[1])))
                continue

            if command == "/close":
                parts = line.split(maxsplit=1)
                if len(parts) != 2 or not parts[1]:
                    print_fn("用法: /close <to_id>")
                    continue
                print_fn(_json_text(await runtime.close_chat(parts[1])))
                continue

            print_fn("未知命令；可用命令: /talk /topology /history /close /quit")
        except AgentGraphError as error:
            print_fn(_json_text(_cli_error_payload(error)))
        except Exception:
            # 未知异常可能持有请求、URL 或第三方对象；CLI 只输出固定安全说明。
            print_fn("命令执行失败")


async def _run_agent(
    agent: Agent,
    *,
    interactive: bool,
    input_fn: Callable[[str], str] | None = None,
    print_fn: Callable[[str], Any] = print,
) -> None:
    """启动 Uvicorn，并在需要时并行运行 root 交互循环。"""

    server = _build_uvicorn_server(agent)
    if not interactive:
        await server.serve()
        return

    # gather 会在 /quit 设置 should_exit 后继续等待 server.serve 完成 lifespan shutdown，
    # 从而保证会话保存和自有客户端关闭完成后进程才退出。
    await asyncio.gather(
        server.serve(),
        _interactive_loop(
            agent.runtime,
            server,
            input_fn=input_fn,
            print_fn=print_fn,
        ),
    )


async def _async_main(args: argparse.Namespace) -> None:
    """构造 Agent 并执行一次异步服务生命周期。"""

    agent = Agent(args.config)
    interactive = _should_run_interactive(
        agent.config,
        args.interactive,
        sys.stdin,
    )
    await _run_agent(agent, interactive=interactive)


def main(argv: Sequence[str] | None = None) -> int:
    """解析命令行并启动 AgentGraph 统一进程入口。

    参数:
        argv: 可选参数序列；省略时由 ``argparse`` 读取当前进程 ``sys.argv``。

    返回值:
        服务正常退出时返回 0；配置错误返回 2；键盘中断返回 130。

    异常:
        argparse 对缺失/冲突参数抛出 ``SystemExit``；其它未预期启动错误继续传播，
        以便进程管理器观察非正常退出。

    状态变化:
        创建运行时、启动 Uvicorn，并按参数决定是否运行 root CLI；普通 Agent 即使
        显式传入 ``--interactive`` 也不会进入交互循环。
    """

    parser = _build_argument_parser()
    args = parser.parse_args(argv)
    try:
        asyncio.run(_async_main(args))
    except ConfigError as error:
        print(f"配置错误: {error}", file=sys.stderr)
        return 2
    except KeyboardInterrupt:
        return 130
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
