from __future__ import annotations

import asyncio
import os
from contextlib import AsyncExitStack
from pathlib import Path
from uuid import uuid4

import httpx
import pytest
from openai import AsyncOpenAI

from AgentRemote import AgentRemote
from User import User
from core import AgentConfig, ConversationKey, load_agent_config
from tests.helpers import HostRoutingASGITransport


PROJECT_ROOT = Path(__file__).resolve().parents[1]
CONFIG_ROOT = PROJECT_ROOT / "agents_setting"

pytestmark = pytest.mark.live


async def _model_endpoint_is_reachable() -> bool:
    """只探测本机 TCP 端点，不发送或记录任何模型密钥。"""

    try:
        _reader, writer = await asyncio.wait_for(
            asyncio.open_connection("127.0.0.1", 20128),
            timeout=1.0,
        )
    except (OSError, TimeoutError):
        return False
    writer.close()
    await writer.wait_closed()
    return True


def _load_live_configs() -> dict[str, AgentConfig]:
    """从公开示例加载 live smoke 使用的三个生产配置模型。"""

    return {
        agent_id: load_agent_config(CONFIG_ROOT / f"{agent_id}.json")
        for agent_id in ("root", "Agent1", "Agent2")
    }


@pytest.mark.asyncio
async def test_live_root_agent1_agent2_chain_uses_real_responses_api(
    tmp_path: Path,
) -> None:
    # 两个显式开关共同防止默认测试套件意外访问模型；不可达诊断只描述本地端口，
    # 绝不把环境变量值放进 skip 原因、响应快照或断言消息。
    if os.getenv("RUN_LIVE") != "1":
        pytest.skip("设置 RUN_LIVE=1 后才运行真实模型 smoke")
    api_key = os.getenv("OPENAI_API_KEY")
    if not api_key:
        pytest.skip("缺少 OPENAI_API_KEY，跳过真实模型 smoke")
    if not await _model_endpoint_is_reachable():
        pytest.skip("本地模型端点 localhost:20128 不可达，跳过 live smoke")

    configs = _load_live_configs()
    agent1_config = configs["Agent1"]
    agent2_config = configs["Agent2"]
    assert agent1_config.openai_baseurl is not None
    assert agent2_config.openai_baseurl is not None

    transport = HostRoutingASGITransport()
    # 每个真实模型客户端构造后立即登记清理；后续构造失败或某个 close 抛错时，
    # AsyncExitStack 仍会继续尝试其余已登记的 async close。
    async with AsyncExitStack() as stack:
        agent1_model = AsyncOpenAI(
            base_url=agent1_config.openai_baseurl,
            api_key=api_key,
            timeout=30.0,
        )
        stack.push_async_callback(agent1_model.close)
        agent2_model = AsyncOpenAI(
            base_url=agent2_config.openai_baseurl,
            api_key=api_key,
            timeout=30.0,
        )
        stack.push_async_callback(agent2_model.close)
        shared_http = await stack.enter_async_context(
            httpx.AsyncClient(transport=transport)
        )
        root = User(
            configs["root"],
            http_client=shared_http,
            storage_root=tmp_path,
        )
        agent1 = AgentRemote(
            agent1_config,
            http_client=shared_http,
            openai_client=agent1_model,
            storage_root=tmp_path,
        )
        agent2 = AgentRemote(
            agent2_config,
            http_client=shared_http,
            openai_client=agent2_model,
            storage_root=tmp_path,
        )
        root_app = root.create_app()
        agent1_app = agent1.create_app()
        agent2_app = agent2.create_app()
        await stack.enter_async_context(root_app.router.lifespan_context(root_app))
        await stack.enter_async_context(
            agent1_app.router.lifespan_context(agent1_app)
        )
        await stack.enter_async_context(
            agent2_app.router.lifespan_context(agent2_app)
        )
        transport.register("127.0.0.1", 9860, root_app)
        transport.register("127.0.0.1", 9861, agent1_app)
        transport.register("127.0.0.1", 9862, agent2_app)

        response = await shared_http.post(
            "http://127.0.0.1:9860/v1/user/chats/Agent1/messages",
            json={
                "message": "请询问Agent2在干嘛",
                "request_id": str(uuid4()),
            },
        )
        if response.status_code != 200 and not await _model_endpoint_is_reachable():
            pytest.skip("live 调用期间 localhost:20128 变为不可达")

        assert response.status_code == 200
        answer = response.json().get("data")
        assert isinstance(answer, str) and answer.strip()
        root_chat = root.chat_spaces["Agent1"]
        agent1_chat = agent1.chat_spaces[
            ConversationKey("root", root_chat.conversation_id)
        ]
        agent2_chat = agent2.chat_spaces[
            ConversationKey("Agent1", agent1_chat.conversation_id)
        ]
        assert agent2_chat.messages
        assert agent1.currentChatSpace is None
        assert agent2.currentChatSpace is None
