from __future__ import annotations

import json
from copy import deepcopy
from dataclasses import dataclass
from typing import Any, Literal

import httpx
from pydantic import BaseModel, Field
from starlette.types import ASGIApp


class FakeOutputText(BaseModel):
    """镜像 Responses assistant message 中生产代码会读取的文本块。"""

    type: Literal["output_text"] = "output_text"
    text: str
    annotations: list[dict[str, Any]] = Field(default_factory=list)
    logprobs: list[dict[str, Any]] = Field(default_factory=list)


class FakeMessageItem(BaseModel):
    """镜像一个已完成的 Responses assistant message output item。"""

    type: Literal["message"] = "message"
    id: str
    role: Literal["assistant"] = "assistant"
    status: Literal["completed"] = "completed"
    content: list[FakeOutputText]


class FakeFunctionCallItem(BaseModel):
    """镜像 openai 2.45.0 的 ResponseFunctionToolCall 必填字段。"""

    type: Literal["function_call"] = "function_call"
    id: str | None = None
    call_id: str
    name: str
    arguments: str
    status: Literal["completed"] | None = "completed"


class FakeReasoningSummary(BaseModel):
    """镜像 reasoning summary_text 内容块。"""

    type: Literal["summary_text"] = "summary_text"
    text: str


class FakeReasoningItem(BaseModel):
    """镜像需要被完整回放的加密 reasoning output item。"""

    type: Literal["reasoning"] = "reasoning"
    id: str
    summary: list[FakeReasoningSummary] = Field(default_factory=list)
    encrypted_content: str | None = None
    status: Literal["completed"] | None = "completed"


@dataclass(slots=True)
class FakeResponse:
    """提供 AgentRemote 生产代码会读取的完整 response 属性集合。"""

    output: list[BaseModel | dict[str, Any]]
    output_text: str


class FakeResponsesClient:
    """按顺序返回真实形状 response，并记录生产调用的完整参数。"""

    def __init__(self, responses: list[FakeResponse | BaseException]) -> None:
        self._responses = list(responses)
        self.create_calls: list[dict[str, Any]] = []

    async def create(self, **kwargs: Any) -> FakeResponse:
        self.create_calls.append(deepcopy(kwargs))
        if not self._responses:
            raise AssertionError("fake Responses 序列已耗尽")
        result = self._responses.pop(0)
        if isinstance(result, BaseException):
            raise result
        return result


class FakeOpenAIClient:
    """仅组合 Responses surface；close 计数用于生命周期测试。"""

    def __init__(self, responses: list[FakeResponse | BaseException]) -> None:
        self.responses = FakeResponsesClient(responses)
        self.close_calls = 0

    async def close(self) -> None:
        self.close_calls += 1


class HostRoutingASGITransport(httpx.AsyncBaseTransport):
    """按请求的 host:port 把 HTTP 调用交给对应的真实 ASGI 应用。

    调用记录刻意只保留方法、目标地址和路径；请求头与正文可能包含 Bearer 或用户
    消息，绝不能进入测试诊断数据。
    """

    def __init__(self) -> None:
        """创建一个尚未注册路由、调用记录为空的异步 transport。"""

        self._routes: dict[tuple[str, int], httpx.ASGITransport] = {}
        self.calls: list[dict[str, str | int]] = []

    def register(self, host: str, port: int, app: ASGIApp) -> None:
        """注册一个 host:port 到 ASGI 应用的唯一映射。"""

        route_key = (host.lower(), port)
        if route_key in self._routes:
            raise ValueError(f"测试路由已注册: {host}:{port}")
        self._routes[route_key] = httpx.ASGITransport(app=app)

    async def handle_async_request(self, request: httpx.Request) -> httpx.Response:
        """记录脱敏元数据，并通过完整 ASGI HTTP 边界处理请求。"""

        port = request.url.port
        if port is None:
            port = 443 if request.url.scheme == "https" else 80
        route_key = (request.url.host.lower(), port)
        self.calls.append(
            {
                "method": request.method,
                "host": request.url.host,
                "port": port,
                "path": request.url.path,
            }
        )
        route = self._routes.get(route_key)
        if route is None:
            raise httpx.ConnectError("测试目标 host:port 未注册", request=request)
        return await route.handle_async_request(request)

    async def aclose(self) -> None:
        """关闭所有内部 ASGI transport，而不接触注入的生产客户端所有权。"""

        first_error: BaseException | None = None
        for route in self._routes.values():
            try:
                await route.aclose()
            except BaseException as error:
                if first_error is None:
                    first_error = error
        if first_error is not None:
            raise first_error


class ThreeNodeFakeResponsesClient:
    """实现 root→Agent1→Agent2 验收所需的确定性 Responses 状态机。"""

    def __init__(self, agent_id: Literal["Agent1", "Agent2"]) -> None:
        """为指定节点创建零步状态和完整调用参数记录。"""

        self.agent_id = agent_id
        self.create_calls: list[dict[str, Any]] = []
        self._step = 0

    async def create(self, **kwargs: Any) -> FakeResponse:
        """验证当前上下文不变量后，返回下一条真实形状的 fake response。"""

        self.create_calls.append(deepcopy(kwargs))
        response_input = kwargs.get("input")
        if not isinstance(response_input, list):
            raise AssertionError("Responses input 必须是 item 列表")

        if self.agent_id == "Agent2":
            if self._step != 0:
                raise AssertionError("Agent2 在固定链路中只应调用模型一次")
            assert response_input == [
                {
                    "type": "message",
                    "role": "user",
                    "content": "你在干嘛？",
                }
            ]
            self._step = 1
            text = "我正在整理今天的任务。"
            return FakeResponse(
                output=[
                    FakeMessageItem(
                        id="msg_agent2_status",
                        content=[FakeOutputText(text=text)],
                    )
                ],
                output_text=text,
            )

        if self._step == 0:
            assert response_input == [
                {
                    "type": "message",
                    "role": "user",
                    "content": "请询问Agent2在干嘛",
                }
            ]
            self._step = 1
            return FakeResponse(
                output=[
                    FakeFunctionCallItem(
                        id="fc_agent2_status",
                        call_id="call_agent2_status",
                        name="send",
                        arguments='{"msg":"你在干嘛？","to_id":"Agent2"}',
                    )
                ],
                output_text="",
            )

        if self._step != 1:
            raise AssertionError("Agent1 固定状态机已完成")

        # 最终文本只能在生产代码原样回放 function_call，并用同一 call_id 配对真实
        # Agent2 HTTP 回复后出现；否则测试会在 fake 内停止，而不是掩盖协议断裂。
        function_calls = [
            item
            for item in response_input
            if isinstance(item, dict) and item.get("type") == "function_call"
        ]
        tool_outputs = [
            item
            for item in response_input
            if isinstance(item, dict)
            and item.get("type") == "function_call_output"
        ]
        assert len(function_calls) == 1
        assert function_calls[0]["call_id"] == "call_agent2_status"
        assert len(tool_outputs) == 1
        assert tool_outputs[0]["call_id"] == "call_agent2_status"
        assert isinstance(tool_outputs[0].get("output"), str)
        assert json.loads(tool_outputs[0]["output"]) == {
            "ok": True,
            "to_id": "Agent2",
            "message": "我正在整理今天的任务。",
        }
        self._step = 2
        text = "Agent2说它正在整理今天的任务。"
        return FakeResponse(
            output=[
                FakeMessageItem(
                    id="msg_agent1_final",
                    content=[FakeOutputText(text=text)],
                )
            ],
            output_text=text,
        )


class ThreeNodeFakeOpenAIClient:
    """组合确定性三节点 Responses surface，并记录可选关闭次数。"""

    def __init__(self, agent_id: Literal["Agent1", "Agent2"]) -> None:
        """为 Agent1 或 Agent2 创建对应的确定性 Responses 状态机。"""

        self.responses = ThreeNodeFakeResponsesClient(agent_id)
        self.close_calls = 0

    async def close(self) -> None:
        """记录显式关闭次数，保持与真实异步 OpenAI 客户端接口一致。"""

        self.close_calls += 1
