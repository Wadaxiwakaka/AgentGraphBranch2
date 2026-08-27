"""AgentGraph 的公共配置、API 数据模型与本地会话基础设施。"""

from __future__ import annotations

import asyncio
import json
import os
import re
import tempfile
from copy import deepcopy
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Annotated, Any, Literal
from uuid import UUID, uuid4

from fastapi import FastAPI
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse
from pydantic import (
    BaseModel,
    Field,
    SecretStr,
    ValidationError,
    field_validator,
    model_validator,
)
from starlette.types import ASGIApp, Receive, Scope, Send


_ENV_REFERENCE = re.compile(r"\$\{([A-Za-z_][A-Za-z0-9_]*)\}")

# skill 名同时也是文件名 stem；与 Agent id 同款字符集但要求字母开头，
# 防止纯数字等易混淆名字进入配置引用。
_SKILL_NAME = re.compile(r"^[A-Za-z][A-Za-z0-9_-]*$")

# 单个 skill 正文的字符上限；与 introduction 上限一致，8 个 skill 封顶约 16K
# 字符，覆盖 instructions 不计入 max_context_chars 预算的盲区。
_SKILL_MAX_BODY_CHARS = 2000


class ConfigError(Exception):
    """表示配置文件读取、解析、变量展开或模型校验失败。

    参数:
        message: 可安全展示给调用方的错误说明；内容不得包含密钥明文。

    返回值:
        构造后得到可由上层捕获并展示的配置异常实例。

    异常:
        本类构造过程不额外抛出异常；原始文件或校验异常由加载函数转换。

    状态变化:
        仅记录错误文本，不修改配置文件、环境变量或运行时配置。
    """


class PeerConfig(BaseModel):
    """描述一个可通过 HTTP(S) 访问的对等 Agent。

    参数:
        id: 对等 Agent 的稳定标识，只允许字母、数字、下划线和连字符。
        ip: 对等 Agent 的监听地址，默认使用本机回环地址。
        protocol: 连接协议，只能是 ``http`` 或 ``https``。
        port: TCP 端口，范围为 1 到 65535。
        key: 调用该对等 Agent 时使用的非空共享密钥。
        ca_file: 校验 HTTPS 证书时可选的 CA 文件路径。

    返回值:
        构造成功后返回经过校验的配置模型。

    异常:
        pydantic.ValidationError: id、端口、协议或密钥不符合约束时抛出。

    状态变化:
        仅保存经过校验的配置，不读取文件、环境变量或网络状态。
    """

    id: str = Field(pattern=r"^[A-Za-z0-9_-]{1,64}$")
    ip: str = "127.0.0.1"
    protocol: Literal["http", "https"] = "http"
    port: int = Field(ge=1, le=65535)
    key: SecretStr
    ca_file: Path | None = None

    @field_validator("key")
    @classmethod
    def _validate_key(cls, value: SecretStr) -> SecretStr:
        if not value.get_secret_value().strip():
            raise ValueError("key 不能为空")
        return value


ToolName = Annotated[str, Field(pattern=r"^[A-Za-z0-9_-]{1,64}$")]
ToolExtensionSelection = Literal["all", "none"] | list[ToolName]


class ToolSelectionConfig(BaseModel):
    """描述普通 Agent 对显式扩展工具目录的启动时选择。"""

    extensions: ToolExtensionSelection = Field(default_factory=list)

    @field_validator("extensions")
    @classmethod
    def _validate_extension_names(
        cls,
        value: ToolExtensionSelection,
    ) -> ToolExtensionSelection:
        if isinstance(value, list) and len(value) != len(set(value)):
            raise ValueError("extensions 不能包含重复的工具名")
        return value


class AgentConfig(BaseModel):
    """描述当前 Agent 的身份、模型连接、对等节点和运行限制。

    参数:
        id: 当前 Agent 的稳定标识；特殊值 ``root`` 表示无需模型凭据的根节点。
        introduction: Agent 的中文或英文职责说明，必须非空且不超过 2000 字符。
        host: HTTP 服务监听地址，默认使用本机回环地址。
        port: HTTP 服务监听端口，范围为 1 到 65535。
        key: 普通 Agent 接收入站调用时使用的非空共享密钥。
        openai_baseurl: 普通 Agent 调用 OpenAI 兼容接口时使用的基础 URL。
        openai_key: 普通 Agent 调用模型时使用的非空密钥。
        model: 普通 Agent 使用的非空模型名称。
        agents: 当前 Agent 可以访问的对等节点列表。
        tools: 扩展工具三态选择；省略时等价于空列表。
        skills: 启用的 skill 名列表；名字必须在启动时 skills 目录加载结果中
            存在，最多 8 个，默认空列表即不启用。
        ssl_certfile: 可选的服务端 TLS 证书文件。
        ssl_keyfile: 可选的服务端 TLS 私钥文件，必须与证书同时提供。
        http_timeout_seconds: Agent 间 HTTP 调用超时秒数，必须为正数。
        openai_timeout_seconds: 模型调用超时秒数，必须为正数。
        include_encrypted_reasoning: 是否把加密推理项纳入 Responses API 上下文。
        max_tool_calls_per_turn: 每轮允许的最大工具调用数，必须为正整数。
        max_response_steps_per_turn: 每轮允许的最大响应步骤数，必须为正整数。
        max_context_chars: 会话上下文的字符预算；省略或为 ``None`` 时不裁剪，
            启用后超限的会话会从最旧内容开始裁剪（见 ``ChatSpace.trim_context``）。
        topology_max_nodes: 拓扑查询允许访问的最大节点数，必须为正整数。
        topology_max_depth: 拓扑查询允许递归的最大深度，必须为正整数。

    返回值:
        构造成功后返回经过跨字段校验的配置模型。

    异常:
        pydantic.ValidationError: 必填凭据、端口、TLS 文件、对等节点或限制不合法时抛出。

    状态变化:
        仅保存配置数据；每个实例拥有独立的 ``agents`` 列表，不修改外部状态。
    """

    id: str
    introduction: str = Field(max_length=2000)
    host: str = "127.0.0.1"
    port: int = Field(ge=1, le=65535)
    key: SecretStr | None = None
    openai_baseurl: str | None = None
    openai_key: SecretStr | None = None
    model: str | None = None
    agents: list[PeerConfig] = Field(default_factory=list)
    tools: ToolSelectionConfig = Field(default_factory=ToolSelectionConfig)
    skills: list[str] = Field(default_factory=list, max_length=8)
    ssl_certfile: Path | None = None
    ssl_keyfile: Path | None = None
    http_timeout_seconds: float = Field(default=60.0, gt=0)
    openai_timeout_seconds: float = Field(default=600.0, gt=0)
    include_encrypted_reasoning: bool = True
    max_tool_calls_per_turn: int = Field(default=200, gt=0)
    max_response_steps_per_turn: int = Field(default=256, gt=0)
    max_context_chars: int | None = Field(default=None, gt=0)
    topology_max_nodes: int = Field(default=1000, gt=0)
    topology_max_depth: int = Field(default=64, gt=0)

    @field_validator("introduction")
    @classmethod
    def _validate_introduction(cls, value: str) -> str:
        if not value.strip():
            raise ValueError("introduction 不能为空")
        return value

    @field_validator("skills")
    @classmethod
    def _validate_skill_names(cls, value: list[str]) -> list[str]:
        if len(value) != len(set(value)):
            raise ValueError("skills 不能包含重复的 skill 名")
        return value

    @field_validator("key", "openai_key")
    @classmethod
    def _validate_optional_secret(cls, value: SecretStr | None) -> SecretStr | None:
        if value is not None and not value.get_secret_value().strip():
            raise ValueError("密钥不能为空")
        return value

    @field_validator("openai_baseurl", "model")
    @classmethod
    def _validate_optional_text(cls, value: str | None) -> str | None:
        if value is not None and not value.strip():
            raise ValueError("模型连接字段不能为空")
        return value

    @model_validator(mode="after")
    def _validate_relationships(self) -> AgentConfig:
        extension_selection = self.tools.extensions
        extensions_enabled = extension_selection == "all" or (
            isinstance(extension_selection, list) and bool(extension_selection)
        )
        if self.id == "root" and extensions_enabled:
            raise ValueError("root 不允许启用扩展工具")
        if self.id == "root" and self.skills:
            raise ValueError("root 不允许启用 skill")

        if self.id != "root":
            required_values = (
                self.key,
                self.openai_baseurl,
                self.openai_key,
                self.model,
            )
            if any(value is None for value in required_values):
                raise ValueError("普通 Agent 必须提供 key、openai_baseurl、openai_key 和 model")

        peer_ids = [peer.id for peer in self.agents]
        if self.id in peer_ids:
            raise ValueError("agents 不能包含当前 Agent 自身")
        if len(peer_ids) != len(set(peer_ids)):
            raise ValueError("agents 不能包含重复的 peer id")

        if (self.ssl_certfile is None) != (self.ssl_keyfile is None):
            raise ValueError("ssl_certfile 和 ssl_keyfile 必须同时提供或同时省略")
        return self


def _expand_environment(value: Any) -> Any:
    if isinstance(value, str):

        def replace_reference(match: re.Match[str]) -> str:
            variable_name = match.group(1)
            if variable_name not in os.environ:
                # 变量可能位于 secret 字符串中；错误只报告变量名，避免把已展开片段泄漏出去。
                raise ConfigError(f"缺少环境变量: {variable_name}")
            return os.environ[variable_name]

        return _ENV_REFERENCE.sub(replace_reference, value)
    if isinstance(value, list):
        return [_expand_environment(item) for item in value]
    if isinstance(value, dict):
        return {key: _expand_environment(item) for key, item in value.items()}
    return value


def _safe_validation_message(error: ValidationError) -> str:
    descriptions: list[str] = []
    for item in error.errors(
        include_url=False,
        include_context=False,
        include_input=False,
    ):
        location = ".".join(str(part) for part in item["loc"]) or "<root>"
        descriptions.append(f"{location}: {item['msg']}")
    return "; ".join(descriptions)


def load_agent_config(path: str | Path) -> AgentConfig:
    """从 UTF-8 JSON 文件加载并校验一个 Agent 配置。

    参数:
        path: 配置文件的字符串路径或 ``Path``；文件内容必须是 UTF-8 JSON。

    返回值:
        环境变量递归展开并通过全部字段及跨字段校验的 ``AgentConfig``。

    异常:
        ConfigError: 文件不可读、UTF-8 解码失败、JSON 无效、环境变量缺失或
            Pydantic 校验失败时抛出。错误文本会保留缺失变量名和字段位置，但不会
            回显配置输入值或保留原始异常链，从而避免泄漏密钥及完整 JSON。

    状态变化:
        读取文件和当前进程环境变量，不修改文件、环境变量或全局配置状态。
    """

    config_path = Path(path)
    try:
        raw_text = config_path.read_text(encoding="utf-8")
    except (OSError, UnicodeError) as error:
        read_error_message = f"无法读取配置文件 {config_path}: {error}"
    else:
        read_error_message = None
    if read_error_message is not None:
        # 离开 except 后再抛出，避免 ConfigError 的隐式 context 保留底层异常对象。
        raise ConfigError(read_error_message) from None

    try:
        raw_data = json.loads(raw_text)
    except json.JSONDecodeError as error:
        json_error_message = (
            f"配置文件 JSON 无效（第 {error.lineno} 行，第 {error.colno} 列）: {error.msg}"
        )
    else:
        json_error_message = None
    if json_error_message is not None:
        # JSONDecodeError 持有原始 doc；不能把该 cause/context 带入日志 traceback。
        raise ConfigError(json_error_message) from None

    # 在模型校验前展开整个 JSON 树，既支持嵌套 peer，也让 secret 始终由 SecretStr 接管。
    expanded_data = _expand_environment(raw_data)
    try:
        return AgentConfig.model_validate(expanded_data)
    except ValidationError as error:
        validation_error_message = _safe_validation_message(error)
    # ValidationError 的 input_value 可能是含明文 secret 的完整配置，必须在异常块外抛出。
    raise ConfigError(f"配置校验失败: {validation_error_message}") from None


@dataclass(frozen=True)
class SkillSpec:
    """描述一个已解析的 skill 指令包。

    参数:
        name: 与文件名 stem 一致的稳定 skill 名。
        description: 供运维者查看的清单说明，不注入模型。
        body: 注入 instructions 的完整指令正文。

    返回值:
        构造后得到不可变的 skill 记录。

    异常:
        构造阶段不抛异常。

    状态变化:
        无；frozen 实例创建后不可修改。
    """

    name: str
    description: str
    body: str


def parse_skill_file(path: Path) -> SkillSpec:
    """解析单个 skill markdown 文件为严格校验的 ``SkillSpec``。

    文件格式：前几行 ``key: value`` 头（仅接受 name 与 description），
    之后一个空行，空行后全部为正文，不再解析任何键。

    参数:
        path: skill 文件路径；文件名去 ``.md`` 后必须满足
            ``^[A-Za-z][A-Za-z0-9_-]*$``。

    返回值:
        携带 name、description 与正文的 ``SkillSpec``。

    异常:
        ConfigError: 文件名非法、不可读、非 UTF-8、头部行不是 ``key: value``
            格式、包含未知键、缺少必填键、name 与文件名不一致或正文超过
            2000 字符时抛出。错误只报告文件名与原因，不回显文件内容。

    状态变化:
        只读取文件，不修改文件系统。
    """

    stem = path.stem
    if _SKILL_NAME.fullmatch(stem) is None:
        raise ConfigError(f"skill 文件名非法: {path.name}")
    try:
        text = path.read_text(encoding="utf-8")
    except (OSError, UnicodeDecodeError):
        raise ConfigError(f"skill 文件不可读: {path.name}") from None

    lines = text.split("\n")
    header: dict[str, str] = {}
    body_start = len(lines)
    for index, line in enumerate(lines):
        if not line.strip():
            body_start = index + 1
            break
        key, separator, value = line.partition(":")
        if not separator or not key.strip():
            raise ConfigError(f"skill 头部行必须是 key: value 格式: {path.name}")
        key = key.strip()
        if key not in {"name", "description"}:
            raise ConfigError(f"skill 头部包含未知键: {path.name}")
        value = value.strip()
        if not value:
            raise ConfigError(f"skill 头部键 {key} 的值不能为空: {path.name}")
        header[key] = value

    if "name" not in header:
        raise ConfigError(f"skill 缺少必填键 name: {path.name}")
    if header["name"] != stem:
        raise ConfigError(f"skill name 与文件名不一致: {path.name}")
    if "description" not in header:
        raise ConfigError(f"skill 缺少必填键 description: {path.name}")

    body = "\n".join(lines[body_start:])
    if len(body) > _SKILL_MAX_BODY_CHARS:
        raise ConfigError(
            f"skill 正文超过 {_SKILL_MAX_BODY_CHARS} 字符: {path.name}"
        )
    return SkillSpec(name=header["name"], description=header["description"], body=body)


def load_skills(directory: Path) -> dict[str, SkillSpec]:
    """加载目录下全部 ``*.md`` 为 skill 映射。

    参数:
        directory: skill 目录；不存在时返回空映射（未配置 skill 的既有部署
            零影响），不递归子目录，忽略非 ``.md`` 文件。

    返回值:
        以 skill 名为键、按文件名排序确定性加载的 ``SkillSpec`` 映射。平铺
        目录内文件名 stem 唯一，因此键不会重复。

    异常:
        ConfigError: 任一文件解析失败时抛出；任一文件失败即整体失败。

    状态变化:
        只读取目录与文件，不修改文件系统。
    """

    if not directory.is_dir():
        return {}
    skills: dict[str, SkillSpec] = {}
    for path in sorted(directory.glob("*.md")):
        if path.is_file():
            skill = parse_skill_file(path)
            skills[skill.name] = skill
    return skills


@dataclass(frozen=True, slots=True)
class ConversationKey:
    """唯一标识一个普通 Agent 的入站会话分支。

    ``from_id`` 只标识直接上游节点；同一上游可以同时拥有多个独立会话，因此所有
    会话映射、FIFO 队列和 busy 状态都必须使用这两个字段组成的完整键。
    """

    from_id: str
    conversation_id: str


class MessageRequest(BaseModel):
    """表示一个 Agent 向另一个 Agent 发送的消息请求。

    参数:
        from_id: 发起请求的 Agent 标识。
        conversation_id: 调用方当前本地 ChatSpace 的 UUID。
        message: 长度为 1 到 65536 字符的消息正文。
        request_id: 用于端到端追踪和幂等关联的 UUID。

    返回值:
        构造成功后返回类型稳定、UUID 已解析的请求模型。

    异常:
        pydantic.ValidationError: 消息为空、过长或 request_id 不是 UUID 时抛出。

    状态变化:
        仅保存请求数据，不发送网络请求或修改会话。
    """

    from_id: str
    conversation_id: UUID
    message: str = Field(min_length=1, max_length=65536)
    request_id: UUID


class CloseRequest(BaseModel):
    """表示一个 Agent 请求关闭双方会话的控制消息。

    参数:
        from_id: 发起关闭请求的 Agent 标识。
        conversation_id: 要关闭的调用方本地 ChatSpace UUID。
        request_id: 用于追踪本次关闭操作的 UUID。

    返回值:
        构造成功后返回 UUID 已解析的关闭请求模型。

    异常:
        pydantic.ValidationError: request_id 不是有效 UUID 或字段类型无效时抛出。

    状态变化:
        仅保存关闭请求数据，本身不会清除或持久化会话。
    """

    from_id: str
    conversation_id: UUID
    request_id: UUID


class TopologyRequest(BaseModel):
    """描述一次有界拓扑遍历请求的当前状态和限制。

    参数:
        visited_ids: 已经访问过、后续应跳过的 Agent 标识列表。
        depth: 当前节点所处的遍历深度。
        max_depth: 本次请求允许达到的最大深度。
        max_nodes: 本次请求允许收集的最大节点数。

    返回值:
        构造成功后返回可通过 API 序列化的拓扑请求模型。

    异常:
        pydantic.ValidationError: 任一字段无法转换为声明的类型时抛出。

    状态变化:
        仅保存遍历参数，不访问网络或改变拓扑状态。
    """

    visited_ids: list[str]
    depth: int
    max_depth: int
    max_nodes: int


class UserMessageRequest(BaseModel):
    """表示外部用户提交给根 Agent 的消息请求。

    参数:
        message: 长度为 1 到 65536 字符的用户消息正文。
        request_id: 可选 UUID；提供时用于调用链追踪，不提供时由上层决定是否生成。

    返回值:
        构造成功后返回消息已校验、UUID 已解析的用户请求模型。

    异常:
        pydantic.ValidationError: 消息为空、过长或 request_id 不是 UUID 时抛出。

    状态变化:
        仅保存用户输入，不启动 Agent 调用或修改会话。
    """

    message: str = Field(min_length=1, max_length=65536)
    request_id: UUID | None = None


class APIErrorDetail(BaseModel):
    """定义 AgentGraph HTTP API 的稳定错误响应结构。

    参数:
        code: 供程序判断错误类别的稳定机器码。
        message: 供调用方阅读的安全错误说明。
        details: 可选的结构化补充信息，不应包含 secret。
        retry_after_seconds: 可选的建议重试等待秒数。

    返回值:
        构造成功后返回可由 FastAPI/Pydantic 序列化的错误详情模型。

    异常:
        pydantic.ValidationError: 输入无法满足声明的字段类型时抛出。

    状态变化:
        仅保存错误描述，不设置 HTTP 响应或执行重试。
    """

    code: str
    message: str
    details: Any | None = None
    retry_after_seconds: float | None = None


class AgentGraphError(Exception):
    """携带 API 错误码、HTTP 状态和可选重试信息的领域异常。

    参数:
        code: 供调用方稳定识别错误类别的机器码。
        message: 安全、可读的错误说明，同时也是异常字符串。
        status_code: 上层转换为 HTTP 响应时使用的状态码。
        details: 可选的结构化上下文，默认 ``None``。
        retry_after_seconds: 可选的建议重试等待秒数，默认 ``None``。

    返回值:
        构造后返回可被常规 ``Exception`` 处理器捕获的异常实例。

    异常:
        构造过程不额外抛出异常；调用方负责提供适合传输的 details。

    状态变化:
        保存错误元数据但不直接发送 HTTP 响应、记录日志或执行重试。
    """

    def __init__(
        self,
        code: str,
        message: str,
        status_code: int,
        details: Any | None = None,
        retry_after_seconds: float | None = None,
    ) -> None:
        super().__init__(message)
        self.code = code
        self.message = message
        self.status_code = status_code
        self.details = details
        self.retry_after_seconds = retry_after_seconds


class _SafeHTTPExceptionBoundary:
    """仅在 HTTP scope 内消费未处理异常并返回固定、无敏感信息的 500。"""

    def __init__(self, app: ASGIApp) -> None:
        self.app = app

    async def __call__(
        self,
        scope: Scope,
        receive: Receive,
        send: Send,
    ) -> None:
        if scope.get("type") != "http":
            # lifespan/websocket 不属于此 HTTP 安全边界，异常必须继续交给服务器。
            await self.app(scope, receive, send)
            return

        response_started = False

        async def tracked_send(message: dict[str, Any]) -> None:
            nonlocal response_started
            if message.get("type") == "http.response.start":
                response_started = True
            await send(message)

        try:
            await self.app(scope, receive, tracked_send)
        except asyncio.CancelledError:
            # 客户端断开或服务关闭使用取消信号控制生命周期，绝不能伪装成 500。
            raise
        except Exception:
            # 不记录也不重新抛出原异常；其文本、URL、请求或第三方对象都可能含 secret。
            if response_started:
                # 响应头已经发出时不能再合法发送第二个 status line；消费异常以阻止
                # sentinel 逃逸。AgentGraph 当前公开路由均在发送前完成 JSON 计算。
                return
            response = JSONResponse(
                status_code=500,
                content={
                    "error": {
                        "code": "INTERNAL_ERROR",
                        "message": "服务器内部错误",
                    }
                },
            )
            await response(scope, receive, send)


def register_api_error_handlers(app: FastAPI) -> None:
    """为 AgentGraph 的 FastAPI 应用注册统一、安全的错误 envelope。

    参数:
        app: 要安装 handler 的 FastAPI 应用；普通 Agent 与 root 网关共用此函数。

    返回值:
        ``None``。

    异常:
        FastAPI 拒绝注册 handler 时传播对应异常；请求处理阶段的领域异常、校验异常
        和未知异常分别由注册函数转换为稳定 JSON 响应。

    状态变化:
        在应用上注册 HTTP-only 安全异常边界，以及 ``AgentGraphError``、
        ``RequestValidationError`` 和 ``Exception`` 三类 handler。校验详情只白名单
        输出 loc/message/type，避免回显用户消息、Bearer、OpenAI key 或 Pydantic
        保存的原始输入。
    """

    app.add_middleware(_SafeHTTPExceptionBoundary)

    @app.exception_handler(AgentGraphError)
    async def handle_agent_graph_error(
        _request: Any,
        error: AgentGraphError,
    ) -> JSONResponse:
        """把受控领域异常转换为稳定的 AgentGraph 错误 envelope。

        参数:
            _request: FastAPI 传入的当前请求；handler 不读取请求头或正文，避免把
                Authorization、用户消息等敏感输入带入响应或诊断。
            error: 已由生产代码构造的 ``AgentGraphError``，包含安全错误码、消息、
                HTTP 状态以及可选 details 和重试秒数。

        返回值:
            与领域异常状态码一致的 ``JSONResponse``；按需携带 ``Retry-After``，认证
            失败时携带 ``WWW-Authenticate: Bearer``。

        异常:
            正常情况下不抛出；若传入无法 JSON 序列化的 details，响应构造阶段可能
            传播序列化异常，调用方必须只提供可传输的安全结构。

        安全脱敏:
            只输出异常公开的稳定字段，不读取或回显请求、底层客户端异常、URL、
            Bearer 或 OpenAI key；认证失败也不会返回目标配置中的真实凭据。

        状态变化:
            不修改应用、请求或领域异常；仅构造一次 HTTP 响应及必要的标准响应头。
        """

        error_payload: dict[str, Any] = {
            "code": error.code,
            "message": error.message,
        }
        if error.details is not None:
            error_payload["details"] = error.details
        headers: dict[str, str] = {}
        if error.retry_after_seconds is not None:
            retry_value = error.retry_after_seconds
            retry_header = (
                str(int(retry_value))
                if float(retry_value).is_integer()
                else str(retry_value)
            )
            error_payload["retry_after_seconds"] = retry_value
            headers["Retry-After"] = retry_header
        if error.code == "AUTHENTICATION_FAILED" and error.status_code == 401:
            headers["WWW-Authenticate"] = "Bearer"
        return JSONResponse(
            status_code=error.status_code,
            content={"error": error_payload},
            headers=headers,
        )

    @app.exception_handler(RequestValidationError)
    async def handle_validation_error(
        _request: Any,
        error: RequestValidationError,
    ) -> JSONResponse:
        """把请求校验失败转换为不含原始输入的 422 错误 envelope。

        参数:
            _request: FastAPI 传入的当前请求；handler 不访问其 headers、body 或 URL。
            error: FastAPI 产生的 ``RequestValidationError``，内部可能保留未经信任的
                input、context 与用户消息。

        返回值:
            状态码为 422 的 ``JSONResponse``；details 中每项只包含字符串化 loc、
            安全 message 和稳定 type。

        异常:
            正常情况下不抛出；若框架返回的 errors 结构违反其公开契约，可能传播本地
            迭代或类型异常，由外层未知异常安全边界统一处理。

        安全脱敏:
            严格白名单复制 loc/message/type，绝不序列化校验项中的 input、context、
            Bearer、用户正文或模型 key，也不保留原异常对象到响应中。

        状态变化:
            只读取校验错误快照并构造响应，不修改请求、错误对象或应用注册状态。
        """

        details = [
            {
                "loc": [str(part) for part in item.get("loc", ())],
                "message": item.get("msg", "参数无效"),
                "type": item.get("type", "validation_error"),
            }
            # FastAPI RequestValidationError.errors() 与 Pydantic 的同名方法签名不同，
            # 只能无参读取；随后严格白名单输出，绝不序列化 item 中的 input/context。
            for item in error.errors()
        ]
        return JSONResponse(
            status_code=422,
            content={
                "error": {
                    "code": "VALIDATION_ERROR",
                    "message": "请求参数校验失败",
                    "details": details,
                }
            },
        )

    @app.exception_handler(Exception)
    async def handle_unexpected_error(
        _request: Any,
        _error: Exception,
    ) -> JSONResponse:
        """把未知 HTTP 异常收敛为固定且脱敏的 500 错误 envelope。

        参数:
            _request: FastAPI 传入的当前请求；为避免泄漏，不读取请求的任何字段。
            _error: 未被更具体 handler 处理的异常；其文本和属性均视为潜在敏感数据。

        返回值:
            状态码为 500、机器码为 ``INTERNAL_ERROR`` 的固定 ``JSONResponse``。

        异常:
            handler 本身正常不抛出，也不会重新抛出或串联原异常；响应发送阶段的底层
            ASGI 故障仍由服务器生命周期处理。

        安全脱敏:
            不记录、检查或回显原异常及请求，因此异常中的 URL、Authorization、消息、
            OpenAI key 和第三方响应对象都不会进入客户端可见内容。

        状态变化:
            不修改应用、请求或异常，只创建固定响应；不会执行重试、日志记录或清理。
        """

        return JSONResponse(
            status_code=500,
            content={
                "error": {
                    "code": "INTERNAL_ERROR",
                    "message": "服务器内部错误",
                }
            },
        )


@dataclass
class _ContextTrimUnit:
    """裁剪单元：一个或多个 context item 下标组成的不可分割整体。

    ``function_call`` 与其同 ``call_id`` 的 ``function_call_output`` 绑定进同一
    单元，保证裁剪不会产生孤儿调用或孤儿结果；其余 item 各自独立成单元。
    记录下标而非内容，重建时才能保持剩余 item 的原始相对顺序。
    """

    item_indices: list[int]
    char_size: int = 0


class ChatSpace:
    """维护两个 Agent 之间同时面向人类和 Responses API 的本地会话。

    参数:
        owner_id: 拥有并持久化此会话的当前 Agent 标识。
        peer_id: 对话另一端的 Agent 标识；兼容属性 ``id`` 与其相同。
        instructions: 每次模型调用使用的会话级指令，默认空字符串。
        tools: Responses API 工具定义列表；``None`` 会转换为独立的空列表。
        storage_root: 历史记录根目录，默认 ``chat_history``。

    返回值:
        构造后得到具有唯一 conversation_id、空消息视图和空上下文的会话对象。

    异常:
        构造阶段不进行文件 I/O；仅当传入对象无法被深拷贝时可能传播其复制异常。

    状态变化:
        在内存中创建独立的 tools、messages 和 context_items 容器，不写入磁盘。
    """

    def __init__(
        self,
        owner_id: str,
        peer_id: str,
        instructions: str = "",
        tools: list[dict[str, Any]] | None = None,
        storage_root: Path = Path("chat_history"),
    ) -> None:
        """初始化会话身份和两套消息视图。

        参数:
            owner_id: 当前会话拥有者标识。
            peer_id: 对等 Agent 标识，同时赋给兼容属性 ``id``。
            instructions: 会话级模型指令。
            tools: 可选工具定义列表；会被深拷贝以隔离调用方后续修改。
            storage_root: ``save`` 使用的历史记录根目录。

        返回值:
            ``None``；初始化结果保存在当前实例的公开属性中。

        异常:
            复制工具定义失败时传播对应异常；本方法不访问文件系统。

        状态变化:
            生成新的 UUID 会话标识，并初始化空的消息与上下文列表。
        """

        self.owner_id = owner_id
        self.peer_id = peer_id
        self.id = peer_id
        self.messages: list[dict[str, Any]] = []
        self.instructions = instructions
        self.tools: list[dict[str, Any]] = deepcopy(tools) if tools is not None else []
        self.context_items: list[dict[str, Any]] = []
        self.conversation_id = str(uuid4())
        self.storage_root = Path(storage_root)

    def add_msg(self, msg: str, role: str) -> None:
        """向会话追加一条 user 或 assistant 可读消息。

        参数:
            msg: 要记录的文本消息。
            role: ``user``、``assistant`` 或兼容角色 ``agent``；``agent`` 会映射为
                Responses API 接受的 ``assistant``。

        返回值:
            ``None``。

        异常:
            ValueError: role 不在允许集合中时抛出，且会话保持不变。

        状态变化:
            成功时分别向 ``messages`` 和 ``context_items`` 追加一条记录。

        """

        if role not in {"user", "assistant", "agent"}:
            raise ValueError("role 仅接受 user、assistant 或 agent")
        normalized_role = "assistant" if role == "agent" else role
        readable_message = {"role": normalized_role, "content": msg}
        response_item = {
            "type": "message",
            "role": normalized_role,
            "content": msg,
        }
        # 双视图让界面只读简洁文本，同时保留 Responses API 可直接重放的 item 结构。
        self.messages.append(readable_message)
        self.context_items.append(response_item)

    def append_response_items(self, items: Any) -> None:
        """追加 Responses API 输出项，并同步可读的 assistant 文本。

        参数:
            items: 单个字典/SDK 项，或由这些项组成的可迭代对象。SDK/Pydantic 项必须
                提供 ``model_dump(exclude_none=True)``。

        返回值:
            ``None``。

        异常:
            TypeError: 某个 item 既不是字典，也不支持合规的 ``model_dump``，或序列化
                结果不是字典时抛出；此前已成功追加的项不会回滚。

        状态变化:
            每个完整 item 只向 ``context_items`` 追加一次；assistant message 中所有
            ``output_text`` 会合并为一条可读消息追加到 ``messages``，function call 和
            reasoning 等非消息项不会进入可读视图。
        """

        if isinstance(items, dict) or callable(getattr(items, "model_dump", None)):
            item_sequence = [items]
        else:
            try:
                item_sequence = iter(items)
            except TypeError as error:
                raise TypeError("items 必须是 response item 或其可迭代对象") from error

        for item in item_sequence:
            if isinstance(item, dict):
                serialized_item = deepcopy(item)
            else:
                model_dump = getattr(item, "model_dump", None)
                if not callable(model_dump):
                    raise TypeError("response item 必须是 dict 或支持 model_dump")
                # SDK 类型会随版本演进；走其公开序列化接口可保留未知字段并剔除 None。
                serialized_item = model_dump(exclude_none=True)
                if not isinstance(serialized_item, dict):
                    raise TypeError("response item 的 model_dump 必须返回 dict")
                serialized_item = deepcopy(serialized_item)

            self.context_items.append(serialized_item)
            if (
                serialized_item.get("type") != "message"
                or serialized_item.get("role") != "assistant"
            ):
                continue

            content = serialized_item.get("content")
            if isinstance(content, str):
                readable_text = content
            elif isinstance(content, list):
                readable_text = "".join(
                    block.get("text", "")
                    for block in content
                    if isinstance(block, dict)
                    and block.get("type") == "output_text"
                    and isinstance(block.get("text"), str)
                )
            else:
                readable_text = ""

            if readable_text:
                # 直接写可读视图，避免调用 add_msg 后把同一 output message 再次写入上下文。
                self.messages.append(
                    {"role": "assistant", "content": readable_text}
                )

    def append_tool_output(self, call_id: str, output: Any) -> None:
        """向 Responses API 上下文追加一个函数调用结果。

        参数:
            call_id: 与先前 ``function_call`` 对应的调用标识。
            output: 工具执行结果；会被深拷贝后放入 ``function_call_output`` item。

        返回值:
            ``None``。

        异常:
            复制 output 失败时传播对应异常。

        状态变化:
            向 ``context_items`` 追加一项，不修改面向人的 ``messages``。
        """

        self.context_items.append(
            {
                "type": "function_call_output",
                "call_id": call_id,
                "output": deepcopy(output),
            }
        )

    def _build_trim_units(self) -> list[_ContextTrimUnit]:
        """把当前 context_items 划分为有序裁剪单元并累计序列化字符数。"""

        units: list[_ContextTrimUnit] = []
        unit_by_call_id: dict[str, _ContextTrimUnit] = {}
        for index, item in enumerate(self.context_items):
            item_type = item.get("type")
            call_id = item.get("call_id")
            if item_type == "function_call" and isinstance(call_id, str):
                unit = _ContextTrimUnit(item_indices=[index])
                unit_by_call_id[call_id] = unit
                units.append(unit)
            elif item_type == "function_call_output" and isinstance(call_id, str):
                paired = unit_by_call_id.get(call_id)
                if paired is None:
                    # 批次预检保证 call 与 output 配对；孤儿 output 理论不应出现，
                    # 出现时按独立单元裁剪而不是抛异常，避免裁剪路径放大故障。
                    units.append(_ContextTrimUnit(item_indices=[index]))
                else:
                    paired.item_indices.append(index)
            else:
                units.append(_ContextTrimUnit(item_indices=[index]))
        for unit in units:
            unit.char_size = sum(
                len(json.dumps(self.context_items[index], ensure_ascii=False))
                for index in unit.item_indices
            )
        return units

    def trim_context(self, max_chars: int | None) -> int:
        """按字符预算从最旧单元开始裁剪 Responses API 上下文。

        参数:
            max_chars: 上下文序列化字符总数上限；``None`` 表示不裁剪。

        返回值:
            被移除的裁剪单元数；未超预算或未启用时为 ``0``。

        异常:
            item 含无法 JSON 序列化对象时传播 ``TypeError``；此时会话保持不变。

        状态变化:
            超预算时原地重建 ``context_items``：从最旧单元开始整单元移除，直到
            总量回到预算内或只剩最后一个单元（宁可超预算也不清空上下文）。
            ``function_call`` 与其 ``function_call_output`` 同属一个单元，同生共死；
            ``messages``、``instructions``、``tools`` 和会话身份不受影响。
        """

        if max_chars is None:
            return 0
        units = self._build_trim_units()
        total = sum(unit.char_size for unit in units)
        if total <= max_chars:
            return 0
        # ponytail: 每次调用全量重算 O(n)，MVP 会话规模（数百 item）足够；
        # 超大会话再演进为增量缓存序列化长度。
        removed = 0
        while total > max_chars and len(units) > 1:
            total -= units[0].char_size
            units.pop(0)
            removed += 1
        kept_indices = {
            index for unit in units for index in unit.item_indices
        }
        self.context_items = [
            item
            for index, item in enumerate(self.context_items)
            if index in kept_indices
        ]
        return removed

    def get_context_messages(self) -> list[dict[str, Any]]:
        """返回当前 Responses API 上下文的深拷贝。

        参数:
            无。

        返回值:
            ``context_items`` 的递归深拷贝，调用方可安全修改。

        异常:
            上下文内对象无法深拷贝时传播对应复制异常。

        状态变化:
            不修改会话；返回对象与内部嵌套字典、列表相互隔离。
        """

        return deepcopy(self.context_items)

    def save(self) -> Path:
        """把当前会话以 UTF-8 JSON 原子保存到历史记录目录。

        参数:
            无。

        返回值:
            最终 JSON 文件路径，格式为
            ``storage_root/owner_id/peer_id/conversation_id.json``。

        异常:
            OSError: 目录创建、临时文件写入、同步或原子替换失败时抛出。
            TypeError: tools、messages 或 context_items 含不可 JSON 序列化对象时抛出。
            发生异常时会尽力删除本次创建的临时文件，旧的目标文件保持不变。

        状态变化:
            创建所需目录并写入/替换会话文件；不改变会话身份、消息、上下文、指令
            或工具定义。
        """

        target_directory = self.storage_root / self.owner_id / self.peer_id
        target_directory.mkdir(parents=True, exist_ok=True)
        target_path = target_directory / f"{self.conversation_id}.json"
        payload = {
            "schema_version": 1,
            "owner_id": self.owner_id,
            "peer_id": self.peer_id,
            "id": self.id,
            "conversation_id": self.conversation_id,
            "saved_at": datetime.now(timezone.utc).isoformat(),
            "instructions": self.instructions,
            "tools": deepcopy(self.tools),
            "messages": deepcopy(self.messages),
            "context_items": deepcopy(self.context_items),
        }

        temporary_path: Path | None = None
        try:
            with tempfile.NamedTemporaryFile(
                mode="w",
                encoding="utf-8",
                newline="\n",
                dir=target_directory,
                prefix=f".{self.conversation_id}.",
                suffix=".tmp",
                delete=False,
            ) as temporary_file:
                temporary_path = Path(temporary_file.name)
                json.dump(payload, temporary_file, ensure_ascii=False, indent=2)
                temporary_file.write("\n")
                temporary_file.flush()
                os.fsync(temporary_file.fileno())

            # 临时文件与目标位于同一目录/文件系统，os.replace 才能提供可靠的原子切换。
            os.replace(temporary_path, target_path)
        except BaseException:
            if temporary_path is not None:
                temporary_path.unlink(missing_ok=True)
            raise
        return target_path

    def clear(self) -> None:
        """清空当前会话的可读消息和 Responses API 上下文。

        参数:
            无。

        返回值:
            ``None``。

        异常:
            本方法仅清空内存列表，正常情况下不抛出异常。

        状态变化:
            原地清空 ``messages`` 和 ``context_items``，同时保留 owner/peer/id、
            conversation_id、instructions、tools 和 storage_root。
        """

        self.messages.clear()
        self.context_items.clear()
