# AgentGraph 可扩展工具系统设计

- 日期：2026-07-19
- 状态：已实现
- 适用范围：普通 `AgentRemote` 的模型工具

## 1. 背景

改造前，普通 Agent 的工具定义与执行分发分别硬编码在
`AgentRemote._build_tools()` 和 `AgentRemote._dispatch_tool()` 中。新增工具至少需要：

1. 修改 Responses API 工具 schema 构造逻辑；
2. 修改工具名分支和参数解析逻辑；
3. 在 `AgentRemote` 中继续增加与工具领域相关的代码；
4. 同时维护 schema 校验与 Python 代码校验，容易出现不一致。

当前只有 `send` 和 `close`，这种结构尚可工作，但工具数量增加后会使
`AgentRemote` 逐渐承担工具目录、参数契约、实例生命周期和业务执行等多种职责。

本设计把工具改造成显式注册、按 Agent 实例绑定、启动后冻结的模块，同时保留现有
Responses 工具循环、会话隔离和错误脱敏不变量。

## 2. 已确认的需求

1. 工具分为内置工具与外部扩展工具。
2. 内置工具默认全部启用。
3. 外部扩展工具由 `./ToolExtension` 管理。
4. `ToolExtension/__init__.py` 显式导入并列出可注册的工具类。
5. 每个普通 Agent 的 JSON 配置通过 `tools.extensions` 选择外部工具：`"all"` 表示全部启用，`"none"` 表示不启用，名称列表表示指定启用；省略等价于空列表。
6. 外部工具属于完全可信的本地代码，不需要沙箱或能力权限系统。
7. 每个工具实例直接持有当前 `AgentRemote` 实例，可以访问或修改其内部状态。
8. 工具只在进程启动时加载和绑定；运行期间不支持热加载。
9. 注册表构建完成后冻结，不允许动态增加、删除、替换或覆盖工具。
10. 内置工具和外部工具使用同一套类契约、参数校验和执行入口。

## 3. 目标

- 新增工具时不再修改 `AgentRemote` 的 schema 构造和工具名分支。
- 工具的名称、描述、参数模型和执行实现保持在同一个工具模块中。
- 每个 Agent 拥有独立的工具实例，不共享 Agent 引用或工具局部状态。
- 工具可以通过 `self.agent` 直接访问当前 Agent 的配置、会话、邻居和其它状态。
- 工具 schema、启用集合和名称映射在启动后保持稳定。
- 保持每个持久化 `function_call` 恰好对应一个 `function_call_output`。
- 保持严格参数校验、工具调用上限、取消传播和错误脱敏行为。
- 为需要网络连接、文件句柄或后台资源的工具提供明确生命周期。

## 4. 非目标

- 不支持运行时热加载、卸载或替换工具。
- 不自动扫描 `ToolExtension` 目录或递归发现子类。
- 不使用装饰器修改进程级全局注册表。
- 不允许外部工具覆盖同名内置工具。
- 不提供第三方不可信代码隔离、权限控制或进程沙箱。
- 不为工具实现在线安装、版本解析或依赖下载。
- 不向 `root` 的 `User` 运行时提供模型工具。
- 不改变 Agent 间 HTTP 协议、拓扑协议或会话 id 传播规则。

## 5. 设计原则

### 5.1 显式优于自动发现

工具类必须出现在对应包的 `__init__.py` 类元组中才会进入工具目录。文件存在本身不构成
注册，避免导入顺序、目录扫描和隐式副作用决定运行行为。

### 5.2 工具声明与 Agent 绑定分离

工具模块公开工具类，而不是模块级工具实例。注册表为每个 `AgentRemote` 单独实例化工具：

```python
tool = tool_class(agent)
```

因此模块级对象不会保存 Agent 引用，也不会在多个 Agent 或测试实例之间串联状态。

### 5.3 注册表冻结，工具状态可变

冻结的内容是：

- 工具名称集合；
- 名称到工具实例的映射；
- 暴露给模型的 schema 列表；
- 内置与扩展工具的选择结果。

工具实例仍然可以修改 `self.agent`，也可以维护自身缓存。冻结注册表不等于冻结 Agent
或工具实例。

### 5.4 运行时继续拥有 Responses 协议

工具只返回执行结果，不自行创建或追加当前调用的 `function_call_output`。`AgentRemote`
继续负责：

- 模型 output 整批预检；
- `call_id` 唯一性；
- 工具调用和响应步骤上限；
- 顺序执行工具；
- 取消传播；
- 工具结果与原 `call_id` 配对；
- 批次写入失败时回滚。

## 6. 建议目录结构

```text
AgentGraph/
├── Agent.py
├── AgentRemote.py
├── core.py
├── tool_system/
│   ├── __init__.py
│   ├── contract.py
│   ├── registry.py
│   └── builtin_tools/
│       ├── __init__.py
│       ├── send.py
│       └── close.py
├── ToolExtension/
│   ├── __init__.py
│   ├── text_stats.py
│   ├── agent_info.py
│   └── get_weather.py
└── tests/
    ├── test_tool_registry.py
    ├── test_builtin_tools.py
    └── test_agent_remote.py
```

`ToolExtension` 保留需求中指定的目录名。内部基础设施使用小写 Python 包名
`tool_system`。

## 7. 核心契约

### 7.1 参数基类

所有工具参数模型继承统一基类，禁止额外字段：

```python
from pydantic import BaseModel, ConfigDict


class ToolArguments(BaseModel):
    model_config = ConfigDict(extra="forbid")
```

注册表使用 `model_validate_json(raw_arguments, strict=True)` 解析模型返回的 JSON 字符串。
参数校验只在工具注册表这一系统接缝执行一次，handler 接收到的对象已经通过类型校验。

第一版继续保持当前严格 schema 约束：所有声明的属性都出现在 `required` 中，且
`additionalProperties` 为 `false`。

### 7.2 ToolSpec

`ToolSpec` 是不可变的工具声明：

```python
from dataclasses import dataclass
from pydantic import BaseModel


@dataclass(frozen=True, slots=True)
class ToolSpec:
    name: str
    description: str
    arguments_model: type[BaseModel]
```

约束：

- `name` 非空、最多 64 个字符，只允许字母、数字、下划线和连字符；
- `description` 必须是非空字符串；
- `arguments_model` 必须是 `ToolArguments` 的子类；
- 工具名在内置和全部扩展目录中全局唯一；
- 不允许通过注册顺序实现同名覆盖。

### 7.3 AgentTool

每个工具定义为 `AgentTool` 子类：

```python
from abc import ABC, abstractmethod
from typing import Any, ClassVar, TYPE_CHECKING

from pydantic import BaseModel

if TYPE_CHECKING:
    from AgentRemote import AgentRemote


class AgentTool(ABC):
    spec: ClassVar[ToolSpec]

    def __init__(self, agent: "AgentRemote") -> None:
        self.agent = agent

    def is_available(self) -> bool:
        return True

    def parameters_schema(self) -> dict[str, Any]:
        return self.spec.arguments_model.model_json_schema()

    async def startup(self) -> None:
        return None

    async def shutdown(self) -> None:
        return None

    @abstractmethod
    async def execute(self, arguments: BaseModel) -> dict[str, Any]:
        raise NotImplementedError
```

说明：

- `self.agent` 是真实的当前 Agent 实例，不是代理或副本；
- `is_available()` 只在启动构建注册表时调用一次；
- `parameters_schema()` 默认从 Pydantic 模型生成，动态 schema 工具可以覆盖；
- `startup()` 与 `shutdown()` 默认为空，供需要异步资源的工具覆盖；
- `execute()` 必须是异步方法，避免同步 I/O 阻塞事件循环；
- handler 返回 JSON 可序列化字典。

### 7.4 当前会话访问

工具执行发生在 `AgentRemote.response()` 已设置 `currentChatSpace` 之后，因此需要当前会话
的工具可以直接使用：

```python
chat = self.agent.currentChatSpace
```

基础类可以提供一个便利方法，将缺少当前会话转换为稳定运行时错误：

```python
def require_current_chat(self) -> ChatSpace:
    chat = self.agent.currentChatSpace
    if chat is None:
        raise ToolStateError("工具执行时不存在活动会话")
    return chat
```

不额外引入 `ToolContext`，也不复制或重复保存 `ChatSpace`。全部 Agent 状态仍由
`AgentRemote` 自身持有。

## 8. 显式工具目录

### 8.1 内置工具目录

`tool_system/builtin_tools/__init__.py`：

```python
from .close import CloseTool
from .send import SendTool

BUILTIN_TOOLS: tuple[type[AgentTool], ...] = (
    SendTool,
    CloseTool,
)
```

类元组顺序决定模型工具 schema 的稳定顺序。所有内置工具默认进入选择集合，但
`is_available()` 可以在启动时隐藏对当前 Agent 无意义的工具。例如没有下游邻居时，
`SendTool` 和 `CloseTool` 不应生成空 `to_id` enum。

### 8.2 外部工具目录

`ToolExtension/__init__.py`：

```python
from tool_system.contract import AgentTool

from .text_stats import TextStatsTool
from .agent_info import AgentInfoTool
from .get_weather import GetWeatherTool

EXTENSION_TOOLS: tuple[type[AgentTool], ...] = (
    TextStatsTool,
    AgentInfoTool,
    GetWeatherTool,
)
```

该元组是扩展工具的唯一注册入口：

- 目录中的未列出类不会注册；
- 元组保存类，不保存实例；
- 模块导入阶段不得执行网络、文件写入或修改 Agent 状态；
- 外部模块会在普通 Agent 启动时导入，但只有配置启用的类会被实例化；
- 需要异步资源时使用 `startup()`，不要在模块导入阶段创建资源。

教学 `text_stats` 工具的 `character_count` 使用 Python `len(text)` 统计 Unicode
code point，不表示用户感知的字素簇，也不是 UTF-8 字节数。

教学 `get_weather` 工具展示资源型扩展：只访问代码中固定的 Open-Meteo 地理编码和
天气端点，模型只能提供地点与温度单位，不能控制 URL。模块导入和注册表构建阶段不创建
HTTP 客户端；只有配置启用该工具并进入 lifespan 后，`startup()` 才创建专用
`httpx.AsyncClient`，`shutdown()` 负责关闭并清除客户端。

该工具为精确匹配 OpenAI function tool 示例而覆写 `parameters_schema()`，从而避免
Pydantic 自动生成额外 `title`。这只控制模型可见的 JSON；执行前仍由
`GetWeatherArguments` 和注册表的 `model_validate_json(..., strict=True)` 做本地严格
校验。完整 `ToolRegistry.schemas()` 对象必须通过等值测试锁定，防止手写 schema 与参数
模型漂移。

## 9. Agent 配置

在 `AgentConfig` 中增加可选的嵌套配置：

```python
ToolExtensionSelection = Literal["all", "none"] | list[ToolName]


class ToolSelectionConfig(BaseModel):
    extensions: ToolExtensionSelection = Field(default_factory=list)


class AgentConfig(BaseModel):
    # 现有字段保持不变
    tools: ToolSelectionConfig = Field(default_factory=ToolSelectionConfig)
```

Agent JSON 示例：

```json
{
  "id": "Agent1",
  "introduction": "协调任务。",
  "port": 9861,
  "key": "${AGENT1_KEY}",
  "openai_baseurl": "http://localhost:20128/v1",
  "openai_key": "${OPENAI_KEY}",
  "model": "example-model",
  "tools": {
    "extensions": [
      "text_stats",
      "agent_info",
      "get_weather"
    ]
  },
  "agents": []
}
```

配置语义：

- 省略 `tools` 或 `extensions` 时等价于 `[]`，不启用任何外部工具；
- 内置工具不需要出现在配置中；
- `extensions` 可为 `"all"`、`"none"` 或名称列表；名称区分大小写、匹配 `[A-Za-z0-9_-]{1,64}`，且列表不允许重复；
- 配置了目录中不存在的扩展工具时启动失败；
- `root` 仅允许 `"none"` 或空列表（包括省略后的默认值），且不导入 `ToolExtension`；
- 扩展工具的最终 schema 顺序遵循 `EXTENSION_TOOLS` 元组顺序，而不是 JSON 列表顺序。

配置名称表示允许当前 Agent 加载该扩展。若工具实例的 `is_available()` 返回 `False`，
该工具在本次启动中不会暴露给模型，但不视为配置错误。缺少依赖、配置无效或资源不可用等
不能安全降级的情况应在构造或 `startup()` 中抛出，而不是通过 `is_available()` 静默隐藏。

嵌套对象允许以后以向后兼容方式增加工具相关配置，而不需要继续向 `AgentConfig` 顶层
增加字段。

## 10. ToolRegistry

### 10.1 对外接口

```python
class ToolRegistry:
    @classmethod
    def build(
        cls,
        *,
        agent: AgentRemote,
        builtin_tool_classes: tuple[type[AgentTool], ...],
        extension_tool_classes: tuple[type[AgentTool], ...],
        enabled_extensions: Literal["all", "none"] | list[str],
    ) -> "ToolRegistry":
        ...

    def schemas(self) -> list[dict[str, Any]]:
        ...

    async def dispatch(
        self,
        name: Any,
        raw_arguments: Any,
    ) -> dict[str, Any]:
        ...

    async def startup(self) -> None:
        ...

    async def shutdown(self) -> None:
        ...
```

注册表不公开 `register()`、`remove()` 或 `replace()`。

### 10.2 构建流程

`build()` 按以下顺序执行：

1. 校验内置和扩展目录中的每一项都是 `AgentTool` 子类；
2. 校验每个类具有有效且不可变的 `ToolSpec`；
3. 对全部目录执行工具名冲突检查，包括未启用扩展工具；
4. 规范化并校验 `enabled_extensions`：`"all"` 选择全部、`"none"` 选择空集、名称列表不得重复且必须全部存在；
5. 选择所有内置工具和规范化后的扩展工具集合；
6. 按“内置目录顺序，再扩展目录顺序”实例化工具；
7. 调用一次 `is_available()`，过滤当前 Agent 不可用的工具；
8. 生成完整 Responses function schema；
9. 校验 schema 为严格 object schema；
10. 将名称映射包装为只读映射，并保存不可变 schema 元组。

如果工具类构造、可用性判断或 schema 生成失败，Agent 启动失败。注册错误转换为安全的
`ConfigError`，不把 secret 或完整底层异常写入公共错误文本。

### 10.3 冻结语义

内部建议使用：

```python
self._tools = MappingProxyType(dict(bound_tools))
self._schemas = tuple(deepcopy(schemas))
```

`schemas()` 每次返回深拷贝，避免 `ChatSpace` 或调用方修改注册表内部 schema。

每个参数 schema 会包装成当前项目使用的 Responses function 结构：

```python
{
    "type": "function",
    "name": tool.spec.name,
    "description": tool.spec.description,
    "parameters": tool.parameters_schema(),
    "strict": True,
}
```

注册表继续使用顶层 `name`、`description` 和 `parameters` 格式，不改成嵌套
`function` 对象，以保持现有 OpenAI Responses 请求与测试契约。

### 10.4 参数与结果处理

`dispatch()` 负责：

1. 校验工具名为已注册字符串；
2. 校验参数是 JSON 对象字符串；
3. 使用对应 `arguments_model` 严格解析；
4. 调用绑定到当前 Agent 的工具实例；
5. 校验返回值是可 JSON 序列化字典；
6. 返回结构化结果给 `AgentRemote`。

工具不需要再次手写 JSON 解码、额外字段检查和基本类型判断。涉及 Agent 动态状态的校验
仍由工具执行，例如 `send.to_id` 必须再次检查本地 peer allowlist。

## 11. Agent 组合与加载位置

`Agent.py` 是进程组合入口。仅在创建普通 Agent 时导入扩展工具目录：

```python
if self.config.id == "root":
    self.runtime = User(self.config)
else:
    from ToolExtension import EXTENSION_TOOLS

    self.runtime = AgentRemote(
        self.config,
        extension_tool_classes=EXTENSION_TOOLS,
    )
```

`AgentRemote` 构造函数增加可注入参数，便于测试：

```python
def __init__(
    self,
    config: AgentConfig,
    *,
    extension_tool_classes: tuple[type[AgentTool], ...] = (),
    ...,
) -> None:
```

注册表必须在 Agent 的配置、peer、客户端、会话映射和锁等基础状态完成初始化后构建，
确保工具构造函数拿到的是可用 Agent 实例。

为避免循环导入，`tool_system.contract` 只在 `TYPE_CHECKING` 中导入 `AgentRemote`。
外部工具模块也应采用同样方式标注类型。

## 12. 内置工具迁移

### 12.1 SendTool

`SendTool` 负责工具层适配，不接管 Agent 间 HTTP 实现：

```python
class SendTool(AgentTool):
    spec = ToolSpec(
        name="send",
        description="向一个直接可见的 Agent 发送消息。",
        arguments_model=SendArguments,
    )

    def is_available(self) -> bool:
        return bool(self.agent._peers)

    def parameters_schema(self) -> dict[str, Any]:
        # 保持当前 to_id enum schema 完全兼容。
        ...

    async def execute(self, arguments: SendArguments) -> dict[str, Any]:
        chat = self.require_current_chat()
        return await self.agent.send(
            arguments.msg,
            arguments.to_id,
            chat.conversation_id,
        )
```

### 12.2 CloseTool

`CloseTool` 使用相同方式调用现有 `AgentRemote.close()`。模型 schema 仍不包含
`conversation_id`，会话 id 继续由运行时从当前 `ChatSpace` 注入。

### 12.3 保留的实现

以下逻辑继续留在 `AgentRemote`：

- peer allowlist 与 URL 构造；
- Bearer 认证信息注入；
- HTTP 请求与协议校验；
- `send()` 和 `close()` 的安全结构化错误；
- Responses output、调用上限、取消和 call id 配对。

工具类是模型工具接口到 Agent 领域方法之间的适配器，不复制通信实现。

## 13. AgentRemote 集成

### 13.1 会话创建

`_get_or_create_chat_space()` 使用冻结注册表生成 schema 快照：

```python
chat = ChatSpace(
    ...,
    tools=self.tool_registry.schemas(),
)
```

同一 Agent 的所有新会话获得相同工具集合。现有会话仍持有创建时的深拷贝，不直接引用
注册表内部 schema。

### 13.2 工具分发

原 `_dispatch_tool()` 的工具名分支与手写参数解析删除，改为：

```python
async def _dispatch_tool(
    self,
    name: Any,
    raw_arguments: Any,
) -> dict[str, Any]:
    return await self.tool_registry.dispatch(name, raw_arguments)
```

也可以在响应循环中直接调用注册表，但保留薄委托方法有利于兼容现有测试接缝。

### 13.3 当前 Agent 绑定不变量

响应流程仍按以下顺序执行：

1. 获取或创建 `ChatSpace`；
2. 设置 `self.currentChatSpace = chat`；
3. 进入 Responses 工具循环；
4. 工具实例通过 `self.agent.currentChatSpace` 访问当前会话；
5. 无论成功、异常或取消，`finally` 将 `currentChatSpace` 恢复为 `None`。

当前普通 Agent 同一时刻只实际处理一个完整会话键，因此工具执行期间的
`currentChatSpace` 是确定的。

## 14. 生命周期

工具类构造函数只应保存 Agent 引用和执行同步校验，不应执行异步 I/O。

FastAPI lifespan 启动阶段：

1. 按注册顺序调用每个工具的 `startup()`；
2. 如果某个工具启动失败，按逆序关闭已经启动成功的工具；
3. 工具启动全部成功后才接受请求。

注册表跟踪是否已完成启动。生产环境中的 `dispatch()` 只允许在 startup 成功后执行；
直接构造 `AgentRemote` 的单元测试若会调用工具，也必须显式进入和退出工具生命周期。

FastAPI lifespan 关闭阶段：

1. 按注册逆序调用所有已启动工具的 `shutdown()`；
2. 再关闭 Agent 自己拥有的 OpenAI 和 HTTP 客户端；
3. 多个关闭错误只向上抛出第一个，同时尽力完成剩余清理。

这样外部工具可以安全管理数据库连接、独立 HTTP 客户端或文件资源，而无需修改
`AgentRemote.create_app()` 的具体工具分支。

## 15. 错误语义

### 15.1 启动错误

以下情况阻止 Agent 启动：

- 工具类不符合 `AgentTool` 契约；
- `ToolSpec` 非法；
- 内置或扩展目录存在重复名称；
- JSON 配置启用了未知扩展工具；
- 工具实例构造失败；
- 工具 schema 非法；
- 工具 `startup()` 失败。

启动错误以安全的 `ConfigError` 或启动失败呈现，不向公共输出包含 key、Authorization、
完整 URL 或敏感参数。

### 15.2 调用错误

保持现有稳定错误风格：

| 场景                     | 工具结果 code                  |
| ---------------------- | -------------------------- |
| 工具名未注册                 | `UNKNOWN_TOOL`             |
| 参数不是 JSON 对象字符串        | `INVALID_TOOL_ARGUMENTS`   |
| JSON 解析或 Pydantic 校验失败 | `INVALID_TOOL_ARGUMENTS`   |
| 工具普通异常                 | `TOOL_EXECUTION_ERROR`     |
| 工具被取消                  | `TOOL_CANCELLED`，随后继续传播取消  |
| 返回值不可序列化               | `TOOL_EXECUTION_ERROR`     |
| 本轮调用超过上限               | `TOOL_CALL_LIMIT_EXCEEDED` |

底层异常文本不返回模型。工具可以主动返回自己的安全领域错误，例如
`PEER_NOT_ALLOWED`。

## 16. 状态修改与事务约束

工具是完全可信代码，可以直接修改：

- `self.agent.chat_spaces`；
- `self.agent.currentChatSpace` 指向的会话内容；
- Agent 自定义内存、缓存和状态字段；
- peer、客户端或其它内部对象。

这种权限不意味着运行时可以自动回滚所有副作用。工具必须遵守：

1. 不自行追加、删除或伪造当前批次的 `function_call` 和 `function_call_output`；
2. 修改当前会话时优先使用 `ChatSpace` 的公开方法；
3. 不破坏 `_active_conversation_key`、pending count 和会话锁的不变量；
4. 外部 I/O 或不可逆操作自行设计幂等性；
5. 不创建依赖当前 `currentChatSpace` 的无管理后台任务；
6. 普通异常不会自动撤销已完成的网络、文件或任意 Agent 字段修改。

注册表只保证工具结果写回 Responses 上下文时的协议原子性，不为任意 Agent 状态提供
通用事务。

## 17. 安全边界

外部工具与 Agent 进程拥有相同权限，可以读取配置中的 secret、修改会话和发起任意本地
代码允许的操作。因此：

- 只有受信任代码可以加入 `ToolExtension`；
- 代码审查必须把扩展工具视为 Agent 核心代码；
- 工具描述和参数仍会暴露给模型，但 Agent 内部对象不会自动暴露给模型；
- 模型只能选择工具名和 schema 中的参数，不能选择要绑定的 Agent 实例；
- Agent 引用由启动时注册表注入，模型无法替换或伪造。

## 18. 测试设计

### 18.1 注册表单元测试

- 内置工具默认全部选择；
- 仅实例化 JSON 启用的扩展工具；
- 未知扩展名称导致启动失败；
- 配置重复名称导致校验失败；
- 内置与扩展同名导致启动失败；
- 两个 Agent 构建出不同工具实例，且分别绑定正确 Agent；
- 注册表不提供运行时注册或替换能力；
- `schemas()` 返回深拷贝；
- 工具顺序稳定；
- `is_available()` 只在构建时调用；
- 参数模型严格拒绝错误类型和额外字段；
- 工具结果必须为可序列化字典。

### 18.2 生命周期测试

- `startup()` 按注册顺序执行；
- `shutdown()` 按逆序执行；
- 启动中途失败时关闭已启动工具；
- 工具关闭异常不阻止其它工具和 Agent 客户端清理；
- 注入客户端的所有权规则保持不变。

### 18.3 内置工具测试

- 无 peer 时不暴露 `send` 和 `close`；
- 有 peer 时 schema 与当前实现完全一致；
- `to_id` enum 来自当前 Agent 本地 allowlist；
- `conversation_id` 不进入模型 schema；
- 执行时使用当前本地 `ChatSpace.conversation_id`；
- `send` 和 `close` 继续调用现有 Agent 领域方法。

### 18.4 AgentRemote 集成测试

- Responses 请求得到注册表生成的工具列表；
- 未知和畸形工具调用得到稳定错误；
- 工具实例可以修改当前 Agent 状态；
- 多个 function call 仍按顺序执行；
- 每个 call id 恰好得到一个 output；
- 工具普通异常、取消和结果写入失败保持现有回滚语义；
- 工具调用和 response step 上限不回归；
- `currentChatSpace` 在全部退出路径恢复为 `None`。

### 18.5 配置测试

- 旧 Agent JSON 未配置 `tools` 时继续通过；
- 普通 Agent 可以启用已注册扩展工具；
- root 配置 `"all"` 或指定扩展名称列表时失败；
- JSON 和持久化输出不会泄漏工具实例或 Agent 引用。

## 19. 兼容性要求

改造完成后，未配置扩展工具的现有 Agent 行为应保持不变：

- 有邻居时工具顺序仍为 `send`、`close`；
- 无邻居时不向模型传递 `tools`；
- `send` 和 `close` schema 保持当前严格格式；
- 模型不能提供 `conversation_id`；
- `ChatSpace` 仍只持久化 JSON schema，不持久化 Python 工具实例；
- 工具异常仍返回脱敏结构；
- 所有现有 call id、取消、并发和持久化测试继续通过。

## 20. 实施完成状态

1. 已新增 `tool_system.contract`、`ToolRegistry` 及对应契约、注册表测试。
2. 已扩展 `AgentConfig.tools.extensions`，支持三态选择、名称校验和 root 限制。
3. 已将 `send`、`close` 迁移为 `BUILTIN_TOOLS` 中的内置工具，并保持 schema 与会话 id 注入兼容。
4. 已在普通 Agent 组合入口导入 `ToolExtension.EXTENSION_TOOLS`，并在 Agent 完成基础状态初始化后构建独立注册表。
5. 已将 ChatSpace schema 切换为 `registry.schemas()`，将 `_dispatch_tool()` 保留为注册表薄委托。
6. 已把工具 startup/shutdown 接入 FastAPI lifespan；`ToolExtension` 默认登记但不默认启用 `text_stats`、`agent_info` 与 `get_weather` 三个教学工具。前两个展示无资源工具，天气工具展示精确 schema、固定外部端点与异步客户端生命周期。
7. 已补充回归测试，并更新 README、中文教程和交接文档中的扩展步骤。

## 21. 验收标准

当前实现必须满足：

1. 新增一个外部工具只需新增工具类、在 `ToolExtension/__init__.py` 加入类元组，并在
   Agent JSON 中启用，不修改 `AgentRemote`。
2. 新增一个内置工具只需新增工具类并加入 `BUILTIN_TOOLS`，不修改工具分发逻辑。
3. 每个工具实例持有且只持有构建它的当前 Agent 引用。
4. 两个 Agent 或两个测试运行时不会共享工具实例。
5. 启动后无法改变注册表的名称集合与 schema 集合。
6. 外部工具可以通过 `self.agent` 直接读写 Agent 内部状态。
7. 现有 `send`、`close` 行为、schema 和安全约束不回归。
8. 全部确定性测试通过，并覆盖注册、配置、生命周期、状态访问和协议错误路径。
