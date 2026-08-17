# 教学扩展工具设计

- 状态：已实现

## 目标

在现有显式扩展目录中加入三个默认不启用的教学工具，让维护者可以直接复制其结构，
理解参数模型、工具声明、Agent 绑定、执行结果、目录登记、JSON 启用流程和异步资源
生命周期，而无需修改 `AgentRemote`。

## 工具一：text_stats

`text_stats` 展示无副作用的确定性工具：

- 参数模型只包含必填的 1..10000 字符字符串 `text`；
- 返回 `character_count`、`non_whitespace_character_count`、按空白切分的 `word_count` 和 `splitlines()` 的 `line_count`；`character_count` 是 Python `len(text)` 的 Unicode code point 数，不是用户感知字素簇或 UTF-8 字节；尾随换行不新增空行；
- 结果只包含 JSON 可序列化的布尔值与整数；
- 不访问 Agent 状态，不需要 `startup()` 或 `shutdown()`。

该工具用于教学 `ToolArguments`、Pydantic 字段约束、`ToolSpec` 和 `execute()`。

## 工具二：agent_info

`agent_info` 展示只读 Agent 绑定工具：

- 参数模型为空；
- 通过 `self.agent.get_profile()` 读取现有脱敏身份资料；
- 成功结果固定含 `ok: true`；再使用显式白名单，取出的身份字段仅为 `agent_id` 与 `introduction`；
- 不返回配置、任意 keys、模型 URL、peer、client、session 或其它运行时对象；
- 不对 `AgentConfig` 或 `PeerConfig` 调用整体 `model_dump()`。

该工具用于教学每个 Agent 独立实例绑定，以及扩展代码应采用明确字段白名单而不是序列化
整个运行时对象。

## 工具三：get_weather

`get_weather` 展示拥有独立异步 HTTP 资源的网络工具：

- 参数模型包含必填字符串 `location`，以及必填的
  `Literal["celsius", "fahrenheit"]` 参数 `units`；
- `location` 只表示城市和国家等地点文字，不能提供 URL、host、端口或任意目标地址；
- 地理编码固定请求
  `https://geocoding-api.open-meteo.com/v1/search`，天气查询固定请求
  `https://api.open-meteo.com/v1/forecast`；
- 先把地点解析为经纬度，再查询 `current=temperature_2m`，并将 `units` 映射到
  Open-Meteo 的 `temperature_unit`；
- 模块导入和工具构造阶段都不创建客户端，也不发送网络请求；
- `startup()` 为当前 Agent 绑定的工具实例创建专用 `httpx.AsyncClient`；
- `shutdown()` 关闭该客户端并清空引用，使注册表停止后仍可在新的 lifespan 中重新启动；
- 普通 HTTP、解析或执行异常不把底层 URL、响应正文或其它实现细节返回给模型，而由注册表
  统一转换为安全的 `TOOL_EXECUTION_ERROR`。

该工具用于教学固定目标的外部服务调用、每 Agent 独立资源所有权，以及 FastAPI lifespan
与工具 `startup()` / `shutdown()` 的组合。登记到扩展目录不等于启用；省略配置、空列表或
`"none"` 时不会实例化该工具，也不会创建 HTTP 客户端。配置 `"all"` 或显式选择
`"get_weather"` 后，它才会进入该 Agent 的工具生命周期。

### 模型可见的精确 schema

天气工具向 Responses API 暴露的完整 function tool 必须按 JSON 对象语义与以下结构逐字段
一致；对象键的书写顺序不参与比较，但不得出现 Pydantic 自动生成的模型或字段 `title`：

```json
{
  "type": "function",
  "name": "get_weather",
  "description": "Retrieves current weather for the given location.",
  "parameters": {
    "type": "object",
    "properties": {
      "location": {
        "type": "string",
        "description": "City and country e.g. Bogotá, Colombia"
      },
      "units": {
        "type": "string",
        "enum": ["celsius", "fahrenheit"],
        "description": "Units the temperature will be returned in."
      }
    },
    "required": ["location", "units"],
    "additionalProperties": false
  },
  "strict": true
}
```

这里存在两条互补但职责不同的路径：

1. `GetWeatherTool.parameters_schema()` 返回上面精确的 `parameters` 对象，避免
   `BaseModel.model_json_schema()` 默认加入 `title`；`ToolRegistry` 再从冻结的
   `ToolSpec` 添加顶层 `type`、`name`、`description` 和 `strict`。
2. 模型返回调用参数后，`ToolRegistry` 仍使用
   `GetWeatherArguments.model_validate_json(..., strict=True)` 做本地严格校验。
   `location: str`、`units: Literal[...]` 和 `ToolArguments.extra="forbid"` 分别保证类型、
   枚举与额外字段边界。

自定义 `parameters_schema()` 不能替代 Pydantic 参数模型。两份声明必须表达同一契约，
并由精确等值测试锁定，避免模型可见 schema 与本地执行校验随维护发生漂移。

## 文件与注册

新增：

- `ToolExtension/text_stats.py`
- `ToolExtension/agent_info.py`
- `ToolExtension/get_weather.py`
- `tests/test_example_extension_tools.py`

`ToolExtension/__init__.py` 显式导入三个类，并按以下稳定顺序登记：

1. `TextStatsTool`
2. `AgentInfoTool`
3. `GetWeatherTool`

目录文件包含面向后续维护者的注释，说明：

- 只有类元组中的工具才是受信任扩展；
- 元组顺序决定模型 schema 顺序；
- 加入目录不等于自动启用；
- 普通 Agent 还必须在 `tools.extensions` 中选择工具；
- root 不导入扩展目录；
- 需要异步资源的工具应在 `startup()` 获取、在 `shutdown()` 释放，不能在模块导入时创建。

## 配置

教学配置片段：

```json
{
  "tools": {
    "extensions": ["text_stats", "agent_info", "get_weather"]
  }
}
```

现有配置省略 `tools`、省略 `extensions`、使用空列表或使用 `"none"` 时行为不变，三个
教学工具都不会启用。`"all"` 会按 `EXTENSION_TOOLS` 的稳定顺序启用全部三个工具；名称
列表只启用明确列出的工具。

## 错误与安全

- 参数继续由注册表使用 `model_validate_json(..., strict=True)` 统一解析；
- 额外字段由 `ToolArguments` 拒绝；
- 工具不自行捕获普通执行异常，统一交给注册表转换为安全错误；
- 返回值不包含 secret，也不接受任意属性名或运行时对象路径；
- `text_stats` 与 `agent_info` 均无网络、文件写入或外部进程副作用；
- `get_weather` 是唯一网络示例，只能访问代码中固定的两个 Open-Meteo HTTPS 端点，
  不能把模型参数解释为 URL，也不能在模块导入或构造阶段执行 I/O；
- 天气工具使用项目已有的 `httpx`，不新增依赖，不修改 `uv.lock`，测试不得访问真实公网。

## 测试

新增测试覆盖：

- `EXTENSION_TOOLS` 的类与稳定顺序；
- 三个工具的 schema 名称、严格 object 参数，以及名称列表和 `"all"` 的稳定目录顺序；
- `text_stats` 的 Unicode、空白和多行统计；
- 空文本、额外字段和错误类型被严格拒绝；
- `agent_info` 的结果精确等于固定 `ok: true` 与 `agent_id`、`introduction` 白名单字段；
- 将结果序列化后不包含 Agent 或 peer 的测试密钥、模型地址和会话数据；
- `get_weather` 的完整注册表 schema 与本文 OpenAI function tool JSON 精确等值，且不包含
  任何额外 `title`；
- 天气参数缺失、类型错误、非法 `units` 和额外字段都返回 `INVALID_TOOL_ARGUMENTS`；
- 使用 mock transport 或等价替身验证固定地理编码端点、固定天气端点、请求参数和成功结果，
  不依赖真实 Open-Meteo 服务；
- 验证天气客户端只在 `startup()` 后存在，`shutdown()` 会关闭并清空客户端，重复 lifespan
  可以重新启动，未启用工具不会被实例化；
- 验证天气请求异常由注册表转换为脱敏 `TOOL_EXECUTION_ERROR`；
- `"none"`/空列表不启用示例，名称列表和 `"all"` 可以启用。

README、中文教程和 HANDOFF 补充示例位置、启用片段，以及复制示例新增工具的完整步骤。
