"""演示如何实现一个拥有外部 HTTP 资源的扩展工具。

这个教学工具对应 OpenAI Function Calling 文档中的 ``get_weather`` 示例。
参数模型负责在本地校验模型返回的 JSON；工具声明负责向模型公开名称、用途
和参数结构。为了让最终传给 Responses API 的工具 JSON 与官方示例逐字段一致，
本工具会显式实现 ``parameters_schema``，避免 Pydantic 默认生成额外的 ``title``。

后续的天气查询实现会使用生命周期钩子管理异步 HTTP 客户端：模块导入和注册表
构建阶段不创建网络资源，``startup`` 获取客户端，``shutdown`` 负责释放客户端。
"""

from __future__ import annotations

import math
from typing import TYPE_CHECKING, Any, Literal

import httpx
from pydantic import ConfigDict, Field

from tool_system.contract import AgentTool, ToolArguments, ToolSpec, ToolStateError

if TYPE_CHECKING:
    from AgentRemote import AgentRemote


def _required_string(payload: dict[str, Any], key: str) -> str:
    """从外部 JSON 对象读取必需字符串，拒绝对象、数组和其它类型。

    Pydantic 只校验模型发给工具的参数，无法替我们信任第三方 HTTP 响应。因此
    Open-Meteo 返回的每个叶子字段也要在进入成功结果前逐一收窄到明确类型。
    错误信息只写本地字段名，不拼接外部值，避免异常日志或安全错误带出响应内容。
    """

    value = payload.get(key)
    if not isinstance(value, str):
        raise ValueError(f"外部响应字段 {key} 必须是字符串")
    return value


def _optional_string(payload: dict[str, Any], key: str) -> str | None:
    """读取允许缺省或为 null 的字符串字段，同时拒绝其它 JSON 类型。"""

    value = payload.get(key)
    if value is None or isinstance(value, str):
        return value
    raise ValueError(f"外部响应字段 {key} 必须是字符串或 null")


def _finite_number(
    payload: dict[str, Any],
    key: str,
    *,
    minimum: float | None = None,
    maximum: float | None = None,
) -> int | float:
    """读取有限 JSON 数字，并可选校验业务范围。

    Python 的 ``bool`` 是 ``int`` 的子类，所以必须显式排除；NaN 和 Infinity 虽然
    某些 JSON 解析器会接受，却不是严格 JSON 数字，也不能作为安全坐标或温度。
    """

    value = payload.get(key)
    if (
        isinstance(value, bool)
        or not isinstance(value, (int, float))
        or not math.isfinite(value)
    ):
        raise ValueError(f"外部响应字段 {key} 必须是有限数字")
    if minimum is not None and value < minimum:
        raise ValueError(f"外部响应字段 {key} 小于允许范围")
    if maximum is not None and value > maximum:
        raise ValueError(f"外部响应字段 {key} 大于允许范围")
    return value


class GetWeatherArguments(ToolArguments):
    """模型调用 ``get_weather`` 时必须提供的严格参数。"""

    # ConfigDict.title 会替换 Pydantic 默认使用的类名，成为 parameters.title。
    # json_schema_extra 中的 description 会覆盖上方类 docstring 自动生成的
    # parameters.description。这样可以保留面向开发者的中文 docstring，同时单独
    # 控制模型可见的英文说明。ToolArguments 的 extra="forbid" 配置会继续继承，
    # 因而默认 schema 仍包含 additionalProperties=false。
    model_config = ConfigDict(
        title="WeatherQueryParameters",
        json_schema_extra={
            "description": "Parameters used to retrieve current weather.",
        },
    )

    # 没有默认值意味着该字段必填；Field.description 会成为 JSON Schema 中同名
    # 属性的 description，并帮助模型理解地点应同时包含城市和国家。Field.title
    # 则显式替换 Pydantic 根据字段名自动生成的 "Location"。
    location: str = Field(
        title="Weather Location",
        description="City and country e.g. Bogotá, Colombia",
    )

    # Literal 同时承担 Python 类型约束和 Pydantic 运行时校验，并会自然生成
    # JSON Schema 的 enum。Field.title 将默认的 "Units" 改为更明确的标签；
    # 模型若返回 kelvin 等未声明值，会在执行工具前被拒绝。
    units: Literal["celsius", "fahrenheit"] = Field(
        title="Temperature Units",
        description="Units the temperature will be returned in.",
    )


class GetWeatherTool(AgentTool):
    """根据地点名称查询当前温度的教学扩展工具。"""

    # 地点名称需要先转换为经纬度，因此一次工具执行会依次访问地理编码接口和
    # 天气接口。URL 固定在工具实现中，模型只能提供查询参数，不能控制目标主机。
    _GEOCODING_URL = "https://geocoding-api.open-meteo.com/v1/search"
    _FORECAST_URL = "https://api.open-meteo.com/v1/forecast"

    spec = ToolSpec(
        name="get_weather",
        description="Retrieves current weather for the given location.",
        arguments_model=GetWeatherArguments,
    )

    def __init__(self, agent: AgentRemote) -> None:
        """创建尚未持有网络资源的、绑定到单个 Agent 的工具实例。

        每个 Agent 都会获得独立的 ``GetWeatherTool`` 实例。构造函数只记录状态，
        不创建客户端也不访问网络，这样配置校验、模块导入和注册表构建都保持无
        副作用；真正的资源所有权从 ``startup`` 开始，到 ``shutdown`` 结束。
        """

        super().__init__(agent)
        self._client: httpx.AsyncClient | None = None

    def parameters_schema(self) -> dict[str, Any]:
        """返回与 OpenAI 官方示例逐字段一致的 parameters 对象。

        默认的 ``model_json_schema`` 会加入根模型和字段的 ``title``。这些字段
        通常不会改变工具语义，但用户要求最终 JSON 完全一致，因此这里显式声明
        模型可见的参数 Schema。运行时参数仍由 ``GetWeatherArguments`` 校验，公开
        Schema 与本地校验各自保持清晰职责。

        为了明确展示差异，下面不是根据经验编写的示意结构，而是在当前项目环境中
        显式调用基类默认实现 ``AgentTool.parameters_schema(self)``，再按照
        ``ToolRegistry`` 的组合方式加入顶层字段后得到的真实完整工具 JSON。对应
        测试也直接调用同一个基类方法锁定这份结果：

        {
          "type": "function",
          "name": "get_weather",
          "description": "Retrieves current weather for the given location.",
          "parameters": {
            "additionalProperties": false,
            "description": "Parameters used to retrieve current weather.",
            "properties": {
              "location": {
                "description": "City and country e.g. Bogotá, Colombia",
                "title": "Weather Location",
                "type": "string"
              },
              "units": {
                "description": "Units the temperature will be returned in.",
                "enum": [
                  "celsius",
                  "fahrenheit"
                ],
                "title": "Temperature Units",
                "type": "string"
              }
            },
            "required": [
              "location",
              "units"
            ],
            "title": "WeatherQueryParameters",
            "type": "object"
          },
          "strict": true
        }

        因此默认结果相对目标 JSON 多出显式自定义的参数层元数据：
        ``ConfigDict.json_schema_extra`` 提供的 ``parameters.description``、
        ``ConfigDict.title`` 提供的 ``parameters.title``，以及两个 ``Field.title``。

        这些字段对 OpenAI Python SDK 的影响需要分层理解：

        1. 在当前锁定的 ``openai==2.45.0`` 中，Responses 的 function tool 把
           ``parameters`` 声明为任意 JSON Schema 字典。SDK 不会用 ``title`` 或
           ``description`` 做客户端参数校验，也不会删除它们；MockTransport 回归测试
           已确认 ``client.responses.create(...)`` 发出的工具对象与传入对象完全相同。
        2. OpenAI SDK 自带的 ``pydantic_function_tool()`` 也会保留 Pydantic 生成的根
           ``title``、字段 ``title`` 和模型 ``description``，再把同一 parameters 对象
           转换给 Responses API。这说明它们是 SDK 支持发送的 schema 注解，而不是
           Python SDK 无法识别的非法键。
        3. 它们不改变严格 JSON Schema 的约束语义。真正决定参数是否合法的仍是
           ``type``、``enum``、``required``、``additionalProperties`` 等约束关键字；
           ``title`` 和 ``description`` 本身不会让原本非法的参数变合法，反之亦然。
        4. 但它们并非对模型绝对无影响。OpenAI 文档说明 function 定义会注入模型上下文
           并按输入 token 计费，也建议为重要字段提供清晰标题和描述。因此
           ``description`` 是明确的提示信息，``title`` 也可能作为标签帮助模型理解；
           服务端如何加权每个注解没有公开保证，不能声称模型一定忽略它们。

        所以本覆写的理由不是“默认 Pydantic schema 会导致 SDK 报错”，而是同时满足
        用户要求的官方示例逐字段一致性，避免这些教学用的参数层标题和说明进入最终目标
        JSON，并减少少量 schema 输入。它只移除这些注解，不改变字段类型、枚举、必填
        集合或 ``additionalProperties=false``；真正执行时仍使用同一个 Pydantic 参数
        模型。
        """

        return {
            "type": "object",
            "properties": {
                "location": {
                    "type": "string",
                    "description": "City and country e.g. Bogotá, Colombia",
                },
                "units": {
                    "type": "string",
                    "enum": ["celsius", "fahrenheit"],
                    "description": "Units the temperature will be returned in.",
                },
            },
            "required": ["location", "units"],
            "additionalProperties": False,
        }

    async def startup(self) -> None:
        """为当前 Agent 的工具实例创建一个可复用异步 HTTP 客户端。

        复用客户端能够共享连接池，避免每次工具调用都重新建立连接。超时由工具
        自己设为较短的 10 秒，防止外部天气服务长时间占用 Agent 的 Responses
        循环。注册表保证已启动状态下重复 startup 不会再次调用本方法。
        """

        self._client = httpx.AsyncClient(timeout=10.0)

    async def shutdown(self) -> None:
        """关闭当前生命周期拥有的客户端，并清除对旧资源的引用。

        先把实例字段置空，确保即使 ``aclose`` 抛出异常，工具也不会在下一次
        lifespan 中误用半关闭客户端；注册表会继续清理其他工具并安全处理异常。
        """

        client = self._client
        self._client = None
        if client is not None:
            await client.aclose()

    async def execute(self, arguments: GetWeatherArguments) -> dict[str, Any]:
        """把地点解析为坐标，再返回该坐标当前的温度。

        ``arguments`` 已由注册表使用 ``GetWeatherArguments`` 严格校验，所以这里
        可以安全地把 ``units`` 直接传给天气服务。外部响应仍是不可信数据：缺少
        预期字段、HTTP 错误或 JSON 解析错误都会自然抛出普通异常，随后由注册表
        统一转换为稳定的 ``TOOL_EXECUTION_ERROR``，避免把 URL、响应正文或其他
        上游细节暴露给模型。
        """

        client = self._client
        if client is None:
            raise ToolStateError("get_weather 工具尚未启动")

        # Open-Meteo 的天气接口接受经纬度而不是自由文本地点，因此先使用地理
        # 编码接口取第一个匹配结果。params 让 httpx 负责安全的 URL 编码。
        geocoding_response = await client.get(
            self._GEOCODING_URL,
            params={
                "name": arguments.location,
                "count": 1,
                "format": "json",
            },
        )
        geocoding_response.raise_for_status()
        geocoding_payload = geocoding_response.json()
        if not isinstance(geocoding_payload, dict):
            raise ValueError("地理编码响应必须是 JSON 对象")

        locations = geocoding_payload.get("results", [])
        if not isinstance(locations, list):
            raise ValueError("地理编码 results 必须是数组")
        if not locations:
            # “没有匹配地点”属于可预期业务结果，不应伪装成执行异常；返回稳定错误
            # 让模型可以询问更精确的城市或国家名称，同时不再发起天气请求。
            return {
                "ok": False,
                "code": "LOCATION_NOT_FOUND",
                "message": "未找到对应地点",
            }

        location = locations[0]
        if not isinstance(location, dict):
            raise ValueError("地理编码地点必须是 JSON 对象")

        # 第三方 JSON 即使整体可序列化，叶子也可能是对象、数组、布尔值或非有限
        # 数字。先把将要用于第二次请求和最终结果的字段收窄到明确的安全原语；坐标
        # 还要满足地理范围，避免把畸形上游数据继续传播到天气接口。
        location_name = _required_string(location, "name")
        country = _optional_string(location, "country")
        latitude = _finite_number(
            location,
            "latitude",
            minimum=-90.0,
            maximum=90.0,
        )
        longitude = _finite_number(
            location,
            "longitude",
            minimum=-180.0,
            maximum=180.0,
        )

        # current 只请求本工具真正需要的 temperature_2m；timezone=auto 让返回的
        # observed_at 使用地点当地时区，temperature_unit 则直接采用严格枚举参数。
        weather_response = await client.get(
            self._FORECAST_URL,
            params={
                "latitude": latitude,
                "longitude": longitude,
                "current": "temperature_2m",
                "temperature_unit": arguments.units,
                "timezone": "auto",
            },
        )
        weather_response.raise_for_status()
        weather_payload = weather_response.json()
        if not isinstance(weather_payload, dict):
            raise ValueError("天气响应必须是 JSON 对象")

        current = weather_payload["current"]
        current_units = weather_payload["current_units"]
        if not isinstance(current, dict) or not isinstance(current_units, dict):
            raise ValueError("天气响应缺少当前天气对象")

        # 成功结果中的每个外部叶子都先经过类型白名单。这样可 JSON 序列化但语义
        # 错误的嵌套对象/数组不会被误标为 ok=true；有限数字检查也提前拒绝 NaN 和
        # Infinity。任何异常仍由注册表统一转换为脱敏执行错误。
        temperature = _finite_number(current, "temperature_2m")
        unit = _required_string(current_units, "temperature_2m")
        observed_at = _required_string(current, "time")

        # 返回值只包含字符串、数字、布尔值或 null 等 JSON 安全原语。注册表还会
        # 再次验证整个结果可序列化且不含 NaN/Infinity，之后才把它交还给模型。
        return {
            "ok": True,
            "location": location_name,
            "country": country,
            "temperature": temperature,
            "unit": unit,
            "observed_at": observed_at,
            "source": "Open-Meteo",
        }
