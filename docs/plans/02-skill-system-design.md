# Step 2：Skill 系统——markdown 指令包与显式启用

> 文档日期：2026-08-06
>
> 前置阅读：[00-gap-analysis.md](00-gap-analysis.md) 第 5 节；
> [01-context-management-design.md](01-context-management-design.md) 第 12 节
> （Skill 的字数限制是"instructions 不计入预算"盲区的封堵措施）。
>
> 状态：已实现并验收（2026-08-06，见第 11 节）；偏差与待完善见第 12、13 节。

## 1. 背景与目标

当前普通 Agent 的模型指令只有 `_build_instructions()`（`AgentRemote.py`）
拼出的静态块：Agent id、职责、caller id、`<peer_metadata>`。想给 Agent 补充
"怎么做研究""怎么写周报"这类**可复用的工作方法指令**，只能改
`introduction`——无法跨 Agent 复用、无法按需组合。

目标：引入 **Skill = 仓库级 markdown 指令包**，配置显式启用后注入
instructions。对标 Claude Code 的 SKILL.md，但取最懒形态：**纯文本注入，
不动工具系统**。

## 2. 决策记录（评审结论）


| #   | 决策                       | 备选与理由                                                          |
| --- | ------------------------ | -------------------------------------------------------------- |
| A   | 全局 `skills/` 目录 + 配置按名启用 | 备选每 Agent 独立目录；skill 天然跨 Agent 复用，且与 `ToolExtension/` 全局目录模式对称 |
| B   | 全量静态注入 instructions      | 备选只注入目录、模型按需请求加载；后者需新增工具与文件读取路径，待实际出现"指令太长"再做                  |
| C   | 启动时加载并冻结，不热加载            | 与冻结 `ToolRegistry` 一致；重启生效，启动早失败优于会话中途炸                        |
| D   | 只做静态文本注入，不触发工具           | 备选 skill 携带工具绑定；Step 3 知识库后再评估                                 |




## 3. Skill 文件格式

```
skills/<name>.md
```

前几行为 `key: value` 头，之后一个空行，再之后正文全为指令文本：

```markdown
name: research
description: 系统性多源调研方法

## 目标
接到调研类任务时，先明确范围与验收标准……

（正文即注入给模型的指令文本）
```

解析规则（`core.py` 新增，纯字符串处理，不引 YAML）：

- 逐行读取，遇到第一个空行前只接受 `key: value` 行；
- `name` 必填、非空、与文件名（去 `.md`）一致——名字即身份，两处不一致
直接拒绝，防止目录名与内容漂移；
- `description` 必填、非空（给运维者看的清单用途，不注入模型）；
- 空行之后全部视为正文，不再解析任何键；
- 正文字符数上限 **2000**（对齐 `introduction` 上限）；
- 文件必须是 UTF-8；解析失败产生安全 `ConfigError`，只报告文件名与原因，
不回显全文。



## 4. 目录加载与校验

新增 `skills/` 仓库级目录（与 `ToolExtension/` 平级），加载器启动时执行：

1. 扫描 `skills/*.md`（非递归），文件名去后缀为 skill 名；
2. 名字约束：`^[A-Za-z][A-Za-z0-9_-]*$`，全局唯一（重名拒绝）；
3. 逐文件解析第 3 节格式，任一失败即启动失败；
4. 结果为不可变映射 `{name: 正文}`，构建后冻结。

不做自动发现之外的任何注册机制——文件放进目录即注册，配置引用即启用
（注册 ≠ 启用，与 ToolExtension 相同）。

## 5. 配置设计

`AgentConfig` 新增：

```python
skills: list[str] = Field(default_factory=list)
```

- 默认 `[]`：不启用，行为与现状零差异；
- 列表内的名字必须已在 `skills/` 目录加载结果中存在（启动时校验，缺失即
`ConfigError` 报"未知 skill 名"）；
- 不得重复（复用 `_validate_extension_names` 同款校验语义）；
- 启用数量上限 **8**：封住 instructions 总膨胀（8 × 2000 = 16K 字符上限），
覆盖"instructions 不计入 `max_context_chars` 预算"的盲区；
- root 禁用：只允许 `[]`（root 无模型、无 instructions，与 `tools` 的 root
约束同思路，在 `_validate_relationships` 中落实）。



## 6. 注入方式

`_build_instructions()` 在现有块末尾追加（仅当启用列表非空）：

```text
<skills>
[skill: research]
（正文）
[skill: code-review]
（正文）
</skills>
```

- 注入顺序 = 配置 `skills` 列表顺序（确定性，便于测试与 diff 审查）；
- skill 文件是本地运维者写的**可信内容**，不需要 `<peer_metadata>` 式的
不可信声明，但保留标签包裹维持结构边界；
- 注入发生在 `_get_or_create_chat_space` 创建 `ChatSpace` 时——已有会话的
instructions 是创建时快照，不回填（与现状语义一致）。



## 7. 非目标

- 不做 skill → 工具绑定、参数化、条件触发；
- 不做运行时增删改与热加载；
- 不做 skill 的模型自选（模型看到什么由配置决定，不由模型决定）；
- 不改 `ChatSpace` 结构、HTTP 契约、归档格式（instructions 已在归档内，
自动随会话保存）。



## 8. 测试计划（先 RED）

新增 `tests/test_skill_loading.py`（目录与解析）：

1. 合法文件解析出 name/description/正文；正文含空行、`key:` 样式行时
  不再解析（空行后全为正文）；
2. 缺 name / 缺 description / name 与文件名不一致 / 正文超 2000 字符 /
  非法文件名 / 目录重名，各自产生 `ConfigError` 且消息只含文件名与原因；
3. 空目录加载为空映射，不报错。

新增到 `tests/test_config.py`：

1. `skills` 默认 `[]`；列表去重校验；root 配置非空 `skills` 被拒绝。

新增到 `tests/test_agent_remote.py`：

1. 启用 skill 后创建的新会话 instructions 含 `<skills>` 块与正文，顺序与
  配置一致；
2. 未启用时 instructions 与现状逐字节一致（零回归保证）；
3. 启用数量 > 8 被拒绝。



## 9. 验收标准

- 全部新测试 GREEN；完整离线测试（`-m "not live"`）通过
（`test_integration_chain` 基线预存失败除外，见 01 文档 §11）；
- 未配置 `skills` 的 Agent 行为与实现前零差异；
- 实现 diff 涉及：`core.py`（配置字段 + 解析/加载）、`AgentRemote.py`
（`_build_instructions` 注入）、`skills/`（新增目录，含 1 个示例 skill）、
README 配置表、测试文件，无其它文件改动。



## 10. 开放问题

无。实现中若发现 root 校验细节与 `tools` 不对称，以更严格者为准并记录。

## 11. 验收记录（2026-08-06）

- 分支 `feature/skill-system`（基于 `feature/context-budget-trim` 栈）；
- RED：`tests/test_skill_loading.py` 11 项（含参数化）、`tests/test_config.py` 5 项、
  `tests/test_agent_remote.py` 3 项先失败后实现转绿；
- 全量离线回归 `1190 passed, 1 deselected, 1 failed`；唯一失败为
  `test_integration_chain`，Step 1a 已确认的本机基线预存失败
  （`agents_setting` 本地端口 1234 vs 契约 20128），与本改动无关；
- `compileall` 与 `git diff --check` 通过。

## 12. 实现与设计的偏差记录

1. **目录重名校验未单独实现**：平铺目录内文件名 stem 全局唯一由文件系统
   保证，设计第 4 节的"重名拒绝"结构性不可触发，相应测试取消；
2. **启用数量 >8 的拒绝放在 `AgentConfig` 字段校验**（`max_length=8`），
   而非设计第 8 节所列的 agent_remote 测试位置；同属启动期拒绝，语义等价，
   测试落在 `tests/test_config.py`；
3. 新增 `tests/test_agent_remote.py` 补充了"目录存在但配置引用不存在名字
   → 启动即 `ConfigError`"的测试（设计第 5 节语义，原测试清单未单列）；
4. 示例 skill：`skills/research.md`（调研方法论），作为目录使用样例。

## 13. 待完善（触发条件延后，不预先排期）

| 待完善项 | 回头时机 |
| --- | --- |
| skill 按需加载（目录注入 + 模型自选） | 静态注入实际导致 instructions 过长或模型选择困难时 |
| skill → 工具绑定 | 出现"指令与工具成套"的真实需求时 |
| 头部重复键拒绝 | 当前末键覆盖，出现实际漂移问题再加 |