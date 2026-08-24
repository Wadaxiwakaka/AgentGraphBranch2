# 数据面能力差距分析与开发路线图

> 文档日期：2026-08-05
>
> 文档角色：本文是"数据面能力补齐"系列开发的总索引与决策留痕。每启动一个新步骤，
> 先在该步骤自己的设计文档中细化，实现以文档为准。
>
> 适用对象：接手本项目开发的新维护者（边了解项目边推进）。

## 1. 背景与问题

师兄口头交接时指出：项目还差**上下文管理、Skill、知识库**三项。本文对照市面上
已有的 Agent 产品（Claude Code / ChatGPT / Dify / Coze / Cursor 等）逐项核对，
确认这三项确实是数据面的主要缺口，并补充了若干师兄未提到的项，最终给出分步
开发顺序。

本路线图只涉及 **Agent 数据面**（`Agent.py` / `AgentRemote.py` / `User.py` /
`core.py` / `tool_system/` / `ToolExtension/`），不改变
[HANDOFF.md](../../HANDOFF.md) 第 12 节规定的 experiment_system 控制面路线图
（ResponseRunner → LiveLLMBackend → 其余策略与评估）。两条线互不接管，
可并行推进；开工前建议与师兄确认当前优先主线。

## 2. 项目现状一句话总结

| 层 | 内容 | 状态 |
| --- | --- | --- |
| 数据面 | 有向图 Agent 网络：JSON 配置邻接表、HTTP+Bearer 通信、Responses 工具循环、`ConversationKey` 会话隔离、拓扑发现、原子归档 | MVP 完成，`1159 passed, 1 deselected` |
| 控制面 | `experiment_system` Attempt 状态内核：Event Sourcing、SQLite WAL、outbox、暂停/恢复 | Phase 6 完成，Backend registry 为空，未接真实模型 |

数据面的**强项**是多 Agent 编排与会话隔离：局部 allowlist、逐跳会话 id 重命名、
FIFO/409 并发语义、工具批次原子回放。这些设计比多数开源框架严谨，是本项目的
核心资产，后续开发不得破坏（不变量清单见 HANDOFF 第 5 节）。

## 3. 对标差距分析

对标对象：Claude Code（skill、compact、工具生态）、ChatGPT/GPTs（记忆、
知识文件）、Dify/Coze（知识库、前端、工作流）、Cursor（上下文裁剪）、
LangChain/LangGraph（RAG、可观测性）。

| 能力 | 市面参考实现 | 本项目现状 | 严重度 | 结论 |
| --- | --- | --- | --- | --- |
| 上下文管理 | Claude Code compact、Cursor 裁剪 | `ChatSpace.context_items` 只增不减，长对话/深工具循环必然撑爆模型窗口 | 🔴 正确性问题 | **第一优先** |
| Skill 系统 | Claude Code SKILL.md | 只有静态 instructions + 工具清单，无可插拔指令包 | 🟠 高 | **第二** |
| 知识库/RAG | Dify/Coze 知识库 | 完全没有检索能力 | 🟠 高 | **第三**（复用 ToolExtension 模式，成本可控） |
| 长期记忆 | ChatGPT memory、Mem0/Letta | 会话 close 即归档，跨会话无记忆 | 🟡 中 | 可并入知识库阶段（同一套本地索引） |
| 流式输出 | 所有产品 SSE/WS | 同步 HTTP 等完整工具循环 | 🟡 中 | 动 HTTP 契约，动静大，后置 |
| 代码执行沙箱 | Claude Code/Codex/Manus | 仅 3 个教学工具 | 🟡 中 | 安全成本高，后置 |
| Web 搜索/浏览 | 几乎所有 Agent | 无 | 🟡 中 | 后置 |
| HITL 人机协同 | Manus 中途确认 | 控制面有 submit-input，数据面无 | 🟡 中 | 后置 |
| 可观测性 | LangSmith/Langfuse | 无指标/trace | 🟡 中 | 后置 |
| Web 前端 | Dify/Coze | 无（README 已列为后续） | 🟢 低 | 最后 |

**师兄说的三项确认准确，且严重度排序成立：上下文管理 → Skill → 知识库**
（按"不做会坏 → 做了马上有用 → 基础设施最重"排列）。

## 4. 为什么上下文管理排第一

这是唯一的**正确性缺口**，其它都是能力缺口：

- `context_items`（`core.py` `ChatSpace`）随每轮 `add_msg` /
  `append_response_items` / `append_tool_output` 单调增长；
- `_run_response_loop`（`AgentRemote.py`）每步把完整
  `chat.get_context_messages()` 作为 `input` 发给模型；
- 后果：长对话或一轮内大量工具输出（单轮上限 200 次工具调用）会让请求超过
  模型上下文窗口，模型调用直接报错，会话不可恢复。

Skill 和知识库反而**加剧**这个问题（注入更多指令与检索内容），所以必须先做。

## 5. 分步路线图

原则：每步 = 先落设计文档 → 写失败测试（RED）→ 最小实现（GREEN）→ 全量
回归 → 单独 commit。diff 控制在可一次审查完的规模。

| 步骤 | 文档 | 内容 | 状态 |
| --- | --- | --- | --- |
| Step 0 | 本文 | 差距分析与总索引 | 已完成 |
| Step 1a | [01-context-management-design.md](01-context-management-design.md) | 上下文字符预算与配对安全裁剪 | 已实现（本文档版） |
| Step 1b | （届期再写） | 超限时模型摘要压缩（compact），替代纯丢弃 | 后置，可选 |
| Step 2 | （届期再写） | Skill 系统：markdown 指令包 + 配置显式启用 | 待实现 |
| Step 3a | （届期再写） | 知识库摄取：本地文档 → 切块 → embedding → 本地索引 | 待实现 |
| Step 3b | （届期再写） | `search_knowledge` 扩展工具：检索注入工具循环 | 待实现 |
| Step 4+ | 不设文档 | 流式输出 / 长期记忆 / 沙箱 / 前端 / 可观测性 | 届期逐项评估 |

设计取舍总原则（贯穿所有步骤）：

- 优先复用现有机制（instructions 注入、ToolExtension、ChatSpace），不引入
  新框架、新数据库、新进程；
- 每个新能力默认**不启用**（配置显式 opt-in），保证既有测试与行为零变化；
- 不破坏 HANDOFF 第 5 节任何不变量，特别是 function_call / function_call_output
  一对一配对、完整 output 回放、会话隔离与脱敏边界。

## 6. 开放问题（开工前与师兄确认）

1. 当前主线是数据面三项，还是 HANDOFF 里的控制面路线图？本文假设前者。
2. Skill 的最终形态预期：静态注入够用，还是要按需触发（类似 Claude Code 的
   模型自选 skill）？Step 2 先做静态注入，预留演进。
3. 知识库预期规模：几十篇文档的本地研究场景（本文假设），还是需要外部向量库？
