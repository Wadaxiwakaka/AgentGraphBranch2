# 附录：中英文术语表与状态速查

> 上一章：[贯穿示例：编排 researcher 与 reviewer](20-integration-walkthrough.md) · [目录](README.md) · 下一篇：无（阅读终点）

## 使用方法

这是一张复习表，不替代前面的因果解释。遇到相似概念时，先看“反例/判据”一列。

## 核心术语

| 中文名称 | English Term | 本项目含义 |
| --- | --- | --- |
| 状态编排 | state orchestration | 把决策、守卫、提交、副作用和恢复变成持久状态转换 |
| 不可变状态 | immutable state | 旧值不修改，新 Event 投影出新 revision |
| 事件溯源 | event sourcing | immutable Event 流是真相源，state 是投影 |
| 归约器 | reducer | 无 I/O 的 `old state + event -> new state` 纯函数 |
| 乐观并发 | optimistic concurrency | 提交时比较 expected revision，冲突则拒绝 |
| 幂等 | idempotency | 重复同一身份请求不产生第二份持久结果/副作用 |
| 事务 outbox | transactional outbox | ActionAccepted 与待投递记录同事务提交 |
| 租约 | lease | worker 在到期前的临时投递权，不是完成证明 |
| 重放 | replay | 从 Event 重建 state，或恢复投递原 Action（上下文需区分） |
| 检查点 | checkpoint | 可验证、可重建的特定 revision 快照 |
| 恢复策略 | recovery policy | REPLAY_SAFE / RECONCILABLE / NON_REPLAYABLE |
| 结果未知 | outcome unknown | 副作用可能发生但本地无法确认，不等于失败 |
| 控制面 | control plane | Engine/Event/outbox/recovery 决定能否和何时执行 |
| 数据面 | data plane | Backend/现有 Agent runtime 真正调用模型、工具或远端 |
| 会话键 | conversation key | 现有 runtime 用 `(from_id, conversation_id)` 隔离入站 ChatSpace，不是 Invocation id |
| 后端适配器 | Backend Adapter | 未来把已批准 NormalizedAction 映射到 live runtime，并把观察映射回 ActionOutcome 的边界 |

## 相似概念对照

| 概念 A | 概念 B | 判据或反例 |
| --- | --- | --- |
| `AttemptState` | `ChatSpace` | 前者是 event-sourced 控制聚合投影；后者是现有数据面会话/context 实现 |
| HTTP `request_id` | Command `command_id` | 前者当前只追踪且 AgentRemote 不据此去重；后者进入持久幂等 ledger |
| Attempt | Invocation | 一次 Attempt 可含多个 Invocation；后者由一个 Agent Action 拥有 |
| `ActionProposal` | `NormalizedAction` | 后者只在守卫通过后出现，并多出 reservation/idempotency key |
| Action | Event | Action 描述副作用单元；Event 是“提案/接受/开始/结果”等已提交事实 |
| Command | Event | Command 可拒绝；Event 已 commit，不可改写 |
| Strategy | Backend | Strategy 提议；Backend 执行已批准 Action |
| Engine | Runner | Engine 处理一个 Command；Runner 循环驱动 Start/Strategy/Executor/Finish |
| Repository | Reducer | Repository 事务保存/claim；Reducer 纯投影/校验，不 I/O |
| `ArtifactRef` | artifact 内容 | Ref 只有 hash/type/size/path；正文在 ArtifactStore，并受 capture policy 控制 |
| `FAILED` | `OUTCOME_UNKNOWN` | FAILED 是已观察失败；unknown 表示不能确定外部结果 |
| pause request | `PAUSED` | `PAUSE_REQUESTED` 可能仍有 STARTED；PAUSED 是持久化安全边界 |
| retry | replay | retry 是新 Action/new id；replay 是重放 Event 或同 id/key 恢复原 Action |
| checkpoint | Event 真相源 | checkpoint 可坏且可重建；Event 流损坏必须 fail-closed |
| LangGraph 风格共享 state | 本项目 Attempt aggregate | 前者常做节点数据合并；本项目强调事务、副作用、幂等、恢复和审计 |
| 内部 `send` 工具调用 | 顶层 Action | 前者可藏在一个 Invocation 内；后者有独立 accepted/outbox、预算和恢复记录 |

## AttemptPhase 速查

| phase | 是否可自动执行新 Action | 关键含义 |
| --- | --- | --- |
| `PLANNED` | 否 | 已创建，未启动 |
| `RUNNING` | 是 | 正常决策/执行 |
| `PAUSE_REQUESTED` | 否 | 请求已记录，等待安全边界 |
| `PAUSED` | 否 | 可跨进程恢复的安全暂停点 |
| `WAITING_EXTERNAL` | 否 | 恰好一个 approval/input/reconciliation 请求 |
| `CANCEL_REQUESTED` | 否 | 等待已开始 Action 的终态观察 |
| `SUCCEEDED` | 否 | 有 result_ref 的成功终态 |
| `FAILED` | 否 | 有 terminal_error 的失败终态 |
| `CANCELLED` | 否 | 已闭合取消终态 |
| `TIMED_OUT` | 否 | Attempt deadline 终态 |
| `INTERRUPTED` | 否 | 无法安全自动继续的终态 |

## ActionStatus 速查

| status | 是否终态观察 | 下一步 |
| --- | --- | --- |
| `PROPOSED` | 否 | REJECTED 或 ACCEPTED |
| `REJECTED` | 是 | Strategy 可据此再决策 |
| `ACCEPTED` | 否 | STARTED，或开始前 CANCELLED |
| `STARTED` | 否 | 任何执行终态；崩溃时按 policy |
| `SUCCEEDED` | 是 | BudgetSettled + Strategy trigger |
| `FAILED` | 是 | BudgetSettled + Strategy trigger |
| `TIMED_OUT` | 是 | BudgetSettled + Strategy trigger |
| `CANCELLED` | 是 | Started 后 settle，未开始则 release |
| `OUTCOME_UNKNOWN` | 是 | 请求 reconciliation；原状态不改写 |

## 三条终极判据

```mermaid
flowchart LR
    C[命令 Command] --> E[AttemptEngine]
    E --> V[Event 真相流]
    V --> R[Reducer 重放]
    R --> S[AttemptState 投影]
```

```text
想改变控制状态？必须经过 AttemptEngine -> Event -> Reducer。
想产生外部副作用？必须先有 committed ActionAccepted + outbox。
不知道远端是否成功？记录 OUTCOME_UNKNOWN，绝不猜 FAILED。
```

## 复习练习

1. 从对照表任选三组概念，各写一个错误实现反例。
2. 给一个 `STARTED + NON_REPLAYABLE` Action 写出恢复后的 phase 和下一条 external request kind。
3. 不看前文，画出 `PLANNED -> RUNNING -> PAUSE_REQUESTED -> PAUSED -> RUNNING` 的 Command/Event 对。
