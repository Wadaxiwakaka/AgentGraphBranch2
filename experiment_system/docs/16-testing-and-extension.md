# 如何读测试并扩展 Strategy、Backend、Repository

> 上一章：[Attempt CLI 动手实践](15-cli-hands-on.md) · [目录](README.md) · 下一章：[端到端事件轨迹](17-end-to-end-walkthrough.md)

## 本章学习目标

- 按风险而不是按文件名阅读测试。
- 知道新增策略（Strategy）、后端（Backend）或仓储（Repository）时必须维持哪些契约。
- 用确定性测试证明扩展没有绕过 Engine 和 outbox。

## 测试是可执行的架构说明

这套内核的关键行为跨多个模块。按下面顺序读，比逐类浏览更容易：

```mermaid
flowchart LR
    M[模型/状态测试] --> R[Reducer 事件不变量]
    R --> G[Guard/Engine]
    G --> S[Repository contract]
    S --> X[Executor/Recovery crash]
    X --> D[Runner/Strategy vertical slice]
    D --> C[CLI 安全输出]
```

| 关注点 | 测试入口 |
| --- | --- |
| immutable 模型/Views | [`test_experiment_state.py`](../../tests/test_experiment_state.py) |
| Command/Action 严格形状 | [`test_experiment_commands.py`](../../tests/test_experiment_commands.py)、[`test_experiment_actions.py`](../../tests/test_experiment_actions.py) |
| 事件顺序/replay | [`test_experiment_reducer.py`](../../tests/test_experiment_reducer.py) |
| 预算/拓扑 | [`test_experiment_guards.py`](../../tests/test_experiment_guards.py) |
| Engine/幂等 | [`test_experiment_engine.py`](../../tests/test_experiment_engine.py) |
| Repository parity/SQLite | [`test_experiment_store_contract.py`](../../tests/test_experiment_store_contract.py)、[`test_experiment_sqlite_store.py`](../../tests/test_experiment_sqlite_store.py) |
| outbox/恢复 | [`test_experiment_executor.py`](../../tests/test_experiment_executor.py)、[`test_experiment_recovery.py`](../../tests/test_experiment_recovery.py)、[`test_experiment_crash_matrix.py`](../../tests/test_experiment_crash_matrix.py) |
| Strategy/Runner | [`test_experiment_strategies.py`](../../tests/test_experiment_strategies.py)、[`test_experiment_runner.py`](../../tests/test_experiment_runner.py) |

## 新增 Strategy

Strategy 扩展应只实现 [`contract.py`](../contract.py) 的 `initialize`/`on_event`：

1. 验证 `StrategyView.strategy_id` 和自己的 state envelope schema。
2. 只处理 eligible committed trigger，并返回匹配 `trigger_sequence_no`。
3. 用稳定 id 生成 ActionProposal；不直接调用 Backend/Repository。
4. 明确处理 rejection、failure、timeout、cancel、unknown 和 reconciliation。
5. 测试同输入产生相同 decision，state 可序列化且有界。

不要复制完整 AttemptState 到私有 strategy state；只保存推进所需的最小 cursor/stage。

## 新增 Backend

Backend 只实现 `async execute(NormalizedAction, ExecutionContext) -> ActionOutcome`。需要
`RECONCILABLE` 时还实现 `reconcile`。扩展测试至少证明：

- 检查 action/context identity 和幂等键；
- 返回严格 outcome，错误只含安全摘要/ArtifactRef；
- 重放 `REPLAY_SAFE` 时同 key 不重复副作用；
- exception 后 durable Action 保持 STARTED，交给 recovery；
- 不拿 Engine/Repository 句柄，不修改 AttemptState。

当前没有 `LiveLLMBackend`；接入时应先建立适配器并保留现有 Responses/tool contract 测试。

## 新增 Repository adapter

Repository 不是只实现 CRUD；必须通过共同 contract：

1. revision 竞争一个赢家。
2. command id + request hash 幂等。
3. Events/head/checkpoint/outbox/command result 原子 commit/rollback。
4. Event canonical/sequence/causal/replay 校验。
5. claim/lease 的 owner、expiry、status 检查。
6. terminal Action/Attempt 不留 outbox/open reservation。

优先复用 [`store.py`](../store.py) 的严格模型，而不是为新数据库另造松散字典接口。

## 一个逐步扩展练习

假设要新增“三阶段固定工作流 Strategy”：

1. 先在测试中用 ScriptedBackend 定义三个 action outcome。
2. Strategy state 只保存 next ordinal/current action id。
3. 每次只对当前 Action terminal trigger 推进。
4. 运行 Runner 到 terminal，断言 Event 类型顺序和 final hash。
5. 在第二 Action 的六个 crash 点注入故障，断言 policy。
6. 再用 InMemory/SQLite 两个 Repository 跑相同语义断言。

## 正确与错误做法

```text
正确：先测试接口不变量，再测试实现细节
正确：相同 contract suite 覆盖 memory/sqlite adapters
错误：只断言 final text，忽略 Action/Event/outbox 轨迹
错误：用 sleep 猜 lease 竞态而不注入 Clock
错误：为了测试方便给 Strategy 传 sqlite connection
```

## 常见误解

- **单元测试够了，不需要 crash matrix。** 副作用正确性发生在模块之间和事务边界。
- **确定性测试不需要真实 Repository。** 仍要用 SQLite contract 证明事务/WAL/hash chain。
- **新增 Strategy 要改 reducer。** 标准 Action/Event 已覆盖时通常不需要；Strategy 私有差异
  留在 envelope。

## 本章小结

扩展的安全路径是遵守小接口、复用严格模型，并用 contract、event invariant 和 crash
matrix 证明行为。测试关注“谁能做什么、何时持久化”，而不仅是最后输出。

## 思考题与练习

1. 为一个新 Backend 写出最小的幂等性 contract 清单。
2. 为什么新 Repository 必须重跑同一 store contract suite？
3. 哪些 Strategy 失败事件不能只用 happy-path 测试覆盖？
