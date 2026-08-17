# AgentGraph 状态编排内核教程

这套教程带你从“会调用 LLM 和工具的 Agent”走到“可暂停、可审计、可恢复的尝试（Attempt）”。
教程只描述当前 `experiment_system` 的实际接口；上位研究平台的设想会明确标为“后续规划”。

## 你会学到什么

读完后，你应能解释并追踪以下问题：一次 `Attempt` 为什么不能只靠一个可变字典；
谁可以改变控制状态；一个副作用何时才算被批准；进程在外部调用后崩溃时为什么不能
直接写成失败；以及如何用事件重放（replay）得到同一个状态。

贯穿示例是“研究 Agent 收到一个问题，先调用 `researcher`，再调用 `reviewer`，最后
生成结果”。示例中的名字是教学用的稳定 id，不是仓库里的凭据、URL 或运行配置。

## 读者画像与前置知识

- 了解 Agent、LLM、提示词和基本工具调用。
- 会读少量 Python 和 JSON，但不要求熟悉 Pydantic、SQLite 或异步编程。
- 不要求预先知道事件溯源（event sourcing）、事务 outbox（transactional outbox）、
  乐观并发（optimistic concurrency）、租约（lease）或崩溃恢复（crash recovery）。

## 推荐阅读路线

### 路线 A：先建立心智模型

适合第一次接触状态编排：
[01](01-why-state-orchestration.md) → [02](02-core-vocabulary.md) →
[03](03-architecture-overview.md) → [04](04-attempt-state-and-views.md) →
[05](05-command-event-reducer.md) → [17](17-end-to-end-walkthrough.md) →
[18](18-boundaries-and-roadmap.md) → [19](19-agentgraphinternet-integration.md) →
[20](20-integration-walkthrough.md) → [附录](appendix-glossary.md)。

### 路线 B：准备改内核

适合要写 Strategy、Backend 或 Repository：
[03](03-architecture-overview.md) → [05](05-command-event-reducer.md) →
[06](06-action-and-invocation.md) → [07](07-strategy-backend-and-guards.md) →
[08](08-engine-and-idempotency.md) → [09](09-repository-and-sqlite.md) →
[11](11-outbox-and-executor.md) → [12](12-deterministic-runner.md) →
[16](16-testing-and-extension.md) → [18](18-boundaries-and-roadmap.md) →
[19](19-agentgraphinternet-integration.md) → [20](20-integration-walkthrough.md)。

### 路线 C：遇到暂停或故障

适合排查运行中的 Attempt：
[04](04-attempt-state-and-views.md) → [10](10-artifacts-and-data-boundaries.md) →
[11](11-outbox-and-executor.md) → [13](13-control-and-external-input.md) →
[14](14-crash-recovery.md) → [15](15-cli-hands-on.md) →
[17](17-end-to-end-walkthrough.md) → [18](18-boundaries-and-roadmap.md) →
[19](19-agentgraphinternet-integration.md) → [20](20-integration-walkthrough.md)。

三条路线都应在最后阅读第 18 至 20 章：先确认边界，再理解现有
AgentGraphInternet 数据面和未来接入方式，避免把规划误认为已实现。

## 章节目录

1. [为什么需要状态编排](01-why-state-orchestration.md)
2. [核心词汇](02-core-vocabulary.md)
3. [架构总览](03-architecture-overview.md)
4. [Attempt 状态与只读视图](04-attempt-state-and-views.md)
5. [Command、Event 与 Reducer](05-command-event-reducer.md)
6. [Action 与 Invocation 生命周期](06-action-and-invocation.md)
7. [Strategy、Backend、预算与拓扑守卫](07-strategy-backend-and-guards.md)
8. [AttemptEngine、revision 与幂等](08-engine-and-idempotency.md)
9. [Repository、事务、SQLite WAL 与 checkpoint](09-repository-and-sqlite.md)
10. [ArtifactRef 与数据边界](10-artifacts-and-data-boundaries.md)
11. [Transactional outbox、lease 与 Executor](11-outbox-and-executor.md)
12. [确定性 Runner 与两个示例 Strategy](12-deterministic-runner.md)
13. [暂停、恢复、取消、过期与外部输入](13-control-and-external-input.md)
14. [崩溃恢复与 crash matrix](14-crash-recovery.md)
15. [Attempt CLI 动手实践](15-cli-hands-on.md)
16. [测试与扩展](16-testing-and-extension.md)
17. [端到端事件轨迹](17-end-to-end-walkthrough.md)
18. [边界、现有 Agent runtime 与路线图](18-boundaries-and-roadmap.md)
19. [与 AgentGraphInternet 结合：架构与接入边界](19-agentgraphinternet-integration.md)
20. [贯穿示例：编排 researcher 与 reviewer](20-integration-walkthrough.md)
21. [术语表与状态速查](appendix-glossary.md)

## 贯穿示例的总图

```mermaid
flowchart LR
    Q[研究问题] --> S[Strategy 决策]
    S --> P[ActionProposal]
    P --> G[预算/拓扑守卫]
    G --> A[ActionAccepted + outbox]
    A --> X[Executor]
    X --> R[researcher Backend]
    R --> O1[ActionOutcome]
    O1 --> S2[Strategy 再决策]
    S2 --> V[reviewer Backend]
    V --> O2[终态 Outcome]
    O2 --> F[AttemptSucceeded/Failed]
```

## 代码事实来源

教程中的实现映射以以下文件和测试为准：

- `../state.py`、`../commands.py`、`../events.py`、`../reducer.py`、`../engine.py`。
- `../store.py`、`../stores/memory.py`、`../stores/sqlite.py`、`../executor.py`。
- `../../Agent.py`、`../../AgentRemote.py`、`../../User.py`、`../../core.py` 和
  `../../tool_system/`（第 19、20 章的数据面事实来源）。
- `../../tests/test_experiment_*.py`（按章节给出更具体链接）。
- 状态语义规范：[状态设计稿](../../docs/superpowers/specs/2026-07-23-agent-orchestration-state-design.md)。
- 实现边界和验收证据：[实现计划](../../docs/superpowers/plans/2026-07-23-agent-orchestration-state-kernel.md)。

## 当前实现与规划的阅读标记

“当前实现”表示代码和测试已经证明；“设计原则”表示规范要求；“后续规划”表示
尚未提供可调用 API。尤其注意：当前 production Backend registry 为空，CLI 不会
执行 live LLM Action。

## 从这里开始

先读 [第 1 章：为什么需要状态编排](01-why-state-orchestration.md)。
