# Agent Orchestration State Kernel Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build an event-sourced Attempt state kernel that supports deterministic orchestration, transactional side-effect dispatch, pause/external-input boundaries, SQLite recovery, and audit replay without changing the current live Agent runtime.

**Architecture:** `AttemptEngine` is the only logical writer of `AttemptState`. Strict Commands produce versioned Events and transactional outbox Actions; a pure reducer rebuilds state, while repository adapters provide in-memory contract tests and SQLite WAL durability. A deterministic Backend and two small Strategies prove the complete control loop before any LiveLLM integration.

**Tech Stack:** Python 3.12+, Pydantic 2, `asyncio`, standard-library `sqlite3`, FastAPI-independent domain modules, pytest, pytest-asyncio.

## Design Authority

- Umbrella product/research architecture: `docs/superpowers/plans/2026-07-23-multi-agent-research-platform-design.md` until Task 1 moves it to `docs/superpowers/specs/2026-07-23-multi-agent-research-platform-design.md`.
- Normative state semantics: `docs/superpowers/specs/2026-07-23-agent-orchestration-state-design.md`.
- Execution order and commit boundaries: this implementation plan.
- If documents conflict on Attempt state, persistence, outbox, pause, or recovery, the state design wins.
- If documents conflict on research goals, strategy matrix, evaluation, metrics, or experimental fairness, the umbrella research design wins.

## Scope

This plan delivers:

- strict Attempt, Action, budget, Command, Event, and view models;
- a pure event reducer and canonical state hashing;
- topology and budget guards;
- an in-memory repository and SQLite WAL repository behind one contract;
- command idempotency, optimistic revision checks, snapshots, checkpoints, and replay;
- a transactional Action outbox, leases, and deterministic Executor;
- deterministic Single-Agent and Static-Workflow strategies;
- pause, resume, external input, and recovery-policy handling;
- a local Attempt CLI and crash-injection acceptance suite.

The following receive separate implementation plans after this plan passes its final checkpoint:

- `ResponseRunner` extraction and `ToolExecutionContext` migration;
- `LiveLLMBackend` and `DecentralizedPeerStrategy` integration;
- the remaining strategy matrix;
- Blackboard/shared memory;
- evaluation, metrics, Pareto analysis, reports, and Dashboard.

## Specification Coverage

| Approved state-design area | Owning tasks |
| --- | --- |
| Attempt state, views, canonical bytes | 2 |
| Event envelopes, replay, lifecycle, Actions, invocations | 3, 4, 8 |
| Budget, topology, batches | 5, 8 |
| Commands, deterministic identities, idempotency | 6, 7, 8 |
| Artifacts, Event store, checkpoints, outbox | 7, 9, 10, 12 |
| Strategy/Backend boundary and deterministic slice | 11, 12, 13 |
| Pause, cancellation, expiry, external input | 14, 15 |
| Recovery policies and crash boundaries | 16, 17 |
| Operator surface and security review | 18, 19 |
| Current-runtime isolation and regressions | Global constraints, Checkpoints C/D, Task 19 |

Sections of the umbrella design assigned to a later plan are listed under Deferred Follow-Up Plans; they are not silently reinterpreted by this plan.

## Global Constraints

- Add no runtime dependency; use standard-library `sqlite3` and existing Pydantic 2.
- Do not modify `Agent.py`, `AgentRemote.py`, `User.py`, `core.py`, `tool_system`, or existing HTTP contracts in this plan.
- The existing runtime must not import `experiment_system`.
- Do not modify, stage, display, or reveal user-owned changes in `agents_setting/Agent1.json` or `agents_setting/Agent2.json`.
- Before the final release gate, the repository owner must rotate any credential exposed in those local tracked changes and restore tracked configuration to environment-variable references. This is an explicit owner action, not permission for this plan to inspect or edit the files.
- Every persisted model uses `ConfigDict(extra="forbid")`; state and Event values are treated as immutable.
- Persist only JSON-compatible values, UUIDs, aware UTC timestamps, and safe artifact references.
- `AttemptPlanned` has `sequence_no = 1`; thereafter `state.revision == last_event.sequence_no`.
- Reducers perform no I/O and read no clock, random source, environment variable, model, tool, or process state.
- No generic control-state patching, `dict.update`, JSON Patch, or hidden transport retry.
- An Action must be committed before execution and must receive one terminal execution-observation status.
- Raw prompts, messages, credentials, headers, URLs, exceptions, and tracebacks never enter state, Events, logs, or test diagnostics.
- All offline tests are deterministic and make no real model, network, or external-tool call.
- Each task uses TDD, ends green, and creates one atomic commit before the next task begins.

## Planned File Map

```text
experiment_system/
  __init__.py              Public stable exports only
  __main__.py              `python -m experiment_system` entry
  contract.py              Clock/id/backend/strategy Protocols and shared results
  state.py                 AttemptState and immutable value models
  commands.py              Strict Command union and deterministic command ids
  events.py                Strict Event union and envelope materialization
  actions.py               Normalized Actions, outcomes, and recovery policy
  reducer.py               Pure Event -> AttemptState projection
  budget.py                Additive reservations and non-additive guards
  topology.py              Logical edge validation
  store.py                 Repository contract and commit/claim records
  artifacts.py             Atomic content-addressed artifact storage
  engine.py                Command handling and transition construction
  executor.py              Outbox claim, start, execute, and outcome loop
  runner.py                Strategy/Engine/Executor deterministic drive loop
  recovery.py              Startup reconciliation by Action recovery policy
  cli.py                   Local create/status/pause/resume/cancel commands
  stores/
    __init__.py
    memory.py              Contract-reference repository adapter
    sqlite.py              SQLite WAL repository adapter
  backends/
    __init__.py
    deterministic.py       Seeded scripted Action execution
  strategies/
    __init__.py
    single_agent.py        One invocation followed by finish
    static_workflow.py     Ordered multi-Agent workflow

tests/
  test_experiment_state.py
  test_experiment_reducer.py
  test_experiment_actions.py
  test_experiment_guards.py
  test_experiment_commands.py
  test_experiment_store_contract.py
  test_experiment_engine.py
  test_experiment_artifacts.py
  test_experiment_sqlite_store.py
  test_experiment_executor.py
  test_experiment_strategies.py
  test_experiment_runner.py
  test_experiment_control.py
  test_experiment_recovery.py
  test_experiment_cli.py
  test_experiment_crash_matrix.py
```

---

## Phase 0: Establish Documentation Authority

### Task 1: Preserve the umbrella design and declare precedence

**Files:**
- Move: `docs/superpowers/plans/2026-07-23-multi-agent-research-platform-design.md` -> `docs/superpowers/specs/2026-07-23-multi-agent-research-platform-design.md`
- Modify: `docs/superpowers/specs/2026-07-23-multi-agent-research-platform-design.md`
- Modify: `docs/superpowers/specs/2026-07-23-agent-orchestration-state-design.md`
- Modify: `README.md`

**Interfaces:**
- Consumes: the approved umbrella research design and approved state design.
- Produces: one explicit document hierarchy with no competing normative state semantics.

- [x] **Step 1: Move the umbrella design without rewriting history**

Run:

```powershell
git mv docs/superpowers/plans/2026-07-23-multi-agent-research-platform-design.md docs/superpowers/specs/2026-07-23-multi-agent-research-platform-design.md
```

Expected: Git reports one tracked rename; the implementation plan remains under `plans/`.

- [x] **Step 2: Add exact authority notices and links**

Place this notice immediately below the umbrella design title:

```markdown
> **Document role:** This is the umbrella architecture for product direction,
> experiment structure, strategy comparison, evaluation, and reporting. Detailed
> Attempt state, persistence, outbox, pause, and recovery semantics are normative in
> [Agent Orchestration State Design](2026-07-23-agent-orchestration-state-design.md).
```

Add a reciprocal `Parent design` link below the state design status. Update the README documentation navigation so both designs appear under an `Architecture designs` item, with the umbrella document listed first.

- [x] **Step 3: Verify the hierarchy and stale-path scope**

Run:

```powershell
rg -n "Document role|Parent design|agent-orchestration-state-design|multi-agent-research-platform-design" README.md docs/superpowers/specs
git diff --check
```

Expected: both designs cross-link; no design document under `specs/` references `docs/superpowers/plans/2026-07-23-multi-agent-research-platform-design.md`; diff check exits 0.

- [x] **Step 4: Commit the documentation normalization**

```powershell
git add README.md docs/superpowers/specs/2026-07-23-multi-agent-research-platform-design.md docs/superpowers/specs/2026-07-23-agent-orchestration-state-design.md
git commit -m "docs: establish architecture document precedence"
```

---

## Phase 1: Pure Domain Kernel

### Task 2: Define strict Attempt state and canonical serialization

**Files:**
- Create: `experiment_system/__init__.py`
- Create: `experiment_system/state.py`
- Create: `tests/test_experiment_state.py`

**Interfaces:**
- Produces: `AttemptPhase`, `ActionStatus`, `RecoveryPolicy`, `ArtifactRef`, `ErrorSummary`, `ResourceBudget`, `BudgetState`, `StrategyStateEnvelope`, `ActionState`, `InvocationState`, `ExternalRequest`, `AttemptState`, `StrategyView`, `AgentView`, `OperatorView`, view projectors, and `canonical_state_bytes()`.

- [x] **Step 1: Write strict state-model tests**

Tests must assert:

```python
assert AttemptPhase.PLANNED.value == "PLANNED"
assert ActionStatus.OUTCOME_UNKNOWN.value == "OUTCOME_UNKNOWN"
assert RecoveryPolicy.NON_REPLAYABLE.value == "NON_REPLAYABLE"
assert state.revision == 1
assert canonical_state_bytes(state) == canonical_state_bytes(
    AttemptState.model_validate(state.model_dump(mode="json"))
)
```

Also assert extra fields fail validation, timestamps without UTC offsets fail validation, artifact paths containing `..` or absolute roots fail validation, Strategy state accepts JSON but rejects arbitrary objects, and `value`/`artifact_ref` are mutually exclusive. Assert `full` artifact refs require hash plus relative path, `hashed` refs require a hash and forbid a path, and `metadata_only` refs forbid both content hash and path. Assert each view projector exposes only the fields authorized by section 9 of the state design and returns no mutable reference into `AttemptState`.

- [x] **Step 2: Run the tests and verify RED**

```powershell
$testTemp = Join-Path $env:TEMP ('agentgraph-state-model-red-' + [guid]::NewGuid())
.\.venv\Scripts\python.exe -m pytest tests\test_experiment_state.py -q -p no:cacheprovider --basetemp=$testTemp
```

Expected: collection fails because `experiment_system.state` does not exist.

- [x] **Step 3: Implement the strict models**

Use string enums and a shared strict base:

```python
class FrozenModel(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)


class AttemptPhase(StrEnum):
    PLANNED = "PLANNED"
    RUNNING = "RUNNING"
    PAUSE_REQUESTED = "PAUSE_REQUESTED"
    PAUSED = "PAUSED"
    WAITING_EXTERNAL = "WAITING_EXTERNAL"
    CANCEL_REQUESTED = "CANCEL_REQUESTED"
    SUCCEEDED = "SUCCEEDED"
    FAILED = "FAILED"
    CANCELLED = "CANCELLED"
    TIMED_OUT = "TIMED_OUT"
    INTERRUPTED = "INTERRUPTED"
```

Implement all fields exactly as sections 8 and 9 of the state design. Use tuples for persisted ordered collections. Validate `revision >= 1`, aware UTC timestamps, SHA-256 lowercase hex hashes, safe relative artifact paths, unique Action/invocation ids, and the budget invariant. Implement explicit `to_strategy_view()`, `to_agent_view()`, and `to_operator_view()` projectors; never serialize the full state and subtract fields. Canonicalize with:

```python
def canonical_state_bytes(state: AttemptState) -> bytes:
    payload = state.model_dump(mode="json", exclude_none=True)
    return json.dumps(
        payload,
        ensure_ascii=False,
        allow_nan=False,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")
```

Export only stable model names from `experiment_system/__init__.py`.

- [x] **Step 4: Run state tests and verify GREEN**

Run the Step 2 command. Expected: all tests in `test_experiment_state.py` pass.

- [x] **Step 5: Commit the state models**

```powershell
git add experiment_system/__init__.py experiment_system/state.py tests/test_experiment_state.py
git commit -m "feat: define orchestration attempt state"
```

### Task 3: Add versioned lifecycle Events and the pure reducer

**Files:**
- Create: `experiment_system/events.py`
- Create: `experiment_system/reducer.py`
- Create: `tests/test_experiment_reducer.py`
- Modify: `experiment_system/__init__.py`

**Interfaces:**
- Consumes: Task 2 state models and `canonical_state_bytes()`.
- Produces: `EventEnvelope`, typed lifecycle Events, `DomainEvent`, `apply_event()`, `replay_events()`, and `StateTransitionError`.

- [x] **Step 1: Write reducer creation and lifecycle tests**

Create explicit Events with fixed UUIDs and aware UTC times. Assert:

```python
planned = apply_event(None, attempt_planned(sequence_no=1))
assert planned.phase is AttemptPhase.PLANNED
assert planned.revision == 1

running = apply_event(planned, attempt_started(sequence_no=2))
assert running.phase is AttemptPhase.RUNNING
assert running.revision == 2

replayed = replay_events((
    attempt_planned(sequence_no=1),
    attempt_started(sequence_no=2),
))
assert canonical_state_bytes(replayed) == canonical_state_bytes(running)
```

Also assert only `AttemptPlanned` accepts `state=None`, duplicate/out-of-order/sequence-gap Events fail, wrong Attempt ids fail, every causal parent names an earlier Event in the same Attempt, unknown schema versions fail with `UnsupportedEventSchema`, `AttemptStarted` from `RUNNING` fails, terminal Events are exclusive, and Events after an execution terminal fail.

- [x] **Step 2: Run reducer tests and verify RED**

```powershell
$testTemp = Join-Path $env:TEMP ('agentgraph-reducer-red-' + [guid]::NewGuid())
.\.venv\Scripts\python.exe -m pytest tests\test_experiment_reducer.py -q -p no:cacheprovider --basetemp=$testTemp
```

Expected: collection fails because `experiment_system.events` and `experiment_system.reducer` do not exist.

- [x] **Step 3: Implement lifecycle Event models**

Use a strict common envelope containing schema version 1, fixed ids, sequence number, Event type, command id, optional causal parent, logical time, and aware UTC wall time. Parse the version and discriminator before the union; version 1 is the baseline and an unknown version fails explicitly until a future version supplies a reviewed upcaster. Implement these initial Event types:

```text
AttemptPlanned
AttemptStarted
AttemptSucceeded
AttemptFailed
AttemptCancelled
AttemptTimedOut
AttemptInterrupted
```

`AttemptPlanned` carries every value required to create the initial `AttemptState`; later Events carry only their domain-specific data. Define `DomainEvent` as a discriminated union on `event_type`.

- [x] **Step 4: Implement explicit reducer handlers**

Use a handler mapping keyed by concrete Event type. `apply_event()` must validate Attempt identity, exact next sequence, legal previous phase, and terminal exclusivity before returning `model_copy(update=validated_changes)`. `replay_events()` starts with `state=None`, rejects an empty stream, and applies Events in order. Do not catch validation failures into generic exceptions; raise `StateTransitionError` with a stable code and no payload values.

- [x] **Step 5: Run reducer tests and verify GREEN**

Run the Step 2 command. Expected: all reducer tests pass.

- [x] **Step 6: Commit lifecycle replay**

```powershell
git add experiment_system/__init__.py experiment_system/events.py experiment_system/reducer.py tests/test_experiment_reducer.py
git commit -m "feat: add event-sourced attempt lifecycle"
```

### Task 4: Model the complete Action lifecycle

**Files:**
- Create: `experiment_system/actions.py`
- Create: `tests/test_experiment_actions.py`
- Modify: `experiment_system/state.py`
- Modify: `experiment_system/events.py`
- Modify: `experiment_system/reducer.py`

**Interfaces:**
- Consumes: `ActionState`, `InvocationState`, lifecycle Event envelope, and `apply_event()`.
- Produces: `ActionType`, `ActionProposal`, `NormalizedAction`, `ActionOutcome`, `ActionExecutionStatus`, `InvocationStatus`, and all Action/invocation lifecycle Events and reducer handlers.

- [x] **Step 1: Write strict Action and transition tests**

Use one fixed proposal and assert:

```python
assert proposal.action_type is ActionType.INVOKE_AGENT
assert proposal.target_ids == ("worker-a",)
assert proposal.recovery_policy is RecoveryPolicy.REPLAY_SAFE

state = apply_event(state, action_proposed(sequence_no=3))
state = apply_event(state, action_accepted(sequence_no=4))
state = apply_event(state, action_started(sequence_no=5))
state = apply_event(state, action_succeeded(sequence_no=6))
assert state.actions[0].status is ActionStatus.SUCCEEDED
```

Also assert duplicate Action/invocation ids, targets outside the stable id format, empty target sets for target-required Action types, result payloads instead of artifact refs, `STARTED` before `ACCEPTED`, two terminal observations, `OUTCOME_UNKNOWN` before `STARTED`, and invocation completion before invocation start all fail.

- [x] **Step 2: Run Action tests and verify RED**

```powershell
$testTemp = Join-Path $env:TEMP ('agentgraph-actions-red-' + [guid]::NewGuid())
.\.venv\Scripts\python.exe -m pytest tests\test_experiment_actions.py -q -p no:cacheprovider --basetemp=$testTemp
```

Expected: collection fails because `experiment_system.actions` does not exist.

- [x] **Step 3: Implement normalized Action contracts**

Define these Action types for the vertical slice:

```python
class ActionType(StrEnum):
    INVOKE_AGENT = "INVOKE_AGENT"
    SEND_MESSAGE = "SEND_MESSAGE"
```

`ActionProposal` carries `action_id`, type, actor, targets, invocation id, causal Event id, payload artifact, recovery policy, requested timeout, call depth, optional `batch_id`, and optional `retry_of_action_id`. `NormalizedAction` adds the accepted reservation id and idempotency key. `ActionOutcome` is a discriminated union for success, failure, timeout, cancellation, and outcome unknown; content is always an artifact reference or safe `ErrorSummary`.

- [x] **Step 4: Add Action Events and reducer handlers**

Implement:

```text
ActionProposed
ActionRejected
ActionAccepted
ActionStarted
ActionSucceeded
ActionFailed
ActionTimedOut
ActionCancelled
ActionOutcomeUnknown
ActionOutcomeReconciled
InvocationRequested
InvocationStarted
InvocationCompleted
InvocationFailed
```

The reducer appends one `ActionState` at proposal and replaces that tuple entry by id on later Events. For `INVOKE_AGENT`, `InvocationRequested` creates the matching invocation summary, start/outcome Events advance both lifecycles explicitly, and ids cannot be reused by another Action. Reconciliation attaches a separate reconciled outcome without changing the original `OUTCOME_UNKNOWN` execution-observation status.

- [x] **Step 5: Run Action and reducer tests**

```powershell
$testTemp = Join-Path $env:TEMP ('agentgraph-actions-green-' + [guid]::NewGuid())
.\.venv\Scripts\python.exe -m pytest tests\test_experiment_actions.py tests\test_experiment_reducer.py -q -p no:cacheprovider --basetemp=$testTemp
```

Expected: all selected tests pass.

- [x] **Step 6: Commit the Action lifecycle**

```powershell
git add experiment_system/actions.py experiment_system/state.py experiment_system/events.py experiment_system/reducer.py tests/test_experiment_actions.py
git commit -m "feat: model orchestration action lifecycle"
```

### Task 5: Enforce budget and logical topology before acceptance

**Files:**
- Create: `experiment_system/budget.py`
- Create: `experiment_system/topology.py`
- Create: `tests/test_experiment_guards.py`
- Modify: `experiment_system/state.py`

**Interfaces:**
- Consumes: `AttemptState` and tuples of `ActionProposal`.
- Produces: `BudgetGuard.evaluate_batch()`, `TopologyGuard.evaluate()`, `ReservationPlan`, and `GuardRejection`.

- [x] **Step 1: Write budget and topology guard tests**

Cover these exact cases:

```python
plan = budget_guard.evaluate_batch(state, (first_action, second_action))
assert plan.accepted is True
assert plan.reservations[0].action_id == first_action.action_id
assert plan.next_budget.counters[ResourceKind.COORDINATION_ACTIONS].reserved == 2

rejection = topology_guard.evaluate(
    action=forbidden_edge_action,
    allowed_edges=frozenset({("root", "worker-a")}),
)
assert rejection.code == "TOPOLOGY_EDGE_FORBIDDEN"
```

Assert an over-budget parallel batch accepts no member, consumed plus reserved never exceeds limit, expired deadlines reject, call-depth and concurrency limits reject, and duplicate targets are rejected by the model before guard execution.

- [x] **Step 2: Run guard tests and verify RED**

```powershell
$testTemp = Join-Path $env:TEMP ('agentgraph-guards-red-' + [guid]::NewGuid())
.\.venv\Scripts\python.exe -m pytest tests\test_experiment_guards.py -q -p no:cacheprovider --basetemp=$testTemp
```

Expected: collection fails because the guard modules do not exist.

- [x] **Step 3: Implement pure guard results**

`BudgetGuard.evaluate_batch()` must calculate all requested reservations on local copies, reject the entire batch on the first stable failure code, and return a new `BudgetState` only on success. It receives `now_utc` as an argument; it does not read the clock. `TopologyGuard.evaluate()` accepts an immutable set of directed `(actor_id, target_id)` edges and checks every target.

Use these stable rejection codes:

```text
BUDGET_EXHAUSTED
ATTEMPT_DEADLINE_EXPIRED
MAX_CALL_DEPTH_EXCEEDED
MAX_CONCURRENCY_EXCEEDED
TOPOLOGY_EDGE_FORBIDDEN
```

- [x] **Step 4: Run state, Action, and guard tests**

```powershell
$testTemp = Join-Path $env:TEMP ('agentgraph-domain-green-' + [guid]::NewGuid())
.\.venv\Scripts\python.exe -m pytest tests\test_experiment_state.py tests\test_experiment_actions.py tests\test_experiment_guards.py -q -p no:cacheprovider --basetemp=$testTemp
```

Expected: all selected tests pass.

- [x] **Step 5: Commit domain guards**

```powershell
git add experiment_system/state.py experiment_system/budget.py experiment_system/topology.py tests/test_experiment_guards.py
git commit -m "feat: enforce orchestration budget and topology"
```

## Checkpoint A: Pure domain kernel

- [x] Run all `tests/test_experiment_state.py`, reducer, Action, and guard tests together.
- [x] Run `python -m compileall -q experiment_system` and expect exit 0.
- [x] Confirm no import from `AgentRemote`, `User`, `core`, or `tool_system` exists under `experiment_system`.
- [x] Review Event names, Action statuses, and stable error codes before persistence makes them expensive to change.

---

## Phase 2: Commands, Repository, and Engine

### Task 6: Define strict Commands and deterministic infrastructure inputs

**Files:**
- Create: `experiment_system/contract.py`
- Create: `experiment_system/commands.py`
- Create: `tests/test_experiment_commands.py`
- Modify: `experiment_system/__init__.py`

**Interfaces:**
- Consumes: state, Action proposal, outcome, Event, and artifact-reference value models.
- Produces: `Clock`, `IdFactory`, `ArtifactVerifier`, `CommandBase`, full `Command` union, `CommandResult`, `command_request_hash()`, `action_command_id()`, `strategy_action_id()`, and `transition_command_id()`.

- [x] **Step 1: Write Command validation and identity tests**

Assert:

```python
assert create.expected_revision == 0
assert command_request_hash(create) == command_request_hash(
    CreateAttempt.model_validate(create.model_dump(mode="json"))
)
assert action_command_id(action_id, "started") == action_command_id(
    action_id,
    "started",
)
assert action_command_id(action_id, "started") != action_command_id(
    action_id,
    "outcome",
)
```

Also assert extra fields fail, all non-create Commands require `expected_revision >= 1`, mismatched Attempt ids inside nested payloads fail, and naive datetimes fail.

- [x] **Step 2: Run Command tests and verify RED**

```powershell
$testTemp = Join-Path $env:TEMP ('agentgraph-commands-red-' + [guid]::NewGuid())
.\.venv\Scripts\python.exe -m pytest tests\test_experiment_commands.py -q -p no:cacheprovider --basetemp=$testTemp
```

Expected: collection fails because `experiment_system.commands` does not exist.

- [x] **Step 3: Implement the Command union**

Define:

```text
CreateAttempt
StartAttempt
ApplyStrategyDecision
ReportActionStarted
ReportActionOutcome
FinishAttempt
PauseAttempt
ResumeAttempt
CancelAttempt
SubmitExternalInput
ExpireAttempt
RecoverAttempt
```

All Commands are frozen strict Pydantic models discriminated by `command_type`. `CreateAttempt` carries the complete initial state inputs and enforces revision 0. `ApplyStrategyDecision` carries the new `StrategyStateEnvelope` and an ordered tuple of proposals. `ReportActionOutcome` carries exactly one `ActionOutcome`.

Define `Clock.now_utc()` and `IdFactory.new_uuid()` as Protocols. Production implementations use `datetime.now(timezone.utc)` and `uuid4`; tests inject fixed sequences. Define `ArtifactVerifier.verify(ref)` so the Engine can reject missing or mismatched full-capture references before committing an Event. Derive Action phase Command ids from Action id plus phase, Strategy proposal ids from Attempt/Strategy/ordinal, and runner transition Command ids from Attempt/trigger-revision/operation with fixed UUIDv5 namespaces so retries reuse the same identity.

- [x] **Step 4: Run Command tests and verify GREEN**

Run the Step 2 command. Expected: all Command tests pass.

- [x] **Step 5: Commit Command contracts**

```powershell
git add experiment_system/__init__.py experiment_system/contract.py experiment_system/commands.py tests/test_experiment_commands.py
git commit -m "feat: define orchestration commands"
```

### Task 7: Build the repository contract and in-memory reference adapter

**Files:**
- Create: `experiment_system/store.py`
- Create: `experiment_system/stores/__init__.py`
- Create: `experiment_system/stores/memory.py`
- Create: `tests/test_experiment_store_contract.py`

**Interfaces:**
- Consumes: `CommandResult`, `DomainEvent`, `NormalizedAction`, and `replay_events()`.
- Produces: `AttemptRepository`, `LoadedAttempt`, `CommitRequest`, `CommitResult`, `ClaimedAction`, repository errors, and `InMemoryAttemptRepository`.

- [x] **Step 1: Write a reusable repository contract suite**

The suite receives an async repository factory and asserts:

```python
created = await repository.commit(create_request)
assert created.state.revision == 1

duplicate = await repository.commit(create_request)
assert duplicate == created

loaded = await repository.load(created.state.attempt_id)
assert loaded is not None
assert loaded.state_hash == created.state_hash
assert loaded.events[-1].sequence_no == loaded.state.revision
```

Also assert request-id reuse with a different hash raises `CommandReuseError`, two concurrent commits at one expected revision produce exactly one winner, stale revisions raise `RevisionConflict`, invalid Event batches leave repository state unchanged, missing Attempts return `None`, Event listing honors `after_sequence_no`, and returned values cannot mutate stored state.

- [x] **Step 2: Run repository tests and verify RED**

```powershell
$testTemp = Join-Path $env:TEMP ('agentgraph-store-red-' + [guid]::NewGuid())
.\.venv\Scripts\python.exe -m pytest tests\test_experiment_store_contract.py -q -p no:cacheprovider --basetemp=$testTemp
```

Expected: collection fails because `experiment_system.store` does not exist.

- [x] **Step 3: Define the repository interface**

Use this async Protocol surface:

```python
class AttemptRepository(Protocol):
    def command_scope(self, attempt_id: str) -> AsyncContextManager[None]:
        raise NotImplementedError

    async def load(self, attempt_id: str) -> LoadedAttempt | None:
        raise NotImplementedError

    async def find_command(
        self,
        attempt_id: str,
        command_id: UUID,
    ) -> CommandRecord | None:
        raise NotImplementedError

    async def record_rejection(
        self,
        request: RejectedCommandRequest,
    ) -> CommandResult:
        raise NotImplementedError

    async def commit(self, request: CommitRequest) -> CommitResult:
        raise NotImplementedError

    async def list_events(
        self,
        attempt_id: str,
        *,
        after_sequence_no: int = 0,
    ) -> tuple[DomainEvent, ...]:
        raise NotImplementedError

    async def claim_action(
        self,
        *,
        worker_id: str,
        now_utc: datetime,
        lease_seconds: float,
    ) -> ClaimedAction | None:
        raise NotImplementedError

    async def list_nonterminal_attempt_ids(self) -> tuple[str, ...]:
        raise NotImplementedError
```

`CommitRequest` contains command id/hash, Attempt id, expected revision, ordered Events, outbox Actions, artifact metadata registrations, optional completed delivery Action id, deterministic `CommandResult`, and a checkpoint flag.

- [x] **Step 4: Implement the in-memory adapter atomically**

Guard all structures with one `asyncio.Lock`. Validate idempotency and expected revision before creating local copies. Replay the proposed complete Event stream, calculate canonical SHA-256 state hash, then replace Event/head/command/outbox dictionaries together. Injecting an invalid Event must leave every dictionary unchanged. Return deep validated copies from every method.

- [x] **Step 5: Run the repository contract suite**

Run the Step 2 command. Expected: all repository contract tests pass against `InMemoryAttemptRepository`.

- [x] **Step 6: Commit the repository seam**

```powershell
git add experiment_system/store.py experiment_system/stores/__init__.py experiment_system/stores/memory.py tests/test_experiment_store_contract.py
git commit -m "feat: add attempt repository contract"
```

### Task 8: Implement AttemptEngine command handling

**Files:**
- Create: `experiment_system/engine.py`
- Create: `tests/test_experiment_engine.py`
- Modify: `experiment_system/events.py`
- Modify: `experiment_system/reducer.py`

**Interfaces:**
- Consumes: `AttemptRepository`, strict Commands, injected `Clock`/`IdFactory`/`ArtifactVerifier`, guards, Event models, and reducer.
- Produces: `AttemptEngine.handle(command) -> CommandResult` for create, start, strategy decision, Action start/outcome, and finish.

- [x] **Step 1: Write Engine tests with fixed metadata sources**

Use `InMemoryAttemptRepository`, a fixed clock, and a queue-backed UUID factory. Assert:

```python
created = await engine.handle(create_command)
assert created.accepted is True
assert created.revision == 1
assert created.phase is AttemptPhase.PLANNED

started = await engine.handle(
    StartAttempt(
        command_id=START_COMMAND_ID,
        attempt_id=ATTEMPT_ID,
        expected_revision=1,
    )
)
assert started.revision == 2
assert started.phase is AttemptPhase.RUNNING
```

Apply a Strategy decision with one allowed invocation Action and assert the ordered committed Events are `StrategyDecisionRecorded`, `ActionProposed`, `InvocationRequested`, `BudgetReserved`, and `ActionAccepted`. Apply a forbidden Action and assert `ActionProposed` plus `ActionRejected`, with no invocation, outbox row, or reservation.

Also assert duplicate Commands return the original result without consuming new UUIDs, stale revision conflicts produce no Event, a missing or hash-mismatched full artifact ref rejects before Event construction, a conclusive outcome settles budget once, `OUTCOME_UNKNOWN` keeps its reservation held until reconciliation/abandonment, and `FinishAttempt` rejects while any Action lacks a terminal observation.

- [x] **Step 2: Run Engine tests and verify RED**

```powershell
$testTemp = Join-Path $env:TEMP ('agentgraph-engine-red-' + [guid]::NewGuid())
.\.venv\Scripts\python.exe -m pytest tests\test_experiment_engine.py -q -p no:cacheprovider --basetemp=$testTemp
```

Expected: collection fails because `experiment_system.engine` does not exist.

- [x] **Step 3: Add decision and budget Events**

Implement `StrategyDecisionRecorded`, `BudgetReserved`, `BudgetSettled`, and `BudgetReleased`. Reducer handlers update only the Strategy envelope or budget counters carried by validated Event values. Guard calculations happen before Event creation; reducer replay independently rechecks budget invariants.

- [x] **Step 4: Implement `AttemptEngine.handle()`**

Required flow:

```text
canonical request hash
load Attempt or confirm CreateAttempt absence
return stored duplicate result before allocating metadata
validate expected revision
construct ordered Events and outbox Actions
preflight by reducing Events locally
commit once through AttemptRepository
return committed CommandResult
```

Assign contiguous sequence numbers, logical time equal to sequence number for this kernel, UUIDs from `IdFactory`, and wall time from `Clock`. Catch only known repository/domain exceptions and map them to stable `CommandRejected` results; programming errors continue to raise.

- [x] **Step 5: Run Engine and domain tests**

```powershell
$testTemp = Join-Path $env:TEMP ('agentgraph-engine-green-' + [guid]::NewGuid())
.\.venv\Scripts\python.exe -m pytest tests\test_experiment_engine.py tests\test_experiment_reducer.py tests\test_experiment_actions.py tests\test_experiment_guards.py -q -p no:cacheprovider --basetemp=$testTemp
```

Expected: all selected tests pass.

- [x] **Step 6: Commit the Engine**

```powershell
git add experiment_system/engine.py experiment_system/events.py experiment_system/reducer.py tests/test_experiment_engine.py
git commit -m "feat: add orchestration attempt engine"
```

---

## Phase 3: Durable Storage and Transactional Dispatch

### Task 9: Add atomic content-addressed artifacts

**Files:**
- Create: `experiment_system/artifacts.py`
- Create: `tests/test_experiment_artifacts.py`
- Modify: `experiment_system/contract.py`
- Modify: `experiment_system/engine.py`

**Interfaces:**
- Consumes: `ArtifactRef` and capture policy values.
- Produces: an `ArtifactStore` implementing `put_bytes()`, `put_json()`, `hash_bytes()`, `metadata_only()`, `read_bytes()`, and `ArtifactVerifier.verify()`.

- [x] **Step 1: Write artifact safety and atomicity tests**

Assert identical full-capture content deduplicates to one SHA-256 path, different content receives a different ref, JSON uses canonical UTF-8 bytes and rejects NaN, a caller cannot escape the root, tampering fails hash verification, and a simulated `os.replace` failure removes the same-directory temporary file while preserving an existing artifact. Assert the file is flushed and `fsync` completes before `os.replace`; `hashed` capture computes hash/size without writing bytes; `metadata_only` records only media type/size/classification; non-full refs cannot be read; and logs or safe exceptions never contain the original content.

Representative assertions:

```python
first = store.put_bytes(b"payload", media_type="text/plain", capture_class="full")
second = store.put_bytes(b"payload", media_type="text/plain", capture_class="full")
assert first == second
assert store.read_bytes(first) == b"payload"
store.verify(first)  # Successful verification returns None.
```

- [x] **Step 2: Run artifact tests and verify RED**

```powershell
$testTemp = Join-Path $env:TEMP ('agentgraph-artifacts-red-' + [guid]::NewGuid())
.\.venv\Scripts\python.exe -m pytest tests\test_experiment_artifacts.py -q -p no:cacheprovider --basetemp=$testTemp
```

Expected: collection fails because `experiment_system.artifacts` does not exist.

- [x] **Step 3: Implement atomic artifact writes**

Store full-capture data under `sha256/<first-two-hex>/<full-hash>`. Create a same-directory named temporary file, write and `fsync`, then use `os.replace`. If the final file exists, verify its bytes/hash and discard the temporary file. `put_json()` uses sorted-key, no-NaN, compact UTF-8 JSON. `read_bytes()` resolves only the path derived from the validated digest; it never trusts a caller-supplied filesystem path and rejects non-full refs.

Artifact bytes land before the Event transaction. The transaction registers only verified refs. A failed database commit may leave an immutable unreferenced file for later garbage collection, but a committed Event cannot point to a missing or mismatched full artifact.

- [x] **Step 4: Run artifact tests and verify GREEN**

Run the Step 2 command. Expected: all artifact tests pass.

- [x] **Step 5: Commit artifact persistence**

```powershell
git add experiment_system/artifacts.py experiment_system/contract.py experiment_system/engine.py tests/test_experiment_artifacts.py
git commit -m "feat: add immutable orchestration artifacts"
```

### Task 10: Implement the SQLite WAL repository

**Files:**
- Create: `experiment_system/stores/sqlite.py`
- Create: `tests/test_experiment_sqlite_store.py`
- Modify: `tests/test_experiment_store_contract.py`
- Modify: `experiment_system/store.py`
- Modify: `experiment_system/stores/__init__.py`

**Interfaces:**
- Consumes: the Task 7 repository contract, reducer replay, canonical state bytes, and strict JSON models.
- Produces: `SQLiteAttemptRepository` with schema version 2, transactional v1 -> v2 migration, and the same behavior as the in-memory adapter.

- [x] **Step 1: Parameterize the repository contract suite**

Run every Task 7 contract assertion against both adapters. Add SQLite-specific tests for reopening the database; WAL, foreign keys, and busy timeout; simultaneous stale writers and claimers; canonical Event hashes and previous-hash continuity; derived head/checkpoint repair; Event JSON/hash/sequence/identity corruption that fails closed; immutable checkpoint retention; ordered three-class Artifact registration; Attempt-scoped outbox identity and exact claim ordering; independently revalidated return values; and transaction rollback after injected outbox and command insert failures.

- [x] **Step 2: Run SQLite tests and verify RED**

```powershell
$testTemp = Join-Path $env:TEMP ('agentgraph-sqlite-red-' + [guid]::NewGuid())
.\.venv\Scripts\python.exe -m pytest tests\test_experiment_store_contract.py tests\test_experiment_sqlite_store.py -q -p no:cacheprovider --basetemp=$testTemp
```

Expected: SQLite test collection fails because `SQLiteAttemptRepository` does not exist; the memory contract cases remain green.

- [x] **Step 3: Create schema version 1**

Create these tables and constraints in one migration transaction:

```sql
CREATE TABLE schema_meta (
    singleton INTEGER PRIMARY KEY CHECK (singleton = 1),
    version INTEGER NOT NULL
);
CREATE TABLE events (
    attempt_id TEXT NOT NULL,
    sequence_no INTEGER NOT NULL,
    event_id TEXT NOT NULL UNIQUE,
    event_json TEXT NOT NULL,
    event_hash TEXT NOT NULL,
    previous_event_hash TEXT,
    PRIMARY KEY (attempt_id, sequence_no)
);
CREATE TABLE attempt_heads (
    attempt_id TEXT PRIMARY KEY,
    revision INTEGER NOT NULL,
    state_json TEXT NOT NULL,
    state_hash TEXT NOT NULL,
    schema_version INTEGER NOT NULL,
    latest_event_id TEXT NOT NULL,
    latest_event_hash TEXT NOT NULL
);
CREATE TABLE attempt_checkpoints (
    attempt_id TEXT NOT NULL,
    revision INTEGER NOT NULL,
    state_json TEXT NOT NULL,
    state_hash TEXT NOT NULL,
    schema_version INTEGER NOT NULL,
    latest_event_id TEXT NOT NULL,
    latest_event_hash TEXT NOT NULL,
    PRIMARY KEY (attempt_id, revision)
);
CREATE TABLE commands (
    attempt_id TEXT NOT NULL,
    command_id TEXT NOT NULL,
    request_hash TEXT NOT NULL,
    result_json TEXT NOT NULL,
    committed_revision INTEGER NOT NULL,
    checkpoint_written INTEGER NOT NULL,
    PRIMARY KEY (attempt_id, command_id)
);
CREATE TABLE artifacts (
    artifact_key TEXT PRIMARY KEY,
    content_hash TEXT,
    media_type TEXT NOT NULL,
    byte_size INTEGER NOT NULL,
    relative_path TEXT,
    capture_class TEXT NOT NULL,
    CHECK (
        (capture_class = 'full' AND content_hash IS NOT NULL AND relative_path IS NOT NULL) OR
        (capture_class = 'hashed' AND content_hash IS NOT NULL AND relative_path IS NULL) OR
        (capture_class = 'metadata_only' AND content_hash IS NULL AND relative_path IS NULL)
    )
);
CREATE TABLE attempt_artifacts (
    attempt_id TEXT NOT NULL,
    artifact_key TEXT NOT NULL,
    ordinal INTEGER NOT NULL,
    PRIMARY KEY (attempt_id, artifact_key),
    UNIQUE (attempt_id, ordinal),
    FOREIGN KEY (attempt_id) REFERENCES attempt_heads(attempt_id),
    FOREIGN KEY (artifact_key) REFERENCES artifacts(artifact_key)
);
CREATE TABLE action_outbox (
    attempt_id TEXT NOT NULL,
    action_id TEXT NOT NULL,
    action_json TEXT NOT NULL,
    action_status TEXT NOT NULL,
    recovery_policy TEXT NOT NULL,
    idempotency_key TEXT NOT NULL,
    accepted_sequence_no INTEGER NOT NULL,
    delivery_state TEXT NOT NULL,
    lease_owner TEXT,
    lease_expires_at TEXT,
    PRIMARY KEY (attempt_id, action_id)
);
```

`artifact_key` is the SHA-256 of canonical JSON for the complete `ArtifactRef`.
This represents `full`, `hashed`, and `metadata_only` refs without treating a
content hash or filesystem path as universal identity. `attempt_artifacts`
preserves each Attempt's first-registration order and exact-ref deduplication.
Outbox Action identity remains scoped to an Attempt.

Open connections with `PRAGMA journal_mode=WAL`, `PRAGMA foreign_keys=ON`, and a configured busy timeout. Reject unknown schema versions rather than silently migrating.

- [x] **Step 4: Implement serialized asynchronous access**

Use an adapter-owned `asyncio.Lock` to serialize writes and a separate per-Attempt command gate for `command_scope()` so Engine code may call `commit()` without self-deadlock. Execute every blocking SQLite operation with `asyncio.to_thread`, open and close each connection inside one worker thread, and use `BEGIN IMMEDIATE` for write transactions and claims. Serialize models with canonical JSON and parse Events through the reviewed `DomainEvent` adapter. One commit transaction performs command check, revision check, Event inserts and hash-chain updates, artifact metadata registration, outbox changes, head update, optional checkpoint insert, and command-result insert.

On load, verify canonical Event JSON, hashes, hash-chain continuity, identity, sequence, and reducer semantics across the complete immutable stream before trusting any derived projection. Validate head/checkpoints against the resulting state or Event-prefix state; use the command ledger's checkpoint boundaries to recreate missing rows. If a checkpoint/head fails but Events remain valid, rewrite only those derived rows in a repair transaction and return rebuilt state. Event hash, identity, sequence, or parse corruption raises stable `CorruptEventStream` and never rewrites history. Full replay is intentional in schema version 1 so fail-closed Event validation does not depend on a snapshot; checkpoint-tail acceleration requires a future reviewed, independently revalidatable snapshot encoding.

- [x] **Step 5: Run repository tests and verify GREEN**

Run the Step 2 command. Expected: both adapters pass the shared contract; SQLite-specific cases pass.

- [x] **Step 6: Commit SQLite durability**

```powershell
git add experiment_system/store.py experiment_system/stores/__init__.py experiment_system/stores/sqlite.py tests/test_experiment_store_contract.py tests/test_experiment_sqlite_store.py
git commit -m "feat: persist attempts in sqlite wal"
```

## Checkpoint B: Durable command processing

- [x] Run all experiment-system tests created through Task 10.
- [x] Reopen a temporary SQLite database in a new repository instance and confirm state hash equality.
- [x] Inspect schema and verify no prompt, message, credential, header, raw URL, or traceback column exists.
- [x] Run `git diff --check` and confirm the working tree contains no task-created uncommitted files.

---

## Phase 4: Deterministic Orchestration Vertical Slice

### Task 11: Define Strategy and Backend contracts with deterministic adapters

**Files:**
- Modify: `experiment_system/contract.py`
- Modify: `experiment_system/state.py`
- Modify: `experiment_system/commands.py`
- Modify: `experiment_system/events.py`
- Modify: `experiment_system/engine.py`
- Modify: `experiment_system/__init__.py`
- Create: `experiment_system/backends/__init__.py`
- Create: `experiment_system/backends/deterministic.py`
- Create: `experiment_system/strategies/__init__.py`
- Create: `experiment_system/strategies/single_agent.py`
- Modify: `tests/test_experiment_state.py`
- Modify: `tests/test_experiment_reducer.py`
- Modify: `tests/test_experiment_commands.py`
- Modify: `tests/test_experiment_engine.py`
- Create: `tests/test_experiment_strategies.py`

**Interfaces:**
- Consumes: `StrategyView`, `StrategyStateEnvelope`, committed Events, `ActionProposal`, `NormalizedAction`, and `ActionOutcome`.
- Produces: strict `StrategyDecision`, `StrategyDirective`, `ExecutionContext`, `Strategy` and `Backend` Protocols, `ScriptedBackend`, and `SingleAgentStrategy`.

The platform persists `trigger_sequence_no`, `directive`, `result_ref`, and `error` in `StrategyDecisionRecorded` and its command. These facts remain outside opaque Strategy state so restart completion is authorized by committed Events. `StrategyView` explicitly carries only the narrow Attempt identity fields (`attempt_id`, `strategy_id`, `revision`) needed for deterministic ids.

- [x] **Step 1: Write Strategy and Backend contract tests**

Assert that a Strategy receives a read-only view rather than `AttemptState`, returns a strict decision with an updated envelope and proposed Actions, and cannot include extra platform-owned state. Verify that two equivalent initial views produce byte-identical decisions. Verify that the scripted Backend returns only the outcome registered for the exact Action id and records the Action id plus idempotency key for inspection.

For `SingleAgentStrategy`, assert the first decision proposes exactly one `INVOKE_AGENT` Action, an unrelated Event produces no new Action, success produces an explicit successful-completion directive, and failure produces a safe failed-completion directive without copying Backend details into Strategy state.

- [x] **Step 2: Run Strategy tests and verify RED**

```powershell
$testTemp = Join-Path $env:TEMP ('agentgraph-strategies-red-' + [guid]::NewGuid())
.\.venv\Scripts\python.exe -m pytest tests\test_experiment_strategies.py -q -p no:cacheprovider --basetemp=$testTemp
```

Expected: collection fails because the Backend and Strategy modules do not exist.

- [x] **Step 3: Add the contract models and Protocols**

Define a frozen strict `StrategyDecision` containing the new Strategy envelope, an ordered tuple of Action proposals, an explicit trigger cursor, and one directive from `CONTINUE`, `SUCCEED`, or `FAIL`. `CONTINUE` may carry proposals but no result or error; `SUCCEED` carries no Actions and requires `result_ref`; `FAIL` carries no Actions or result and requires only an `ErrorSummary`. Define `ExecutionContext` with Attempt, Action, invocation, idempotency, deadline, and cancellation values, but no repository, provider client, or global mutable state.

Use this call surface:

```python
class Strategy(Protocol):
    def initialize(self, view: StrategyView) -> StrategyDecision:
        raise NotImplementedError

    def on_event(
        self,
        state: StrategyStateEnvelope,
        event: DomainEvent,
        view: StrategyView,
    ) -> StrategyDecision:
        raise NotImplementedError


class Backend(Protocol):
    async def execute(
        self,
        action: NormalizedAction,
        context: ExecutionContext,
    ) -> ActionOutcome:
        raise NotImplementedError
```

Strategies derive proposal ids from stable Attempt id, Strategy id, and decision ordinal using the Task 6 UUIDv5 helper. They receive no clock or random source.

- [x] **Step 4: Implement the scripted deterministic Backend**

`ScriptedBackend` accepts an immutable mapping from Action id to `ActionOutcome`. It rejects an unknown Action with a stable test-only error code, records calls in order, and performs no filesystem, network, model, tool, clock, or random access. Repeated execution with the same idempotency key returns the same scripted outcome.

- [x] **Step 5: Implement `SingleAgentStrategy`**

Use a small versioned private state value with stages `READY`, `WAITING_FOR_INVOCATION`, and `DONE`. Initialization proposes one invocation. Only the terminal Event for that exact Action advances the stage. Success returns `SUCCEED`; failure, timeout, cancellation, or outcome unknown returns `FAIL` with a normalized safe code. Validate the Strategy envelope hash and schema version on every call.

- [x] **Step 6: Run Strategy tests and verify GREEN**

Run the Step 2 command. Expected: all Strategy and deterministic Backend cases pass.

- [x] **Step 7: Commit deterministic contracts and adapters**

```powershell
git add experiment_system/contract.py experiment_system/backends experiment_system/strategies tests/test_experiment_strategies.py
git commit -m "feat: add deterministic strategy backend contracts"
```

### Task 12: Dispatch committed Actions through the outbox Executor

**Files:**
- Create: `experiment_system/executor.py`
- Create: `tests/test_experiment_executor.py`
- Modify: `experiment_system/engine.py`
- Modify: `experiment_system/store.py`
- Modify: `experiment_system/stores/memory.py`
- Modify: `experiment_system/stores/sqlite.py`
- Modify: `tests/test_experiment_store_contract.py`
- Modify: `tests/test_experiment_sqlite_store.py`
- Modify: `tests/test_experiment_engine.py`

**Interfaces:**
- Consumes: repository leases, `AttemptEngine`, Backend registry, Action phase Command ids, and an injected clock.
- Produces: `ActionExecutor.run_once(worker_id) -> ExecutorStepResult` with no unmanaged background task.

- [x] **Step 1: Write lease and execution-order tests**

Assert all of the following with both repository adapters where applicable:

```text
uncommitted Action -> never claimable
committed ACCEPTED Action -> one worker lease
active lease -> unavailable to a second worker
expired lease -> reclaimable with the same Action id
ActionStarted commit -> visible before Backend.execute begins
Backend outcome commit -> terminal Event and outbox completion are atomic
second run_once after completion -> no Backend call
```

Make the fake Backend reload the Attempt inside `execute()` and assert the Action is already `STARTED`. Make a replay-safe fake return one explicitly classified known failure and assert the Executor records `ActionFailed` with a stable safe code, without persisting exception text or traceback. Assert unexpected programming exceptions and cancellation escape the Executor so recovery sees the durable `STARTED` state.

- [x] **Step 2: Run Executor tests and verify RED**

```powershell
$testTemp = Join-Path $env:TEMP ('agentgraph-executor-red-' + [guid]::NewGuid())
.\.venv\Scripts\python.exe -m pytest tests\test_experiment_executor.py tests\test_experiment_store_contract.py -q -p no:cacheprovider --basetemp=$testTemp
```

Expected: collection fails because `experiment_system.executor` does not exist; existing repository contract cases remain green.

- [x] **Step 3: Complete repository lease semantics**

Claim in stable `(accepted_sequence_no, action_id, attempt_id)` order inside one repository transaction. The Attempt id is the final tie-breaker because Action identity is scoped to an Attempt. A newly accepted Action is eligible only while its Attempt is `RUNNING`; an already `STARTED` Action with an expired lease remains eligible for recovery. A claim changes `PENDING` to `CLAIMED` and writes worker plus expiry. An active claim cannot be stolen. An expired claim can be reclaimed, but its durable Action status remains available to the Executor so recovery can distinguish `ACCEPTED` from `STARTED`.

`ReportActionStarted` updates the outbox Action status but retains the lease. `ReportActionOutcome` appends the terminal Event, updates the head, stores the command result, and removes the completed outbox row in one commit. A rollback preserves the pre-command row and state. A transient delivery-claim fence revalidates the exact worker and lease before the started transition; a late known external result remains replayable without invoking the Backend again.

The repository schema is now version 2. Opening a genuine v1 database performs a transactional `action_outbox` rebuild whose check constraint permits `ACCEPTED`/`STARTED`, preserves every Action JSON, lease, status, and claim-order index, validates foreign keys, and updates metadata last. Unknown versions fail closed and migration failures roll back to the exact v1 schema and rows.

- [x] **Step 4: Implement one bounded Executor step**

`run_once()` performs exactly one claim and returns a typed result such as `IDLE`, `COMPLETED`, or `RECOVERY_REQUIRED`:

1. Claim one committed row.
2. Load the Attempt and validate Action identity and status.
3. If status is `ACCEPTED`, submit idempotent `ReportActionStarted` and verify its commit.
4. Invoke the selected Backend with the committed Action and `ExecutionContext`.
5. Normalize success or an explicitly classified known execution failure into one `ActionOutcome`.
6. Submit idempotent `ReportActionOutcome`, completing the outbox row atomically.

If a reclaimed Action was already `STARTED`, return `RECOVERY_REQUIRED`; Task 16 owns policy-specific behavior. Re-raise `asyncio.CancelledError` and test-only crash signals. Do not loop, sleep, retry, or spawn a task inside the Executor.

- [x] **Step 5: Run Executor and persistence tests**

Run the Step 2 command. Expected: all selected cases pass for memory and SQLite adapters.

- [x] **Step 6: Commit transactional dispatch**

```powershell
git add experiment_system/executor.py experiment_system/store.py experiment_system/stores/memory.py experiment_system/stores/sqlite.py tests/test_experiment_executor.py
git commit -m "feat: dispatch actions through transactional outbox"
```

### Task 13: Drive complete deterministic Attempts

**Files:**
- Create: `experiment_system/runner.py`
- Create: `experiment_system/strategies/static_workflow.py`
- Create: `tests/test_experiment_runner.py`
- Modify: `tests/test_experiment_strategies.py`
- Modify: `experiment_system/actions.py`
- Modify: `experiment_system/events.py`
- Modify: `experiment_system/engine.py`
- Modify: `experiment_system/reducer.py`
- Modify: `experiment_system/strategies/single_agent.py`
- Modify: `tests/test_experiment_engine.py`
- Modify: `tests/test_experiment_reducer.py`

**Interfaces:**
- Consumes: Strategy registry, `AttemptEngine`, `ActionExecutor`, repository Events, and deterministic transition Command ids.
- Produces: `StaticWorkflowStrategy` and `DeterministicAttemptRunner.run_until_blocked()`.

- [x] **Step 1: Write two end-to-end deterministic tests**

Run one Single-Agent Attempt and one two-step Static-Workflow Attempt from `PLANNED` to `SUCCEEDED`. Assert:

- every decision is committed before its Action is claimable;
- the Static Workflow invokes Agent ids in configured order and never runs step 2 before step 1 succeeds;
- terminal outcomes contain only artifact references and safe summaries;
- no Action remains nonterminal and no outbox row remains;
- replaying Events produces canonical bytes and a hash equal to the stored head;
- repeating each scenario in a second fresh repository with the same fixed inputs produces the same Event types, ids, logical times, state bytes, and state hash.

- [x] **Step 2: Run runner tests and verify RED**

```powershell
$testTemp = Join-Path $env:TEMP ('agentgraph-runner-red-' + [guid]::NewGuid())
.\.venv\Scripts\python.exe -m pytest tests\test_experiment_runner.py tests\test_experiment_strategies.py -q -p no:cacheprovider --basetemp=$testTemp
```

Expected: collection fails because `experiment_system.runner` and `StaticWorkflowStrategy` do not exist.

- [x] **Step 3: Implement `StaticWorkflowStrategy`**

Its versioned private state stores the next workflow index and current Action id. Initialization proposes only the first step. A matching success advances and proposes the next step; final success returns `SUCCEED`. A matching failure returns `FAIL`. Duplicate or unrelated Events produce a no-op `CONTINUE` decision with no Actions. Reject duplicate workflow Agent ids only if the workflow definition declares ids unique; preserve intentional repeated roles.

- [x] **Step 4: Implement the bounded runner loop**

`run_until_blocked(attempt_id, max_steps)` repeatedly reloads the committed state and performs one legal operation:

```text
PLANNED -> StartAttempt
RUNNING with an unconsumed Strategy trigger -> ApplyStrategyDecision
RUNNING with dispatchable Action -> ActionExecutor.run_once
committed SUCCEED/FAIL directive with no nonterminal Action -> FinishAttempt
PAUSE_REQUESTED/PAUSED/WAITING_EXTERNAL -> return BLOCKED
terminal phase -> return TERMINAL
no legal progress -> return QUIESCENT
```

Use UUIDv5 transition Command ids derived from Attempt id, trigger Event revision, and operation so a crash between commit and response can repeat the same Command. The platform cursor is persisted in `StrategyDecisionRecorded.trigger_sequence_no` and is replay-validated as the FIFO head of eligible triggers; it is not inferred from opaque Strategy state. Enforce `max_steps` with a stable `StepLimitExceeded` error; do not infer completion from an empty `CONTINUE` proposal list.

- [x] **Step 5: Run deterministic vertical-slice tests**

```powershell
$testTemp = Join-Path $env:TEMP ('agentgraph-vertical-green-' + [guid]::NewGuid())
.\.venv\Scripts\python.exe -m pytest tests\test_experiment_runner.py tests\test_experiment_strategies.py tests\test_experiment_executor.py tests\test_experiment_engine.py -q -p no:cacheprovider --basetemp=$testTemp
```

Expected: all selected tests pass without network access.

- [x] **Step 6: Commit the deterministic vertical slice**

```powershell
git add experiment_system/runner.py experiment_system/strategies/static_workflow.py tests/test_experiment_runner.py tests/test_experiment_strategies.py
git commit -m "feat: run deterministic orchestrated attempts"
```

## Checkpoint C: Deterministic end-to-end proof

- [x] Run every experiment-system test created through Task 13 twice with fresh test directories.
- [x] Compare Event identity sequences, canonical final state bytes, and state hashes across both runs.
- [x] Confirm both final outboxes are empty and every accepted Action has exactly one terminal execution-observation Event.
- [x] Confirm `Agent.py`, `AgentRemote.py`, `User.py`, `core.py`, and `tool_system/` have no diff.

### Phase 4 implementation deviations

- Task 11 expanded beyond its initial file list because the directive/cursor contract crosses `StrategyView`, Command/Event serialization, Engine admission/finish validation, reducer compatibility, and public exports. The cursor remains a platform Event fact; legacy v1 decision payloads are upcast without rewriting their stored JSON or hash.
- Task 12 expanded to `engine.py` and persistence tests to carry transient lease evidence and to prove the versioned SQLite migration. The v1 -> v2 rebuild is the minimal schema change required to persist `STARTED` safely; it does not silently alter schema version 1.
- Task 13 expanded to replay validation, stable-id configuration validation, and legacy multi-decision compatibility. Replay keeps cursor/finality tracking local to the authoritative event walk, while a nonserialized legacy-origin fingerprint prevents compatibility markers from weakening modern validation.
- The global Executor claim order remains `(accepted_sequence_no, action_id, attempt_id)` as required; the runner consumes the existing one-Action `run_once(worker_id)` API and does not introduce an attempt filter or Task 14 recovery policy.

---

## Phase 5: Control Boundaries and Crash Recovery

### Task 14: Implement pause, resume, cancellation, and expiry

**Files:**
- Modify: `experiment_system/commands.py`
- Modify: `experiment_system/events.py`
- Modify: `experiment_system/reducer.py`
- Modify: `experiment_system/engine.py`
- Create: `tests/test_experiment_control.py`

**Interfaces:**
- Consumes: `PauseAttempt`, `ResumeAttempt`, `CancelAttempt`, `ExpireAttempt`, current Action statuses, and checkpoint flags.
- Produces: persisted safe-boundary control transitions without suspending a Python stack.

- [x] **Step 1: Write control-transition tests**

Cover these cases:

```text
RUNNING with no STARTED Action + PauseAttempt
  -> PauseRequested, AttemptPaused, PAUSED checkpoint

RUNNING with STARTED Action + PauseAttempt
  -> PauseRequested, PAUSE_REQUESTED
  -> terminal Action outcome, AttemptPaused, PAUSED checkpoint

PAUSED + ResumeAttempt
  -> AttemptResumed, RUNNING

RUNNING + CancelAttempt
  -> CancelRequested; unstarted Actions cancelled and reservations released
  -> AttemptCancelled after no unsafe Action remains

deadline reached + ExpireAttempt
  -> AttemptTimedOut only through the explicit Command
```

Assert no new accepted Action is claimable while the Attempt is `PAUSE_REQUESTED`, `PAUSED`, `WAITING_EXTERNAL`, or `CANCEL_REQUESTED`. Assert duplicate control Commands are idempotent, terminal Attempts reject control Commands, and a rejected/stale control Command writes no checkpoint.

- [x] **Step 2: Run control tests and verify RED**

```powershell
$testTemp = Join-Path $env:TEMP ('agentgraph-control-red-' + [guid]::NewGuid())
.\.venv\Scripts\python.exe -m pytest tests\test_experiment_control.py -q -p no:cacheprovider --basetemp=$testTemp
```

Expected: tests fail because the Engine does not handle the control Commands.

- [x] **Step 3: Add control Events and reducer rules**

Implement `PauseRequested`, `AttemptPaused`, `AttemptResumed`, `CancelRequested`, and `ActionCancellationRequested`. Reuse the terminal Events from Task 3 and Action terminal Events from Task 4. Reducer rules must enforce the lifecycle in the state design and permit Action outcome Events while an Attempt is waiting for an in-flight Action to reach a safe boundary.

The cancellation-request Event records intent without claiming an external call was cancelled. Only `ActionCancelled` records that observation.

- [x] **Step 4: Handle control Commands in the Engine**

`PauseAttempt` first appends `PauseRequested`, then appends `AttemptPaused` in the same transition only when there is no `STARTED` Action. Otherwise later `ReportActionOutcome` appends `AttemptPaused` once all unsafe Actions are terminal. Accepted but unstarted Actions remain committed and unclaimable until resume.

`CancelAttempt` cancels accepted but unstarted Actions, releases their reservations, and requests cancellation for started Actions. It appends `AttemptCancelled` only when all Actions are terminal. `ExpireAttempt` validates the deadline value supplied by the Command against the committed budget deadline; it never reads wall time inside the reducer.

Mark `PAUSED`, `CANCELLED`, and `TIMED_OUT` transitions as checkpoint boundaries.

- [x] **Step 5: Run control, Engine, Executor, and reducer tests**

```powershell
$testTemp = Join-Path $env:TEMP ('agentgraph-control-green-' + [guid]::NewGuid())
.\.venv\Scripts\python.exe -m pytest tests\test_experiment_control.py tests\test_experiment_engine.py tests\test_experiment_executor.py tests\test_experiment_reducer.py -q -p no:cacheprovider --basetemp=$testTemp
```

Expected: all selected tests pass.

- [x] **Step 6: Commit lifecycle controls**

```powershell
git add experiment_system/commands.py experiment_system/events.py experiment_system/reducer.py experiment_system/engine.py tests/test_experiment_control.py
git commit -m "feat: add durable attempt controls"
```

### Task 15: Add external approval and reconciliation boundaries

**Files:**
- Modify: `experiment_system/actions.py`
- Modify: `experiment_system/state.py`
- Modify: `experiment_system/events.py`
- Modify: `experiment_system/reducer.py`
- Modify: `experiment_system/engine.py`
- Modify: `tests/test_experiment_control.py`

**Interfaces:**
- Consumes: an Action proposal with an external-input requirement and `SubmitExternalInput`.
- Produces: stable approval, rejection, additional-input, and outcome-reconciliation boundaries represented by `ExternalRequest` state and Events.

- [x] **Step 1: Write approval and restart tests**

Propose one approval-gated Action and assert the Engine commits `ActionProposed` plus `ExternalInputRequested`, enters `WAITING_EXTERNAL`, writes a checkpoint, reserves no budget, and creates no outbox row. Close and reopen the SQLite repository, submit the same stable request id at the loaded revision, and assert approval commits before `BudgetReserved` and `ActionAccepted` make the Action claimable.

Also assert rejection produces `ExternalInputRejected` plus `ActionRejected`, returns the Attempt to `RUNNING`, and never executes the Action. Wrong request ids, mismatched Attempt ids, unsupported response kinds, missing artifact references, and stale revisions must leave state unchanged.

- [x] **Step 2: Run external-input tests and verify RED**

Run the Task 14 test command. Expected: newly added cases fail because external requests are not handled.

- [x] **Step 3: Define strict external-input contracts**

Add request kinds `ACTION_APPROVAL`, `ADDITIONAL_INPUT`, and `OUTCOME_RECONCILIATION`. Add response kinds `APPROVE`, `REJECT`, `PROVIDE_INPUT`, `CONFIRM_SUCCEEDED`, `CONFIRM_FAILED`, and `ABANDON`. Requests and responses carry safe codes plus optional immutable artifact references; they never carry raw prompt or response text.

An `ActionProposal` may carry one external-input requirement. `SubmitExternalInput` carries the exact request id, response kind, and optional response artifact/error summary under the normal Command id and expected revision.

- [x] **Step 4: Add external Events and explicit reducer handlers**

Implement `ExternalInputRequested`, `ExternalInputReceived`, `ExternalInputApproved`, `ExternalInputRejected`, and `ExternalInputExpired`. The reducer appends/removes `ExternalRequest` tuple entries by id, enforces one active request per Action, and changes phase only through the explicit Event handler.

For an unknown external Action outcome, `CONFIRM_SUCCEEDED` or `CONFIRM_FAILED` appends `ActionOutcomeReconciled` and settles the held reservation without changing the original `OUTCOME_UNKNOWN` status. `ABANDON` conservatively settles the reservation and interrupts the Attempt.

- [x] **Step 5: Implement Engine transitions and checkpoint rules**

Approval continues from the already committed `ActionProposed`; it must not call the Strategy again or create a second Action id. Rejection returns to `RUNNING`. Additional input becomes an artifact reference in the Strategy-visible Event. Outcome reconciliation returns to `RUNNING` only when the Attempt remains valid; operator abandonment terminates it as `INTERRUPTED`.

Checkpoint every transition into `WAITING_EXTERNAL` and every accepted external response.

- [x] **Step 6: Run control, persistence, and Action tests**

```powershell
$testTemp = Join-Path $env:TEMP ('agentgraph-external-green-' + [guid]::NewGuid())
.\.venv\Scripts\python.exe -m pytest tests\test_experiment_control.py tests\test_experiment_actions.py tests\test_experiment_sqlite_store.py -q -p no:cacheprovider --basetemp=$testTemp
```

Expected: all selected tests pass, including repository reopen cases.

- [x] **Step 7: Commit external-input boundaries**

```powershell
git add experiment_system/actions.py experiment_system/state.py experiment_system/events.py experiment_system/reducer.py experiment_system/engine.py tests/test_experiment_control.py
git commit -m "feat: add durable external input boundaries"
```

### Phase 5A implementation deviations

- Repository commits use the plural `completed_delivery_action_ids: tuple[StableId, ...]` contract so one cancellation transition can delete multiple completed outbox deliveries atomically; duplicate ids are invalid, while single-Action outcomes pass a one-element tuple.
- External-input requirements are decision-level companions on `StrategyDecision`, `ApplyStrategyDecision`, and `StrategyDecisionRecorded`, rather than fields embedded in `ActionProposal` or `ActionState`.
- Empty external requirements are excluded from canonical serialization, and no defaulted fields were added to existing Event or Action state models. This preserves Phase 4 Event JSON, Event hashes, canonical state hashes, legacy fingerprints, and SQLite replay/reopen behavior.
- `ExpireAttempt` rejects with `UNSAFE_IN_FLIGHT_ACTIONS` and writes no Event when a genuinely `STARTED` Action exists; it does not claim an external timeout or failure that has not been observed.

### Task 16: Reconcile interrupted Actions by declared recovery policy

**Files:**
- Create: `experiment_system/recovery.py`
- Create: `tests/test_experiment_recovery.py`
- Modify: `experiment_system/contract.py`
- Modify: `experiment_system/executor.py`
- Modify: `experiment_system/events.py`
- Modify: `experiment_system/reducer.py`
- Modify: `experiment_system/engine.py`

**Interfaces:**
- Consumes: `RecoverAttempt`, nonterminal Attempt enumeration, expired leases, Action recovery policy, and Backend registry.
- Produces: `RecoveryCoordinator.recover_startup()` and policy-exact replay/reconciliation behavior.

- [x] **Step 1: Write the recovery-policy matrix tests**

Persist and reopen one Attempt for each row:

| Last durable status | Policy | Expected behavior |
| --- | --- | --- |
| `ACCEPTED` | any | normal claim with the same Action id |
| `STARTED` | `REPLAY_SAFE` | call `execute` with the same idempotency key |
| `STARTED` | `RECONCILABLE` | call `reconcile`, never `execute` |
| `STARTED` | `NON_REPLAYABLE` | call neither method; append `ActionOutcomeUnknown` |

For inconclusive reconciliation, assert `ActionOutcomeUnknown`, a held reservation, `WAITING_EXTERNAL`, and an `OUTCOME_RECONCILIATION` request. Assert old Events remain byte-identical and recovery only appends new Events. A second recovery pass must not duplicate terminal observations or Backend calls.

- [x] **Step 2: Run recovery tests and verify RED**

```powershell
$testTemp = Join-Path $env:TEMP ('agentgraph-recovery-red-' + [guid]::NewGuid())
.\.venv\Scripts\python.exe -m pytest tests\test_experiment_recovery.py -q -p no:cacheprovider --basetemp=$testTemp
```

Expected: collection fails because `experiment_system.recovery` does not exist.

- [x] **Step 3: Add auditable recovery Events and Backend reconciliation**

Implement `AttemptRecoveryRequested` as an Event that advances revision without changing lifecycle phase. Handle `RecoverAttempt` only for nonterminal Attempts and use a deterministic transition Command id on startup.

Add a separate runtime-checkable `ReconciliableBackend` Protocol:

```python
class ReconciliableBackend(Protocol):
    async def reconcile(
        self,
        action: NormalizedAction,
        context: ExecutionContext,
    ) -> ActionOutcome | None:
        raise NotImplementedError
```

Returning `None` means the external outcome is inconclusive. A Backend that lacks this capability cannot be used for a `RECONCILABLE` Action and yields a safe configuration failure before first execution.

- [x] **Step 4: Implement policy-specific Executor recovery**

Add `recover_once(claimed_action)` as a bounded operation. For `REPLAY_SAFE`, call `execute` with the original idempotency key. For `RECONCILABLE`, call only `reconcile`. For `NON_REPLAYABLE`, synthesize `ActionOutcomeUnknown` without invoking the Backend. Submit every observed result through the same idempotent `ReportActionOutcome` path used by normal execution.

Never convert a programming error, cancellation, or injected crash into a successful recovery result.

- [x] **Step 5: Implement startup coordination**

`recover_startup(max_actions)` lists nonterminal Attempt ids in stable order, verifies each loaded stream/hash, records `AttemptRecoveryRequested`, then processes only expired or unowned outbox leases. It returns a structured summary of recovered, waiting, terminal, and failed Attempt ids. It does not start an infinite worker loop.

- [x] **Step 6: Run recovery, control, and Executor tests**

```powershell
$testTemp = Join-Path $env:TEMP ('agentgraph-recovery-green-' + [guid]::NewGuid())
.\.venv\Scripts\python.exe -m pytest tests\test_experiment_recovery.py tests\test_experiment_control.py tests\test_experiment_executor.py -q -p no:cacheprovider --basetemp=$testTemp
```

Expected: the complete recovery matrix passes.

- [x] **Step 7: Commit recovery coordination**

```powershell
git add experiment_system/recovery.py experiment_system/contract.py experiment_system/executor.py experiment_system/events.py experiment_system/reducer.py experiment_system/engine.py tests/test_experiment_recovery.py
git commit -m "feat: recover actions by declared policy"
```

### Task 17: Prove crash boundaries and replay invariants

**Files:**
- Create: `tests/test_experiment_crash_matrix.py`
- Modify: `experiment_system/contract.py`
- Modify: `experiment_system/stores/sqlite.py`
- Modify: `experiment_system/executor.py`
- Modify: `experiment_system/runner.py`

**Interfaces:**
- Consumes: an injected no-op-by-default fault hook and a real temporary SQLite database.
- Produces: table-driven crash/reopen/recovery proof for every durability boundary in the state design.

- [x] **Step 1: Add failing crash-boundary tests**

Parameterize these exact injection points:

```text
BEFORE_TRANSACTION_COMMIT
AFTER_COMMIT_BEFORE_CLAIM
AFTER_CLAIM_BEFORE_ACTION_STARTED
AFTER_ACTION_STARTED_BEFORE_EXTERNAL_CALL
AFTER_EXTERNAL_CALL_BEFORE_OUTCOME_COMMIT
AFTER_OUTCOME_COMMIT_BEFORE_RESPONSE_DELIVERY
```

At each point, raise a `SimulatedCrash` derived directly from `BaseException`, discard all in-memory Engine/Repository/Executor objects, reopen the same SQLite database, run startup recovery, and drive to a stable boundary.

- [x] **Step 2: Run the crash matrix and verify RED**

```powershell
$testTemp = Join-Path $env:TEMP ('agentgraph-crash-red-' + [guid]::NewGuid())
.\.venv\Scripts\python.exe -m pytest tests\test_experiment_crash_matrix.py -q -p no:cacheprovider --basetemp=$testTemp
```

Expected: tests fail because named fault hooks are not available.

- [x] **Step 3: Add narrow fault-injection hooks**

Define a `FaultPoint` enum and `FaultInjector.hit(point)` Protocol in `contract.py`; production defaults to a no-op. Invoke it only at the six named boundaries. The hook receives no prompt, payload, credentials, exception object, or database connection. Do not add a generic callback inside the reducer.

- [x] **Step 4: Assert post-recovery invariants**

Every matrix row must prove:

- committed Events are contiguous, unique, immutable, and replayable;
- a pre-commit crash exposes none of that transaction's state or outbox changes;
- command retry returns the original committed result;
- no Action executes before a durable `ActionStarted` Event;
- an idempotent replay uses the same Action id and idempotency key;
- a non-replayable post-call uncertainty becomes `OUTCOME_UNKNOWN`, never an automatic second call;
- budget reservation and settlement happen at most once;
- final stored state hash equals a full replay hash.

Add table-driven replay tests that rebuild from every possible checkpoint split in the fixture Event stream and obtain the same final canonical state bytes.

Using only `random.Random` instances constructed with fixed committed seeds, generate legal and deliberately illegal Action batches/Event sequences. Prove budget non-negativity, `reserved + consumed <= limit`, causal parents referencing earlier same-Attempt Events, one terminal observation per accepted Action, and at most one Attempt terminal Event. Persist the seed in each pytest case id so failures reproduce exactly; do not add a property-testing dependency.

- [x] **Step 5: Run the matrix repeatedly**

```powershell
$testTemp1 = Join-Path $env:TEMP ('agentgraph-crash-green-1-' + [guid]::NewGuid())
.\.venv\Scripts\python.exe -m pytest tests\test_experiment_crash_matrix.py -q -p no:cacheprovider --basetemp=$testTemp1
$testTemp2 = Join-Path $env:TEMP ('agentgraph-crash-green-2-' + [guid]::NewGuid())
.\.venv\Scripts\python.exe -m pytest tests\test_experiment_crash_matrix.py -q -p no:cacheprovider --basetemp=$testTemp2
```

Expected: both runs pass with identical parameter ids and no network access.

- [x] **Step 6: Commit crash-injection coverage**

```powershell
git add experiment_system/contract.py experiment_system/stores/sqlite.py experiment_system/executor.py experiment_system/runner.py tests/test_experiment_crash_matrix.py
git commit -m "test: prove orchestration crash recovery"
```

### Phase 5B implementation deviations

- The runtime-checkable reconciliation Protocol is named `ReconcilableBackend`.
  The Task 16 sketch's `ReconciliableBackend` spelling was a typo; correcting it
  changes no serialized Event, Command, state, hash, or fingerprint contract.

### Phase 5B verification evidence

- Task 16 RED failed during collection because `experiment_system.recovery` did
  not exist. The final recovery/control/Executor command passed `184` tests on
  commit `a447b92` plus the Task 17 no-op hooks.
- Task 17 RED failed during collection because the named fault contract did not
  exist. Review-strengthening RED runs then exposed the missing callable guard,
  recovery-call hooks, exact effect accounting, durable-history protection, and
  clock-aware recovery classification before the final GREEN.
- The final crash matrix passed twice with the same `32` cases on commit
  `4ae583f`. It covers all six FaultPoints across `REPLAY_SAFE`, `RECONCILABLE`,
  and `NON_REPLAYABLE`, plus fixed-seed generation, persisted checkpoints,
  sequential recovery crashes, and Checkpoint D demonstrations.
- The recovery classification proof enumerates every unresolved Action. A
  `PROPOSED` approval candidate is explicitly pre-acceptance, has no outbox row,
  and is protected by its approval request; every accepted or started execution
  Action is safely dispatchable or currently leased, while `OUTCOME_UNKNOWN` is
  protected by an `OUTCOME_RECONCILIATION` request.
- `test_checkpoint_d_pause_reopen_status_and_resume` proves pause, object
  disposal, SQLite reopen/status load, and resume on one Attempt.
  `test_checkpoint_d_approval_reopen_response_is_exactly_once` proves durable
  approval wait, reopen, duplicate response submission, one Action acceptance,
  one reservation, and one outbox row.
- The committed-tree offline suite passed with `1135 passed, 1 deselected`; the
  detached verification worktree excluded protected uncommitted user settings.

## Checkpoint D: Controllable and recoverable kernel

- [x] Demonstrate pause, repository close, reopen, status load, and resume on one deterministic Attempt.
- [x] Demonstrate approval wait, repository close, reopen, response submission, and exactly-once Action acceptance.
- [x] Run the full recovery-policy and crash-injection matrices.
- [x] Verify every nonterminal persisted Action is either safely dispatchable, leased, or represented by an external reconciliation request.

---

## Phase 6: Operator Surface and Release Gate

### Task 18: Add a local Attempt control CLI

**Files:**
- Create: `experiment_system/cli.py`
- Create: `experiment_system/__main__.py`
- Create: `tests/test_experiment_cli.py`
- Modify: `experiment_system/__init__.py`

**Interfaces:**
- Consumes: SQLite repository path, strict Command JSON, Engine, recovery coordinator, and redacted `OperatorView` projection.
- Produces: `python -m experiment_system` commands for create, status, pause, resume, cancel, submit-input, and recover.

- [x] **Step 1: Write CLI contract tests**

Invoke `main(argv)` against a temporary database and capture stdout/stderr. Assert:

```text
attempt create --command-json <path>  -> exit 0, attempt id/revision/phase JSON
attempt status <attempt_id>           -> exit 0, redacted OperatorView JSON
attempt pause <attempt_id>            -> exit 0, PAUSED or PAUSE_REQUESTED
attempt resume <attempt_id>           -> exit 0, RUNNING
attempt cancel <attempt_id>           -> exit 0, durable cancellation state
attempt submit-input <attempt_id> --request-id <id> --response-kind <kind>
                                        -> exit 0, committed response revision
recover                               -> exit 0, structured recovery summary
```

Malformed UUIDs/JSON return exit 2, missing Attempts return exit 3, and revision conflicts return exit 4. Assert stdout never includes manifest contents, Strategy private values, payload/result contents, credentials, headers, URLs, exception text, or traceback text.

- [x] **Step 2: Run CLI tests and verify RED**

```powershell
$testTemp = Join-Path $env:TEMP ('agentgraph-cli-red-' + [guid]::NewGuid())
.\.venv\Scripts\python.exe -m pytest tests\test_experiment_cli.py -q -p no:cacheprovider --basetemp=$testTemp
```

Expected: collection fails because `experiment_system.cli` does not exist.

- [x] **Step 3: Implement a dependency-injected CLI**

Use standard-library `argparse`. Keep parsing in `build_parser()`, async work in `run_command(args, dependencies)`, and process adaptation in `main(argv=None) -> int`. Tests inject repositories, clocks, ids, and Backend registries; `__main__.py` wires production defaults.

For mutating convenience commands, load the current Attempt, use its exact revision, and generate a new external Command id before calling the Engine. Print one canonical JSON object per invocation. Render only `OperatorView`, `CommandResult`, or recovery-summary fields.

- [x] **Step 4: Verify module entry-point behavior**

```powershell
.\.venv\Scripts\python.exe -m experiment_system --help
```

Expected: exit 0 and usage listing `attempt` plus `recover`. Do not add a package build backend or a console-script dependency in this plan; a short `agentgraph` executable alias belongs with the later packaging/deployment plan.

- [x] **Step 5: Run CLI and control tests**

```powershell
$testTemp = Join-Path $env:TEMP ('agentgraph-cli-green-' + [guid]::NewGuid())
.\.venv\Scripts\python.exe -m pytest tests\test_experiment_cli.py tests\test_experiment_control.py tests\test_experiment_recovery.py -q -p no:cacheprovider --basetemp=$testTemp
```

Expected: all selected tests pass.

- [x] **Step 6: Commit the operator surface**

```powershell
git add experiment_system/__init__.py experiment_system/__main__.py experiment_system/cli.py tests/test_experiment_cli.py
git commit -m "feat: add local attempt control cli"
```

**Task 18 evidence (2026-07-29):**

- RED: `tests/test_experiment_cli.py` collection failed with
  `ModuleNotFoundError: experiment_system.cli` before production implementation.
- CLI contract suite: `23 passed in 3.03s`.
- CLI/control/recovery gate: `169 passed in 29.19s`.
- `python -m experiment_system --help`: exit `0`, listing `attempt` and `recover`.
- Commit: `a8b61d6 feat: add local attempt control cli`.
- Clean detached worktree, complete offline suite: `1159 passed, 1 deselected in 71.68s`.
- Clean detached worktree, focused store/SQLite/recovery/crash suite:
  `123 passed in 26.36s`.

### Task 19: Document boundaries and pass the release gate

**Files:**
- Modify: `README.md`
- Modify: `HANDOFF.md`
- Modify: `docs/superpowers/plans/2026-07-23-agent-orchestration-state-kernel.md`

**Interfaces:**
- Consumes: the passing state-kernel implementation and its approved designs.
- Produces: concise operator/developer guidance, completed checklist evidence, and an explicit next-plan boundary.

- [x] **Step 1: Add focused documentation**

In README, document:

- the control-plane/data-plane boundary;
- the two architecture design links and their precedence;
- temporary database/artifact layout;
- deterministic CLI create/status/pause/resume/cancel/recover examples;
- the fact that current `Agent.py` behavior and HTTP contracts remain unchanged;
- the explicit absence of LiveLLM integration in this phase.

Update HANDOFF with the implemented module map, test commands, durable invariants, and the next three plans in order: runtime adapter extraction, live Backend/decentralized strategy, then remaining strategies plus evaluation/reporting.

- [x] **Step 2: Run formatting, import-boundary, and placeholder checks**

```powershell
.\.venv\Scripts\python.exe -m compileall -q experiment_system
rg -n "experiment_system" Agent.py AgentRemote.py User.py core.py tool_system
rg -n "TODO|FIXME|pass$" experiment_system tests -g "test_experiment_*.py"
git diff --check
```

Expected: compile exits 0; runtime import search has no matches; placeholder search has no matches; diff check exits 0. Review Protocol methods separately and permit `raise NotImplementedError` only as their explicit interface body.

- [x] **Step 3: Run the complete offline suite**

```powershell
$testTemp = Join-Path $env:TEMP ('agentgraph-full-offline-' + [guid]::NewGuid())
.\.venv\Scripts\python.exe -m pytest -m "not live" -q -p no:cacheprovider --basetemp=$testTemp
```

Expected: all offline tests pass. Do not run live model tests as part of this kernel gate.

- [x] **Step 4: Re-run the highest-risk suites independently**

```powershell
$testTemp = Join-Path $env:TEMP ('agentgraph-release-risk-' + [guid]::NewGuid())
.\.venv\Scripts\python.exe -m pytest tests\test_experiment_store_contract.py tests\test_experiment_sqlite_store.py tests\test_experiment_recovery.py tests\test_experiment_crash_matrix.py -q -p no:cacheprovider --basetemp=$testTemp
```

Expected: repository, corruption repair, recovery policy, and all crash-boundary cases pass together.

**Task 19 technical evidence (2026-07-29):**

- `compileall`: exit `0` in the isolated feature worktree.
- Current-runtime import boundary: no matches (`rg` exit `1`, expected).
- placeholder search: no matches (`rg` exit `1`, expected).
- explicit review found `raise NotImplementedError` only in `Protocol` interface bodies.
- `git diff --check`: exit `0`.
- clean detached complete offline suite: `1159 passed, 1 deselected in 71.68s`.
- clean detached focused store/SQLite/recovery/crash suite: `123 passed in 26.36s`.
- `uv lock --check`: exit `0`, `35` packages resolved, no lockfile change.
- legacy runtime diff and both architecture-spec diffs: empty.
- Repository owner confirmation received on 2026-07-31: exposed credentials were
  rotated and tracked Agent settings were restored to environment-variable references.
  No protected configuration value or diff was inspected, displayed, staged, or committed.

- [x] **Step 5: Verify dependency and worktree scope**

```powershell
uv lock --check
git status --short
git diff -- Agent.py AgentRemote.py User.py core.py tool_system
```

Expected: lockfile is current and legacy runtime diff is empty. Do not stage or inspect the pre-existing user-owned `agents_setting/Agent1.json` and `agents_setting/Agent2.json` changes. If the repository owner has not confirmed credential rotation and restoration to environment-variable references, report the release gate as blocked instead of marking the final checklist complete.

- [x] **Step 6: Commit documentation and completion evidence**

```powershell
git add README.md HANDOFF.md docs/superpowers/plans/2026-07-23-agent-orchestration-state-kernel.md
git commit -m "docs: complete orchestration state kernel"
```

## Final Acceptance Checklist

- [x] `AttemptEngine` is the only logical writer of platform control state.
- [x] Strict Commands and Events reject unknown fields and unsupported schema versions.
- [x] Canonical Event replay reproduces stored state bytes and hash.
- [x] Duplicate Commands return their original result; mismatched reuse and stale revisions append nothing.
- [x] An accepted Action and its outbox row commit atomically before execution.
- [x] Every accepted Action reaches exactly one terminal execution-observation status.
- [x] Batch budget reservation is atomic, bounded, and settled or released once.
- [x] Pause and external input survive repository close/reopen without replaying prior logic.
- [x] Recovery behavior matches every Action's declared policy and never guesses that an external effect failed.
- [x] Non-replayable uncertainty becomes `OUTCOME_UNKNOWN` and requires reconciliation or interruption.
- [x] SQLite head corruption rebuilds from immutable Events; Event corruption fails closed.
- [x] Crash injection passes at all six durability boundaries.
- [x] State, Events, CLI output, and logs contain no raw secrets or captured content.
- [x] Current Agent processes, tool contracts, and HTTP behavior remain unchanged.
- [x] Both architecture designs remain available with explicit scope and precedence.
- [x] All offline tests and focused high-risk suites pass from a clean task-created diff.

## Deferred Follow-Up Plans

Create these only after this plan's final gate passes:

1. `ResponseRunner` extraction plus explicit `ToolExecutionContext`, preserving current complete-Responses replay and tool semantics.
2. `LiveLLMBackend` plus `DecentralizedPeerStrategy`, with live calls gated and recorded as artifacts/Events.
3. Remaining strategy matrix, Blackboard, Experiment/Trial planning, evaluation, metrics, Pareto reports, and Dashboard.

The approved umbrella research design is retained as the parent for those plans. It is moved from `plans/` to `specs/` in Task 1 so it cannot be mistaken for an executable checklist; it is not deleted.
