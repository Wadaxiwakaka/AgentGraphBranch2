from __future__ import annotations

import json
import random
import sqlite3
from datetime import datetime, timedelta
from hashlib import sha256
from pathlib import Path
from uuid import UUID

import pytest

from experiment_system.actions import (
    ActionSucceededOutcome,
    ActionType,
    ExternalInputRequirement,
    ResourceKind,
    ResourceRequest,
)
from experiment_system.commands import (
    ApplyStrategyDecision,
    PauseAttempt,
    ResumeAttempt,
    SubmitExternalInput,
    transition_command_id,
)
from experiment_system.contract import (
    ExecutionContext,
    FaultInjector,
    FaultPoint,
    NoOpFaultInjector,
    StrategyDecision,
    StrategyDirective,
    Uuid4IdFactory,
)
from experiment_system.engine import AttemptEngine
from experiment_system.events import (
    ActionAccepted,
    ActionCancelled,
    ActionFailed,
    ActionOutcomeUnknown,
    ActionStarted,
    ActionSucceeded,
    ActionTimedOut,
    AttemptCancelled,
    AttemptFailed,
    AttemptInterrupted,
    AttemptSucceeded,
    AttemptTimedOut,
    BudgetReserved,
    BudgetReleased,
    BudgetSettled,
    BudgetUncertainSettled,
    StrategyDecisionRecorded,
)
from experiment_system.executor import ActionExecutor
from experiment_system.recovery import RecoveryCoordinator
from experiment_system.reducer import StateTransitionError, apply_event, replay_events
from experiment_system.runner import DeterministicAttemptRunner
from experiment_system.state import (
    ActionStatus,
    AttemptState,
    AttemptPhase,
    ExternalRequestKind,
    ExternalResponseKind,
    RecoveryPolicy,
    canonical_state_bytes,
)
from experiment_system.stores import SQLiteAttemptRepository
from tests.test_experiment_engine import (
    ATTEMPT_ID,
    RecordingArtifactVerifier,
    UTC_NOW,
    _artifact,
    _create_command,
    _decision_command,
    _engine,
    _proposal,
    _start_command,
    _strategy,
)


FAULT_POINTS = (
    FaultPoint.BEFORE_TRANSACTION_COMMIT,
    FaultPoint.AFTER_COMMIT_BEFORE_CLAIM,
    FaultPoint.AFTER_CLAIM_BEFORE_ACTION_STARTED,
    FaultPoint.AFTER_ACTION_STARTED_BEFORE_EXTERNAL_CALL,
    FaultPoint.AFTER_EXTERNAL_CALL_BEFORE_OUTCOME_COMMIT,
    FaultPoint.AFTER_OUTCOME_COMMIT_BEFORE_RESPONSE_DELIVERY,
)
POLICIES = (
    RecoveryPolicy.REPLAY_SAFE,
    RecoveryPolicy.RECONCILABLE,
    RecoveryPolicy.NON_REPLAYABLE,
)


class SimulatedCrash(BaseException):
    pass


class OneShotFaultInjector:
    def __init__(self, target: FaultPoint) -> None:
        self.target = target
        self.armed = False
        self.hits: list[FaultPoint] = []

    def __bool__(self) -> bool:
        return False

    def hit(self, point: FaultPoint) -> None:
        self.hits.append(point)
        if self.armed and point is self.target:
            self.armed = False
            raise SimulatedCrash()


class NonCallableFaultInjector:
    hit = 1


class MutableClock:
    def __init__(self) -> None:
        self.value = UTC_NOW

    def now_utc(self):
        return self.value


class SeededIdFactory:
    def __init__(self, rng: random.Random) -> None:
        self.rng = rng

    def new_uuid(self) -> UUID:
        return UUID(int=self.rng.getrandbits(128))


class PolicyStrategy:
    def __init__(self, policy: RecoveryPolicy) -> None:
        self.policy = policy

    def initialize(self, view) -> StrategyDecision:
        proposal = _proposal().model_copy(
            update={
                "causal_parent_id": str(view.latest_committed_event["event_id"]),
                "recovery_policy": self.policy,
            }
        )
        return StrategyDecision(
            trigger_sequence_no=view.latest_committed_event["sequence_no"],
            strategy=_strategy(1),
            proposals=(proposal,),
            directive=StrategyDirective.CONTINUE,
        )

    def on_event(self, state, event, view) -> StrategyDecision:
        if isinstance(event, ActionSucceeded):
            return StrategyDecision(
                trigger_sequence_no=event.sequence_no,
                strategy=_strategy(2),
                proposals=(),
                directive=StrategyDirective.SUCCEED,
                result_ref=event.outcome.result_ref,
            )
        return StrategyDecision(
            trigger_sequence_no=event.sequence_no,
            strategy=_strategy(2),
            proposals=(),
            directive=StrategyDirective.FAIL,
            error={
                "code": "ACTION_NOT_OBSERVED_SUCCEEDED",
                "retryable": False,
                "safe_message": "The action did not have a successful observation.",
            },
        )


class RejectedProposalStrategy(PolicyStrategy):
    def initialize(self, view) -> StrategyDecision:
        decision = super().initialize(view)
        return decision.model_copy(
            update={
                "proposals": (
                    decision.proposals[0].model_copy(
                        update={"target_ids": ("forbidden-worker",)}
                    ),
                )
            }
        )


class DurableBackend:
    def __init__(self, path: Path, orchestration_path: Path) -> None:
        self.path = path
        self.orchestration_path = orchestration_path
        with sqlite3.connect(path) as connection:
            connection.executescript(
                """
                CREATE TABLE IF NOT EXISTS operations (
                    ordinal INTEGER PRIMARY KEY AUTOINCREMENT,
                    method TEXT NOT NULL,
                    action_id TEXT NOT NULL,
                    idempotency_key TEXT NOT NULL,
                    started_visible INTEGER NOT NULL
                );
                CREATE TABLE IF NOT EXISTS effects (
                    idempotency_key TEXT PRIMARY KEY,
                    action_id TEXT NOT NULL
                );
                """
            )

    def _started_visible(self, action_id: str) -> int:
        with sqlite3.connect(self.orchestration_path) as connection:
            payloads = tuple(
                json.loads(row[0])
                for row in connection.execute(
                    "SELECT event_json FROM events ORDER BY sequence_no"
                )
            )
        return sum(
            payload.get("event_type") == "ACTION_STARTED"
            and payload.get("action_id") == action_id
            for payload in payloads
        )

    async def execute(self, action, context: ExecutionContext):
        with sqlite3.connect(self.path) as connection:
            connection.execute(
                "INSERT INTO operations(method, action_id, idempotency_key, started_visible) VALUES (?, ?, ?, ?)",
                ("execute", action.action_id, action.idempotency_key, self._started_visible(action.action_id)),
            )
            connection.execute(
                "INSERT OR IGNORE INTO effects(idempotency_key, action_id) VALUES (?, ?)",
                (action.idempotency_key, action.action_id),
            )
        return ActionSucceededOutcome(
            status=ActionStatus.SUCCEEDED,
            action_id=action.action_id,
            result_ref=_artifact("crash-matrix-result"),
        )

    async def reconcile(self, action, context: ExecutionContext):
        with sqlite3.connect(self.path) as connection:
            connection.execute(
                "INSERT INTO operations(method, action_id, idempotency_key, started_visible) VALUES (?, ?, ?, ?)",
                ("reconcile", action.action_id, action.idempotency_key, self._started_visible(action.action_id)),
            )
            exists = connection.execute(
                "SELECT 1 FROM effects WHERE idempotency_key = ?",
                (action.idempotency_key,),
            ).fetchone()
        if exists is None:
            return None
        return ActionSucceededOutcome(
            status=ActionStatus.SUCCEEDED,
            action_id=action.action_id,
            result_ref=_artifact("crash-matrix-result"),
        )


def _runtime(
    database_path: Path,
    effects_path: Path,
    policy: RecoveryPolicy,
    *,
    injector: FaultInjector = NoOpFaultInjector(),
    clock: MutableClock | None = None,
    id_factory=None,
):
    actual_clock = clock or MutableClock()
    repository = SQLiteAttemptRepository(database_path, fault_injector=injector)
    engine = AttemptEngine(
        repository=repository,
        clock=actual_clock,
        id_factory=id_factory or Uuid4IdFactory(),
        artifact_verifier=RecordingArtifactVerifier(),
        allowed_edges=frozenset({("engine", "worker-a")}),
    )
    backend = DurableBackend(effects_path, database_path)
    executor = ActionExecutor(
        backends={ActionType.INVOKE_AGENT: backend},
        repository=repository,
        engine=engine,
        clock=actual_clock,
        lease_seconds=10,
        fault_injector=injector,
    )
    runner = DeterministicAttemptRunner(
        repository=repository,
        engine=engine,
        executor=executor,
        strategies={"router": PolicyStrategy(policy)},
        legal_topology={"allowed_edges": (("engine", "worker-a"),)},
        worker_id="matrix-worker",
        fault_injector=injector,
    )
    recovery = RecoveryCoordinator(
        repository=repository,
        engine=engine,
        executor=executor,
        clock=actual_clock,
        worker_id="recovery-worker",
        lease_seconds=10,
    )
    return repository, engine, executor, recovery, runner, backend, actual_clock


def _operations(path: Path) -> tuple[tuple[str, str, str, int], ...]:
    with sqlite3.connect(path) as connection:
        return tuple(
            connection.execute(
                "SELECT method, action_id, idempotency_key, started_visible FROM operations ORDER BY ordinal"
            )
        )


def _database_snapshot(path: Path) -> dict[str, tuple[tuple[object, ...], ...]]:
    tables = (
        "events",
        "attempt_heads",
        "attempt_checkpoints",
        "commands",
        "artifacts",
        "attempt_artifacts",
        "action_outbox",
    )
    with sqlite3.connect(path) as connection:
        return {
            table: tuple(connection.execute(f"SELECT * FROM {table} ORDER BY rowid"))
            for table in tables
        }


def _raw_event_rows(
    path: Path,
) -> tuple[tuple[int, str, str, str, str | None], ...]:
    with sqlite3.connect(path) as connection:
        return tuple(
            connection.execute(
                "SELECT sequence_no, event_id, event_json, event_hash, "
                "previous_event_hash FROM events ORDER BY sequence_no"
            )
        )


async def _assert_recovery_classification(
    repository: SQLiteAttemptRepository,
    database_path: Path,
    *,
    now_utc: datetime,
) -> dict[str, str]:
    loaded = await repository.load(ATTEMPT_ID)
    assert loaded is not None
    with sqlite3.connect(database_path) as connection:
        outbox = {
            row[0]: row[1:]
            for row in connection.execute(
                "SELECT action_id, delivery_state, lease_owner, lease_expires_at "
                "FROM action_outbox"
            )
        }
    classifications: dict[str, str] = {}
    for action in loaded.state.actions:
        outbox_row = outbox.get(action.action_id)
        if action.status in {
            ActionStatus.REJECTED,
            ActionStatus.SUCCEEDED,
            ActionStatus.FAILED,
            ActionStatus.TIMED_OUT,
            ActionStatus.CANCELLED,
        }:
            assert outbox_row is None
            continue

        if action.status is ActionStatus.PROPOSED:
            assert outbox_row is None
            assert any(
                request.request_kind is ExternalRequestKind.ACTION_APPROVAL
                and request.action_id == action.action_id
                for request in loaded.state.pending_external
            )
            classifications[action.action_id] = "preacceptance-approval"
            continue

        if action.status is ActionStatus.OUTCOME_UNKNOWN:
            assert outbox_row is None
            assert any(
                request.request_kind
                is ExternalRequestKind.OUTCOME_RECONCILIATION
                and request.action_id == action.action_id
                for request in loaded.state.pending_external
            )
            classifications[action.action_id] = "outcome-reconciliation"
            continue

        assert action.status in {ActionStatus.ACCEPTED, ActionStatus.STARTED}
        assert outbox_row is not None
        delivery_state, lease_owner, lease_expires_at = outbox_row
        if delivery_state == "PENDING":
            assert lease_owner is None and lease_expires_at is None
            classifications[action.action_id] = "safely-dispatchable"
        else:
            assert delivery_state == "LEASED"
            assert lease_owner is not None and lease_expires_at is not None
            lease_expiry = datetime.fromisoformat(
                str(lease_expires_at).replace("Z", "+00:00")
            )
            classifications[action.action_id] = (
                "currently-leased"
                if lease_expiry > now_utc
                else "safely-dispatchable"
            )
    return classifications


def _expected_methods(point: FaultPoint, policy: RecoveryPolicy) -> tuple[str, ...]:
    if point is FaultPoint.AFTER_ACTION_STARTED_BEFORE_EXTERNAL_CALL:
        return {
            RecoveryPolicy.REPLAY_SAFE: ("execute",),
            RecoveryPolicy.RECONCILABLE: ("reconcile",),
            RecoveryPolicy.NON_REPLAYABLE: (),
        }[policy]
    if point is FaultPoint.AFTER_EXTERNAL_CALL_BEFORE_OUTCOME_COMMIT:
        return {
            RecoveryPolicy.REPLAY_SAFE: ("execute", "execute"),
            RecoveryPolicy.RECONCILABLE: ("execute", "reconcile"),
            RecoveryPolicy.NON_REPLAYABLE: ("execute",),
        }[policy]
    return ("execute",)


def _decision_retry_command(loaded) -> ApplyStrategyDecision:
    event = next(event for event in loaded.events if isinstance(event, StrategyDecisionRecorded))
    return ApplyStrategyDecision(
        schema_version=1,
        command_type="APPLY_STRATEGY_DECISION",
        command_id=transition_command_id(ATTEMPT_ID, event.trigger_sequence_no, "apply-strategy-decision"),
        attempt_id=ATTEMPT_ID,
        expected_revision=event.sequence_no - 1,
        trigger_sequence_no=event.trigger_sequence_no,
        strategy=event.strategy,
        proposals=event.proposals,
        external_requirements=event.external_requirements,
        directive=event.directive,
        result_ref=event.result_ref,
        error=event.error,
    )


def _assert_stream_invariants(
    database_path: Path,
    loaded,
    prefix: tuple[tuple[int, str, str, str, str | None], ...],
) -> None:
    events = loaded.events
    assert tuple(event.sequence_no for event in events) == tuple(range(1, len(events) + 1))
    assert len({event.event_id for event in events}) == len(events)
    with sqlite3.connect(database_path) as connection:
        raw_events = tuple(
            connection.execute(
                "SELECT sequence_no, event_id, event_json, event_hash, "
                "previous_event_hash FROM events ORDER BY sequence_no"
            )
        )
    assert raw_events[: len(prefix)] == prefix
    assert all(
        sha256(event_json.encode("utf-8")).hexdigest() == event_hash
        for _, _, event_json, event_hash, _ in raw_events
    )
    assert all(
        previous_event_hash
        == (None if index == 0 else raw_events[index - 1][3])
        for index, (*_, previous_event_hash) in enumerate(raw_events)
    )
    replayed = replay_events(events)
    assert canonical_state_bytes(replayed) == canonical_state_bytes(loaded.state)
    seen: set[str] = set()
    for event in events:
        parent = event.causal_parent_id
        if parent is not None:
            assert str(parent) in seen
        seen.add(str(event.event_id))
    accepted_ids = {event.action.action_id for event in events if isinstance(event, ActionAccepted)}
    terminal = tuple(
        event
        for event in events
        if isinstance(event, (ActionSucceeded, ActionFailed, ActionTimedOut, ActionCancelled, ActionOutcomeUnknown))
    )
    assert all(sum(event.action_id == action_id for event in terminal) == 1 for action_id in accepted_ids)
    attempt_terminal = tuple(
        event
        for event in events
        if isinstance(event, (AttemptSucceeded, AttemptFailed, AttemptTimedOut, AttemptCancelled, AttemptInterrupted))
    )
    assert len(attempt_terminal) <= 1
    assert sum(isinstance(event, BudgetReserved) for event in events) == 1
    closed_reservations = tuple(
        event
        for event in events
        if isinstance(
            event,
            (BudgetSettled, BudgetReleased, BudgetUncertainSettled),
        )
    )
    assert len(closed_reservations) <= len(accepted_ids)
    assert len(
        {event.reservation.reservation_id for event in closed_reservations}
    ) == len(
        closed_reservations
    )
    assert all(item.reserved >= 0 and item.consumed >= 0 and item.reserved + item.consumed <= item.limit for item in loaded.state.budget.resources)
    with sqlite3.connect(database_path) as connection:
        state_hash = connection.execute(
            "SELECT state_hash FROM attempt_heads WHERE attempt_id = ?", (ATTEMPT_ID,)
        ).fetchone()[0]
    assert state_hash == sha256(canonical_state_bytes(replayed)).hexdigest()


def test_fault_contract_is_exact_and_runtime_checkable() -> None:
    assert tuple(FaultPoint) == FAULT_POINTS
    assert isinstance(NoOpFaultInjector(), FaultInjector)
    assert isinstance(OneShotFaultInjector(FAULT_POINTS[0]), FaultInjector)
    assert SimulatedCrash.__bases__ == (BaseException,)


def test_fault_injector_requires_a_callable_hit(tmp_path: Path) -> None:
    bad = NonCallableFaultInjector()
    with pytest.raises(ValueError):
        SQLiteAttemptRepository(tmp_path / "bad.sqlite3", fault_injector=bad)
    repository, engine, executor, recovery, runner, backend, clock = _runtime(
        tmp_path / "valid.sqlite3",
        tmp_path / "valid-effects.sqlite3",
        RecoveryPolicy.REPLAY_SAFE,
    )
    with pytest.raises(ValueError):
        ActionExecutor(
            backends={ActionType.INVOKE_AGENT: backend},
            repository=repository,
            engine=engine,
            clock=clock,
            lease_seconds=10,
            fault_injector=bad,
        )
    with pytest.raises(ValueError):
        DeterministicAttemptRunner(
            repository=repository,
            engine=engine,
            executor=executor,
            strategies={"router": PolicyStrategy(RecoveryPolicy.REPLAY_SAFE)},
            legal_topology={},
            worker_id="bad-injector",
            fault_injector=bad,
        )


@pytest.mark.asyncio
async def test_after_commit_before_claim_requires_a_claimable_outbox_action(
    tmp_path: Path,
) -> None:
    database_path = tmp_path / "rejected-decision.sqlite3"
    effects_path = tmp_path / "rejected-decision-effects.sqlite3"
    injector = OneShotFaultInjector(FaultPoint.AFTER_COMMIT_BEFORE_CLAIM)
    repository, engine, executor, recovery, runner, backend, clock = _runtime(
        database_path, effects_path, RecoveryPolicy.REPLAY_SAFE, injector=injector
    )
    runner = DeterministicAttemptRunner(
        repository=repository,
        engine=engine,
        executor=executor,
        strategies={"router": RejectedProposalStrategy(RecoveryPolicy.REPLAY_SAFE)},
        legal_topology={},
        worker_id="rejected-decision",
        fault_injector=injector,
    )
    assert (await engine.handle(_create_command())).accepted
    assert (await engine.handle(_start_command())).accepted
    injector.armed = True

    await runner.run_until_blocked(ATTEMPT_ID, max_steps=12)

    loaded = await repository.load(ATTEMPT_ID)
    assert loaded is not None
    assert not any(isinstance(event, ActionAccepted) for event in loaded.events)
    with sqlite3.connect(database_path) as connection:
        assert connection.execute("SELECT COUNT(*) FROM action_outbox").fetchone()[0] == 0
    assert FaultPoint.AFTER_COMMIT_BEFORE_CLAIM not in injector.hits


@pytest.mark.parametrize("policy", POLICIES, ids=lambda value: value.value)
@pytest.mark.parametrize("point", FAULT_POINTS, ids=lambda value: value.value)
@pytest.mark.asyncio
async def test_crash_reopen_recover_matrix(
    tmp_path: Path,
    point: FaultPoint,
    policy: RecoveryPolicy,
) -> None:
    database_path = tmp_path / "attempt.sqlite3"
    effects_path = tmp_path / "effects.sqlite3"
    injector = OneShotFaultInjector(point)
    repository, engine, executor, recovery, runner, backend, clock = _runtime(
        database_path, effects_path, policy, injector=injector
    )
    assert (await engine.handle(_create_command())).accepted
    assert (await engine.handle(_start_command())).accepted
    before = await repository.load(ATTEMPT_ID)
    assert before is not None
    before_rows = _raw_event_rows(database_path)
    before_snapshot = _database_snapshot(database_path)
    injector.armed = True

    with pytest.raises(SimulatedCrash):
        await runner.run_until_blocked(ATTEMPT_ID, max_steps=12)

    crash_rows = _raw_event_rows(database_path)
    crash_classifications = await _assert_recovery_classification(
        repository,
        database_path,
        now_utc=clock.now_utc(),
    )
    expected_crash_category = {
        FaultPoint.BEFORE_TRANSACTION_COMMIT: (),
        FaultPoint.AFTER_COMMIT_BEFORE_CLAIM: ("safely-dispatchable",),
        FaultPoint.AFTER_CLAIM_BEFORE_ACTION_STARTED: ("currently-leased",),
        FaultPoint.AFTER_ACTION_STARTED_BEFORE_EXTERNAL_CALL: (
            "currently-leased",
        ),
        FaultPoint.AFTER_EXTERNAL_CALL_BEFORE_OUTCOME_COMMIT: (
            "currently-leased",
        ),
        FaultPoint.AFTER_OUTCOME_COMMIT_BEFORE_RESPONSE_DELIVERY: (),
    }[point]
    assert tuple(crash_classifications.values()) == expected_crash_category
    del repository, engine, executor, recovery, runner, backend, clock, injector
    if point is FaultPoint.BEFORE_TRANSACTION_COMMIT:
        assert _database_snapshot(database_path) == before_snapshot
        visible = SQLiteAttemptRepository(database_path)
        unchanged = await visible.load(ATTEMPT_ID)
        assert unchanged is not None
        assert _raw_event_rows(database_path) == before_rows
        del visible

    restarted_clock = MutableClock()
    restarted_clock.value = UTC_NOW + timedelta(seconds=30)
    repository, engine, executor, recovery, runner, backend, clock = _runtime(
        database_path, effects_path, policy, clock=restarted_clock
    )
    restart_classifications = await _assert_recovery_classification(
        repository,
        database_path,
        now_utc=clock.now_utc(),
    )
    expected_restart_category = (
        ("safely-dispatchable",)
        if point
        in {
            FaultPoint.AFTER_COMMIT_BEFORE_CLAIM,
            FaultPoint.AFTER_CLAIM_BEFORE_ACTION_STARTED,
            FaultPoint.AFTER_ACTION_STARTED_BEFORE_EXTERNAL_CALL,
            FaultPoint.AFTER_EXTERNAL_CALL_BEFORE_OUTCOME_COMMIT,
        }
        else ()
    )
    assert tuple(restart_classifications.values()) == expected_restart_category
    await recovery.recover_startup(max_actions=2)
    await runner.run_until_blocked(ATTEMPT_ID, max_steps=16)
    loaded = await repository.load(ATTEMPT_ID)
    assert loaded is not None
    retry = await engine.handle(_decision_retry_command(loaded))
    record = next(record for record in loaded.command_records if record.command_id == retry.command_id)
    assert retry == record.result
    operations_before_second_start = _operations(effects_path)
    terminal_before_second_start = sum(
        isinstance(event, (ActionSucceeded, ActionFailed, ActionTimedOut, ActionCancelled, ActionOutcomeUnknown))
        for event in loaded.events
    )
    del repository, engine, executor, recovery, runner, backend, clock

    second_clock = MutableClock()
    second_clock.value = UTC_NOW + timedelta(seconds=60)
    repository, engine, executor, recovery, runner, backend, clock = _runtime(
        database_path, effects_path, policy, clock=second_clock
    )
    await recovery.recover_startup(max_actions=2)
    await runner.run_until_blocked(ATTEMPT_ID, max_steps=16)
    final = await repository.load(ATTEMPT_ID)
    assert final is not None
    final_classifications = await _assert_recovery_classification(
        repository,
        database_path,
        now_utc=clock.now_utc(),
    )
    assert _operations(effects_path) == operations_before_second_start
    assert tuple(method for method, *_ in operations_before_second_start) == _expected_methods(point, policy)
    assert all(started_visible == 1 for *_, started_visible in operations_before_second_start)
    accepted_action = final.state.actions[0]
    assert all(
        action_id == accepted_action.action_id
        and idempotency_key == accepted_action.idempotency_key
        for _, action_id, idempotency_key, _ in operations_before_second_start
    )
    with sqlite3.connect(effects_path) as connection:
        effect_count = connection.execute("SELECT COUNT(*) FROM effects").fetchone()[0]
    assert effect_count == int("execute" in _expected_methods(point, policy))
    terminal_after_second_start = sum(
        isinstance(event, (ActionSucceeded, ActionFailed, ActionTimedOut, ActionCancelled, ActionOutcomeUnknown))
        for event in final.events
    )
    assert terminal_after_second_start == terminal_before_second_start == 1
    should_be_unknown = (
        point
        in {
            FaultPoint.AFTER_ACTION_STARTED_BEFORE_EXTERNAL_CALL,
            FaultPoint.AFTER_EXTERNAL_CALL_BEFORE_OUTCOME_COMMIT,
        }
        and policy is RecoveryPolicy.NON_REPLAYABLE
    ) or (
        point is FaultPoint.AFTER_ACTION_STARTED_BEFORE_EXTERNAL_CALL
        and policy is RecoveryPolicy.RECONCILABLE
    )
    assert final.state.actions[0].status is (
        ActionStatus.OUTCOME_UNKNOWN
        if should_be_unknown
        else ActionStatus.SUCCEEDED
    )
    assert final_classifications == (
        {accepted_action.action_id: "outcome-reconciliation"}
        if should_be_unknown
        else {}
    )
    _assert_stream_invariants(database_path, final, crash_rows)


@pytest.mark.parametrize(
    "policy", (RecoveryPolicy.REPLAY_SAFE, RecoveryPolicy.RECONCILABLE), ids=lambda value: value.value
)
@pytest.mark.parametrize(
    "point",
    (
        FaultPoint.AFTER_ACTION_STARTED_BEFORE_EXTERNAL_CALL,
        FaultPoint.AFTER_EXTERNAL_CALL_BEFORE_OUTCOME_COMMIT,
    ),
    ids=lambda value: value.value,
)
@pytest.mark.asyncio
async def test_recovery_backend_calls_use_the_same_external_fault_boundaries(
    tmp_path: Path,
    point: FaultPoint,
    policy: RecoveryPolicy,
) -> None:
    database_path = tmp_path / "recovery-hooks.sqlite3"
    effects_path = tmp_path / "recovery-hooks-effects.sqlite3"
    initial = OneShotFaultInjector(
        FaultPoint.AFTER_ACTION_STARTED_BEFORE_EXTERNAL_CALL
    )
    repository, engine, executor, recovery, runner, backend, clock = _runtime(
        database_path, effects_path, policy, injector=initial
    )
    assert (await engine.handle(_create_command())).accepted
    assert (await engine.handle(_start_command())).accepted
    initial.armed = True
    with pytest.raises(SimulatedCrash):
        await runner.run_until_blocked(ATTEMPT_ID, max_steps=12)
    del repository, engine, executor, recovery, runner, backend, clock, initial

    injector = OneShotFaultInjector(point)
    injector.armed = True
    restarted_clock = MutableClock()
    restarted_clock.value = UTC_NOW + timedelta(seconds=30)
    repository, engine, executor, recovery, runner, backend, clock = _runtime(
        database_path,
        effects_path,
        policy,
        injector=injector,
        clock=restarted_clock,
    )
    with pytest.raises(SimulatedCrash):
        await recovery.recover_startup(max_actions=1)
    methods = tuple(method for method, *_ in _operations(effects_path))
    if point is FaultPoint.AFTER_ACTION_STARTED_BEFORE_EXTERNAL_CALL:
        assert methods == ()
    else:
        assert methods == (
            "execute" if policy is RecoveryPolicy.REPLAY_SAFE else "reconcile",
        )
    del repository, engine, executor, recovery, runner, backend, clock, injector

    final_clock = MutableClock()
    final_clock.value = UTC_NOW + timedelta(seconds=60)
    repository, engine, executor, recovery, runner, backend, clock = _runtime(
        database_path,
        effects_path,
        policy,
        clock=final_clock,
    )
    await recovery.recover_startup(max_actions=1)
    await runner.run_until_blocked(ATTEMPT_ID, max_steps=16)
    final = await repository.load(ATTEMPT_ID)
    assert final is not None
    final_classifications = await _assert_recovery_classification(
        repository,
        database_path,
        now_utc=clock.now_utc(),
    )
    final_methods = tuple(method for method, *_ in _operations(effects_path))
    expected_method = (
        "execute" if policy is RecoveryPolicy.REPLAY_SAFE else "reconcile"
    )
    expected_calls = (
        1
        if point is FaultPoint.AFTER_ACTION_STARTED_BEFORE_EXTERNAL_CALL
        else 2
    )
    assert final_methods == (expected_method,) * expected_calls
    with sqlite3.connect(effects_path) as connection:
        effect_count = connection.execute("SELECT COUNT(*) FROM effects").fetchone()[0]
    assert effect_count == int(policy is RecoveryPolicy.REPLAY_SAFE)
    terminal = tuple(
        event
        for event in final.events
        if isinstance(
            event,
            (
                ActionSucceeded,
                ActionFailed,
                ActionTimedOut,
                ActionCancelled,
                ActionOutcomeUnknown,
            ),
        )
    )
    assert len(terminal) == 1
    assert terminal[0].action_id == final.state.actions[0].action_id
    assert final_classifications == (
        {}
        if policy is RecoveryPolicy.REPLAY_SAFE
        else {terminal[0].action_id: "outcome-reconciliation"}
    )


@pytest.mark.parametrize("seed", (1729, 2718, 31415), ids=lambda seed: f"seed-{seed}")
@pytest.mark.asyncio
async def test_fixed_seed_legal_and_illegal_sequences_preserve_invariants(
    tmp_path: Path, seed: int
) -> None:
    rng = random.Random(seed)
    database_path = tmp_path / f"generated-{seed}.sqlite3"
    effects_path = tmp_path / f"generated-effects-{seed}.sqlite3"
    repository, engine, executor, recovery, runner, backend, clock = _runtime(
        database_path,
        effects_path,
        RecoveryPolicy.REPLAY_SAFE,
        id_factory=SeededIdFactory(rng),
    )
    assert (await engine.handle(_create_command())).accepted
    assert (await engine.handle(_start_command())).accepted
    legal_proposals = tuple(
        _proposal(ordinal=ordinal, batch_id=f"legal-{seed}").model_copy(
            update={
                "resource_requests": (
                    ResourceRequest(
                        resource=ResourceKind.MODEL_CALLS,
                        amount=rng.randint(1, 2),
                    ),
                )
            }
        )
        for ordinal in range(2)
    )
    legal = _decision_command(legal_proposals[0]).model_copy(
        update={"proposals": legal_proposals}
    )
    assert (await engine.handle(legal)).accepted
    assert (await executor.run_once("generated-0")).status == "COMPLETED"
    assert (await executor.run_once("generated-1")).status == "COMPLETED"
    loaded = await repository.load(ATTEMPT_ID)
    assert loaded is not None
    _assert_stream_invariants(database_path, loaded, ())
    terminal = next(event for event in loaded.events if isinstance(event, ActionSucceeded))
    invalid_parent = terminal.model_copy(
        update={
            "event_id": UUID(int=seed),
            "sequence_no": loaded.state.revision + 1,
            "causal_parent_id": UUID(int=seed + 1),
        }
    )
    with pytest.raises(StateTransitionError):
        replay_events(loaded.events + (invalid_parent,))
    duplicate_terminal = terminal.model_copy(
        update={
            "event_id": UUID(int=seed + 2),
            "sequence_no": loaded.state.revision + 1,
            "causal_parent_id": loaded.events[-1].event_id,
        }
    )
    with pytest.raises(StateTransitionError):
        apply_event(loaded.state, duplicate_terminal)

    base = _proposal(batch_id=f"invalid-{seed}")
    too_large = _proposal(ordinal=1, batch_id=f"invalid-{seed}").model_copy(
        update={
            "resource_requests": (
                ResourceRequest(resource=ResourceKind.MODEL_CALLS, amount=10),
            )
        }
    )
    illegal_batches = (
        ("duplicate", (base, base), "DUPLICATE_ACTION_ID"),
        (
            "over-budget",
            (base, too_large),
            None,
        ),
        (
            "mixed-batch",
            (
            base,
            _proposal(ordinal=1, batch_id=f"different-{seed}"),
            ),
            "INVALID_ACTION_BATCH",
        ),
    )
    for ordinal, (kind, proposals, error_code) in enumerate(
        illegal_batches, start=1
    ):
        invalid_path = tmp_path / f"invalid-{seed}-{kind}.sqlite3"
        invalid_repository = SQLiteAttemptRepository(invalid_path)
        invalid_engine, *_ = _engine(repository=invalid_repository)
        assert (await invalid_engine.handle(_create_command())).accepted
        assert (await invalid_engine.handle(_start_command())).accepted
        command = _decision_command(proposals[0]).model_copy(
            update={
                "command_id": UUID(int=seed * 10 + ordinal),
                "proposals": proposals,
            }
        )
        result = await invalid_engine.handle(command)
        if error_code is None:
            assert result.accepted
        else:
            assert not result.accepted
            assert result.error is not None and result.error.code == error_code
        invalid_loaded = await invalid_repository.load(ATTEMPT_ID)
        assert invalid_loaded is not None
        if error_code is None:
            assert all(
                action.status is ActionStatus.REJECTED
                for action in invalid_loaded.state.actions
            )
            assert all(
                item.reserved >= 0
                and item.consumed >= 0
                and item.reserved + item.consumed <= item.limit
                for item in invalid_loaded.state.budget.resources
            )
        else:
            assert invalid_loaded.state.actions == ()
        with sqlite3.connect(invalid_path) as connection:
            assert connection.execute("SELECT COUNT(*) FROM action_outbox").fetchone()[0] == 0


@pytest.mark.asyncio
async def test_checkpoint_d_pause_reopen_status_and_resume(tmp_path: Path) -> None:
    database_path = tmp_path / "pause-reopen.sqlite3"
    effects_path = tmp_path / "pause-reopen-effects.sqlite3"
    repository, engine, executor, recovery, runner, backend, clock = _runtime(
        database_path, effects_path, RecoveryPolicy.REPLAY_SAFE
    )
    assert (await engine.handle(_create_command())).accepted
    assert (await engine.handle(_start_command())).accepted
    paused = await engine.handle(
        PauseAttempt(
            schema_version=1,
            command_type="PAUSE_ATTEMPT",
            command_id=UUID("30000000-0000-0000-0000-000000000017"),
            attempt_id=ATTEMPT_ID,
            expected_revision=2,
        )
    )
    assert paused.accepted and paused.phase is AttemptPhase.PAUSED
    del repository, engine, executor, recovery, runner, backend, clock

    repository, engine, executor, recovery, runner, backend, clock = _runtime(
        database_path, effects_path, RecoveryPolicy.REPLAY_SAFE
    )
    restored = await repository.load(ATTEMPT_ID)
    assert restored is not None
    assert restored.state.phase is AttemptPhase.PAUSED
    resumed = await engine.handle(
        ResumeAttempt(
            schema_version=1,
            command_type="RESUME_ATTEMPT",
            command_id=UUID("40000000-0000-0000-0000-000000000017"),
            attempt_id=ATTEMPT_ID,
            expected_revision=restored.state.revision,
        )
    )
    assert resumed.accepted and resumed.phase is AttemptPhase.RUNNING


@pytest.mark.asyncio
async def test_recovery_classification_uses_supplied_time_for_expired_lease(
    tmp_path: Path,
) -> None:
    database_path = tmp_path / "expired-lease.sqlite3"
    effects_path = tmp_path / "expired-lease-effects.sqlite3"
    repository, engine, executor, recovery, runner, backend, clock = _runtime(
        database_path, effects_path, RecoveryPolicy.REPLAY_SAFE
    )
    assert (await engine.handle(_create_command())).accepted
    assert (await engine.handle(_start_command())).accepted
    assert (await engine.handle(_decision_command(_proposal()))).accepted
    pending = await repository.load(ATTEMPT_ID)
    assert pending is not None
    pending_action = pending.state.actions[0]
    pending_classifications = await _assert_recovery_classification(
        repository,
        database_path,
        now_utc=UTC_NOW,
    )
    assert pending_classifications == {
        pending_action.action_id: "safely-dispatchable"
    }
    claim = await repository.claim_action(
        worker_id="expired-classification",
        now_utc=UTC_NOW,
        lease_seconds=10,
    )
    assert claim is not None

    active_classifications = await _assert_recovery_classification(
        repository,
        database_path,
        now_utc=UTC_NOW,
    )
    assert active_classifications == {
        claim.action.action_id: "currently-leased"
    }

    classifications = await _assert_recovery_classification(
        repository,
        database_path,
        now_utc=UTC_NOW + timedelta(seconds=30),
    )

    assert classifications == {
        claim.action.action_id: "safely-dispatchable"
    }


@pytest.mark.asyncio
async def test_checkpoint_d_approval_reopen_response_is_exactly_once(
    tmp_path: Path,
) -> None:
    database_path = tmp_path / "approval-reopen.sqlite3"
    effects_path = tmp_path / "approval-reopen-effects.sqlite3"
    repository, engine, executor, recovery, runner, backend, clock = _runtime(
        database_path, effects_path, RecoveryPolicy.REPLAY_SAFE
    )
    assert (await engine.handle(_create_command())).accepted
    assert (await engine.handle(_start_command())).accepted
    proposal = _proposal().model_copy(
        update={"recovery_policy": RecoveryPolicy.REPLAY_SAFE}
    )
    request_id = "checkpoint-d-approval"
    decision = _decision_command(proposal).model_copy(
        update={
            "external_requirements": (
                ExternalInputRequirement(
                    request_id=request_id,
                    request_kind=ExternalRequestKind.ACTION_APPROVAL,
                    action_id=proposal.action_id,
                ),
            )
        }
    )
    waiting_result = await engine.handle(decision)
    assert waiting_result.accepted
    waiting = await repository.load(ATTEMPT_ID)
    assert waiting is not None
    assert waiting.state.phase is AttemptPhase.WAITING_EXTERNAL
    classifications = await _assert_recovery_classification(
        repository,
        database_path,
        now_utc=clock.now_utc(),
    )
    assert classifications == {
        proposal.action_id: "preacceptance-approval"
    }
    del repository, engine, executor, recovery, runner, backend, clock

    repository, engine, executor, recovery, runner, backend, clock = _runtime(
        database_path, effects_path, RecoveryPolicy.REPLAY_SAFE
    )
    restored = await repository.load(ATTEMPT_ID)
    assert restored is not None
    response = SubmitExternalInput(
        schema_version=1,
        command_type="SUBMIT_EXTERNAL_INPUT",
        command_id=UUID("80000000-0000-0000-0000-000000000017"),
        attempt_id=ATTEMPT_ID,
        expected_revision=restored.state.revision,
        request_id=request_id,
        response_kind=ExternalResponseKind.APPROVE,
    )
    first = await engine.handle(response)
    second = await engine.handle(response)
    assert first == second
    assert first.accepted
    accepted = await repository.load(ATTEMPT_ID)
    assert accepted is not None
    assert sum(
        isinstance(event, ActionAccepted)
        and event.action.action_id == proposal.action_id
        for event in accepted.events
    ) == 1
    reservations = tuple(
        reservation
        for event in accepted.events
        if isinstance(event, BudgetReserved)
        for reservation in event.reservations
        if reservation.action_id == proposal.action_id
    )
    assert len(reservations) == 1
    with sqlite3.connect(database_path) as connection:
        outbox = tuple(
            connection.execute(
                "SELECT action_id FROM action_outbox WHERE action_id = ?",
                (proposal.action_id,),
            )
        )
    assert outbox == ((proposal.action_id,),)


@pytest.mark.asyncio
async def test_every_checkpoint_split_rebuilds_identical_canonical_state(tmp_path: Path) -> None:
    repository, engine, executor, recovery, runner, backend, clock = _runtime(
        tmp_path / "checkpoint.sqlite3",
        tmp_path / "checkpoint-effects.sqlite3",
        RecoveryPolicy.REPLAY_SAFE,
    )
    assert (await engine.handle(_create_command())).accepted
    assert (await engine.handle(_start_command())).accepted
    await runner.run_until_blocked(ATTEMPT_ID, max_steps=16)
    loaded = await repository.load(ATTEMPT_ID)
    assert loaded is not None
    with sqlite3.connect(tmp_path / "checkpoint.sqlite3") as connection:
        checkpoint_rows = tuple(
            connection.execute(
                "SELECT revision, state_json, state_hash "
                "FROM attempt_checkpoints WHERE attempt_id = ? ORDER BY revision",
                (ATTEMPT_ID,),
            )
        )
    assert checkpoint_rows
    assert tuple(row[0] for row in checkpoint_rows) == loaded.checkpoint_revisions
    for revision, state_json, state_hash in checkpoint_rows:
        checkpoint_state = replay_events(loaded.events[:revision])
        assert state_json.encode("utf-8") == canonical_state_bytes(checkpoint_state)
        assert state_hash == sha256(state_json.encode("utf-8")).hexdigest()
    expected = canonical_state_bytes(loaded.state)
    for split in range(1, len(loaded.events) + 1):
        state = None
        for event in loaded.events[:split]:
            state = apply_event(state, event)
        assert state is not None
        state = AttemptState.model_validate_json(state.model_dump_json())
        for event in loaded.events[split:]:
            state = apply_event(state, event)
        assert canonical_state_bytes(state) == expected
