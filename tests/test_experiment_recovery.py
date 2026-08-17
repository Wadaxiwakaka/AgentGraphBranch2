from __future__ import annotations

import asyncio
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest

from experiment_system.actions import ActionSucceededOutcome, ActionType
from experiment_system.commands import ReportActionStarted, action_command_id
from experiment_system.recovery import RecoveryCoordinator, RecoverySummary
from experiment_system.contract import (
    ExecutionContext,
    ReconcilableBackend,
    Uuid4IdFactory,
)
from experiment_system.engine import AttemptEngine
from experiment_system.executor import (
    ActionExecutor,
    ExecutorConfigurationError,
    ExecutorStepStatus,
)
from experiment_system.state import (
    ActionStatus,
    AttemptPhase,
    ExternalRequestKind,
    RecoveryPolicy,
)
from experiment_system.store import AttemptRepository, OutboxActionStatus
from experiment_system.stores import InMemoryAttemptRepository, SQLiteAttemptRepository
from tests.test_experiment_engine import (
    ATTEMPT_ID,
    RecordingArtifactVerifier,
    FixedClock,
    _artifact,
    _create_command,
    _decision_command,
    _engine,
    _proposal,
    _start_command,
)
from tests.test_experiment_executor import MutableClock, NOW


class RecoveryBackend:
    def __init__(self, *, reconcile_result=NotImplemented) -> None:
        self.execute_calls: list[tuple[str, ExecutionContext]] = []
        self.reconcile_calls: list[tuple[str, ExecutionContext]] = []
        self.reconcile_result = reconcile_result

    async def execute(self, action, context: ExecutionContext):
        self.execute_calls.append((action.action_id, context))
        return _success(action.action_id)

    async def reconcile(self, action, context: ExecutionContext):
        self.reconcile_calls.append((action.action_id, context))
        if self.reconcile_result is NotImplemented:
            return _success(action.action_id)
        return self.reconcile_result


class ExecuteOnlyBackend:
    def __init__(self) -> None:
        self.execute_calls: list[str] = []

    async def execute(self, action, context: ExecutionContext):
        self.execute_calls.append(action.action_id)
        return _success(action.action_id)


class ExplodingRecoveryBackend(RecoveryBackend):
    def __init__(self, failure: BaseException) -> None:
        super().__init__()
        self.failure = failure

    async def execute(self, action, context: ExecutionContext):
        self.execute_calls.append((action.action_id, context))
        raise self.failure

    async def reconcile(self, action, context: ExecutionContext):
        self.reconcile_calls.append((action.action_id, context))
        raise self.failure


class CountingRepository:
    def __init__(self, delegate: AttemptRepository) -> None:
        self.delegate = delegate
        self.claim_calls = 0
        self.confirm_calls = 0

    def __getattr__(self, name: str):
        return getattr(self.delegate, name)

    async def claim_action(self, **kwargs):
        self.claim_calls += 1
        return await self.delegate.claim_action(**kwargs)

    async def confirm_action_claim(self, **kwargs):
        self.confirm_calls += 1
        return await self.delegate.confirm_action_claim(**kwargs)


class RejectingConfirmationRepository(CountingRepository):
    async def confirm_action_claim(self, **kwargs):
        self.confirm_calls += 1
        return False


def _success(action_id: str) -> ActionSucceededOutcome:
    return ActionSucceededOutcome(
        status=ActionStatus.SUCCEEDED,
        action_id=action_id,
        result_ref=_artifact(f"recovery-result-{action_id}"),
    )


async def _persist_action(
    repository: AttemptRepository,
    policy: RecoveryPolicy,
    *,
    started: bool,
    reclaim_started: bool = True,
):
    engine, _, _, _, _ = _engine(repository=repository)
    assert (await engine.handle(_create_command())).accepted is True
    assert (await engine.handle(_start_command())).accepted is True
    proposal = _proposal().model_copy(update={"recovery_policy": policy})
    assert (await engine.handle(_decision_command(proposal))).accepted is True
    accepted = await repository.load(ATTEMPT_ID)
    assert accepted is not None
    action = accepted.state.actions[0]
    claim = await repository.claim_action(
        worker_id="pre-crash-worker",
        now_utc=NOW,
        lease_seconds=10,
    )
    assert claim is not None
    if not started:
        return engine, action, claim
    result = await engine.handle(
        ReportActionStarted(
            schema_version=1,
            command_type="REPORT_ACTION_STARTED",
            command_id=action_command_id(action.action_id, "started"),
            attempt_id=ATTEMPT_ID,
            expected_revision=accepted.state.revision,
            action_id=action.action_id,
        ),
        delivery_claim=claim.delivery_claim(),
    )
    assert result.accepted is True
    if not reclaim_started:
        return engine, action, claim
    recovered_claim = await repository.claim_action(
        worker_id="recovery-worker",
        now_utc=NOW + timedelta(seconds=10),
        lease_seconds=10,
    )
    assert recovered_claim is not None
    assert recovered_claim.action_status is OutboxActionStatus.STARTED
    return engine, action, recovered_claim


async def _persist_started_actions(repository: AttemptRepository, proposals):
    engine, _, _, _, _ = _engine(repository=repository)
    assert (await engine.handle(_create_command())).accepted is True
    assert (await engine.handle(_start_command())).accepted is True
    decision = _decision_command(proposals[0]).model_copy(
        update={"proposals": proposals}
    )
    assert (await engine.handle(decision)).accepted is True

    for ordinal in range(len(proposals)):
        loaded = await repository.load(ATTEMPT_ID)
        assert loaded is not None
        claim = await repository.claim_action(
            worker_id=f"pre-crash-worker-{ordinal}",
            now_utc=NOW,
            lease_seconds=10,
        )
        assert claim is not None
        started = await engine.handle(
            ReportActionStarted(
                schema_version=1,
                command_type="REPORT_ACTION_STARTED",
                command_id=action_command_id(claim.action.action_id, "started"),
                attempt_id=ATTEMPT_ID,
                expected_revision=loaded.state.revision,
                action_id=claim.action.action_id,
            ),
            delivery_claim=claim.delivery_claim(),
        )
        assert started.accepted is True

    loaded = await repository.load(ATTEMPT_ID)
    assert loaded is not None
    assert all(action.status is ActionStatus.STARTED for action in loaded.state.actions)
    return engine, loaded.state.actions


def _reopened_engine(
    repository: AttemptRepository,
    *,
    clock=None,
) -> AttemptEngine:
    return AttemptEngine(
        repository=repository,
        clock=clock or FixedClock(),
        id_factory=Uuid4IdFactory(),
        artifact_verifier=RecordingArtifactVerifier(),
        allowed_edges=frozenset({("engine", "worker-a")}),
    )


def _executor(
    repository: AttemptRepository,
    engine,
    backend,
    *,
    clock: MutableClock,
) -> ActionExecutor:
    return ActionExecutor(
        backends={ActionType.INVOKE_AGENT: backend},
        repository=repository,
        engine=engine,
        clock=clock,
        lease_seconds=10,
    )


def test_reconcilable_backend_is_correctly_spelled_and_runtime_checkable() -> None:
    assert isinstance(RecoveryBackend(), ReconcilableBackend)
    assert not isinstance(ExecuteOnlyBackend(), ReconcilableBackend)


def test_recovery_summary_categories_are_mutually_exclusive() -> None:
    with pytest.raises(ValueError):
        RecoverySummary(
            recovered_attempt_ids=(ATTEMPT_ID,),
            failed_attempt_ids=(ATTEMPT_ID,),
        )


@pytest.mark.asyncio
async def test_accepted_recovery_uses_existing_claim_without_reclaiming(
    tmp_path: Path,
) -> None:
    path = tmp_path / "matrix-accepted.sqlite3"
    before_restart = SQLiteAttemptRepository(path)
    _, action, _ = await _persist_action(
        before_restart, RecoveryPolicy.NON_REPLAYABLE, started=False
    )
    base = SQLiteAttemptRepository(path)
    engine = _reopened_engine(base)
    claim = await base.claim_action(
        worker_id="accepted-recovery",
        now_utc=NOW + timedelta(seconds=10),
        lease_seconds=10,
    )
    assert claim is not None
    assert claim.action_status is OutboxActionStatus.ACCEPTED
    repository = CountingRepository(base)
    backend = RecoveryBackend()
    executor = _executor(
        repository,
        engine,
        backend,
        clock=MutableClock(NOW + timedelta(seconds=10)),
    )

    result = await executor.recover_once(claim)

    assert result.status is ExecutorStepStatus.COMPLETED
    assert repository.claim_calls == 0
    assert repository.confirm_calls == 1
    assert backend.execute_calls[0][0] == action.action_id
    loaded = await base.load(ATTEMPT_ID)
    assert loaded is not None
    assert loaded.state.actions[0].status is ActionStatus.SUCCEEDED


@pytest.mark.parametrize(
    ("policy", "expected_method"),
    (
        (RecoveryPolicy.REPLAY_SAFE, "execute"),
        (RecoveryPolicy.RECONCILABLE, "reconcile"),
        (RecoveryPolicy.NON_REPLAYABLE, "neither"),
    ),
)
@pytest.mark.asyncio
async def test_started_recovery_obeys_declared_policy(
    policy: RecoveryPolicy,
    expected_method: str,
    tmp_path: Path,
) -> None:
    path = tmp_path / f"matrix-{policy.value}.sqlite3"
    before_restart = SQLiteAttemptRepository(path)
    _, action, _ = await _persist_action(
        before_restart, policy, started=True, reclaim_started=False
    )
    repository = SQLiteAttemptRepository(path)
    engine = _reopened_engine(repository)
    claim = await repository.claim_action(
        worker_id="matrix-recovery",
        now_utc=NOW + timedelta(seconds=10),
        lease_seconds=10,
    )
    assert claim is not None
    backend = RecoveryBackend()
    executor = _executor(
        repository,
        engine,
        backend,
        clock=MutableClock(NOW + timedelta(seconds=10)),
    )

    result = await executor.recover_once(claim)

    assert result.status is ExecutorStepStatus.COMPLETED
    assert [call[0] for call in backend.execute_calls] == (
        [action.action_id] if expected_method == "execute" else []
    )
    assert [call[0] for call in backend.reconcile_calls] == (
        [action.action_id] if expected_method == "reconcile" else []
    )
    calls = backend.execute_calls or backend.reconcile_calls
    if calls:
        context = calls[0][1]
        assert context.action_id == action.action_id
        assert context.idempotency_key == action.idempotency_key
    loaded = await repository.load(ATTEMPT_ID)
    assert loaded is not None
    expected = (
        ActionStatus.OUTCOME_UNKNOWN
        if policy is RecoveryPolicy.NON_REPLAYABLE
        else ActionStatus.SUCCEEDED
    )
    assert loaded.state.actions[0].status is expected


@pytest.mark.asyncio
async def test_inconclusive_reconciliation_waits_external_and_holds_reservation(
) -> None:
    repository = InMemoryAttemptRepository()
    engine, action, claim = await _persist_action(
        repository, RecoveryPolicy.RECONCILABLE, started=True
    )
    started = await repository.load(ATTEMPT_ID)
    assert started is not None
    reserved = started.state.budget.resources[0].reserved
    backend = RecoveryBackend(reconcile_result=None)
    executor = _executor(
        repository,
        engine,
        backend,
        clock=MutableClock(NOW + timedelta(seconds=10)),
    )

    result = await executor.recover_once(claim)

    assert result.status is ExecutorStepStatus.COMPLETED
    loaded = await repository.load(ATTEMPT_ID)
    assert loaded is not None
    assert loaded.state.phase is AttemptPhase.WAITING_EXTERNAL
    assert loaded.state.actions[0].status is ActionStatus.OUTCOME_UNKNOWN
    assert loaded.state.actions[0].reservation_id == action.reservation_id
    assert loaded.state.budget.resources[0].reserved == reserved
    assert len(loaded.state.pending_external) == 1
    assert (
        loaded.state.pending_external[0].request_kind
        is ExternalRequestKind.OUTCOME_RECONCILIATION
    )


@pytest.mark.asyncio
async def test_reconcilable_action_without_capability_fails_closed() -> None:
    repository = InMemoryAttemptRepository()
    engine, _, claim = await _persist_action(
        repository, RecoveryPolicy.RECONCILABLE, started=True
    )
    backend = ExecuteOnlyBackend()
    executor = _executor(
        repository,
        engine,
        backend,
        clock=MutableClock(NOW + timedelta(seconds=10)),
    )

    with pytest.raises(ExecutorConfigurationError):
        await executor.recover_once(claim)

    assert backend.execute_calls == []
    loaded = await repository.load(ATTEMPT_ID)
    assert loaded is not None
    assert loaded.state.actions[0].status is ActionStatus.STARTED


@pytest.mark.asyncio
async def test_non_replayable_started_recovery_needs_no_backend() -> None:
    repository = InMemoryAttemptRepository()
    engine, action, claim = await _persist_action(
        repository, RecoveryPolicy.NON_REPLAYABLE, started=True
    )
    executor = ActionExecutor(
        backends={},
        repository=repository,
        engine=engine,
        clock=MutableClock(NOW + timedelta(seconds=10)),
        lease_seconds=10,
    )

    result = await executor.recover_once(claim)

    assert result.status is ExecutorStepStatus.COMPLETED
    loaded = await repository.load(ATTEMPT_ID)
    assert loaded is not None
    assert loaded.state.phase is AttemptPhase.WAITING_EXTERNAL
    assert loaded.state.actions[0].action_id == action.action_id
    assert loaded.state.actions[0].status is ActionStatus.OUTCOME_UNKNOWN
    assert loaded.state.pending_external[0].request_kind is (
        ExternalRequestKind.OUTCOME_RECONCILIATION
    )


@pytest.mark.asyncio
async def test_normal_dispatch_reconcilable_capability_fails_before_started() -> None:
    repository = InMemoryAttemptRepository()
    engine, _, _ = await _persist_action(
        repository, RecoveryPolicy.RECONCILABLE, started=False
    )
    backend = ExecuteOnlyBackend()
    executor = _executor(
        repository,
        engine,
        backend,
        clock=MutableClock(NOW + timedelta(seconds=10)),
    )

    with pytest.raises(ExecutorConfigurationError):
        await executor.run_once("normal-worker")

    assert backend.execute_calls == []
    loaded = await repository.load(ATTEMPT_ID)
    assert loaded is not None
    assert loaded.state.actions[0].status is ActionStatus.ACCEPTED
    assert not any(event.event_type == "ACTION_STARTED" for event in loaded.events)


@pytest.mark.parametrize(
    "policy", (RecoveryPolicy.REPLAY_SAFE, RecoveryPolicy.RECONCILABLE)
)
@pytest.mark.asyncio
async def test_recovery_confirms_claim_before_each_backend_call(
    policy: RecoveryPolicy,
) -> None:
    base = InMemoryAttemptRepository()
    engine, _, claim = await _persist_action(
        base, policy, started=True
    )
    repository = RejectingConfirmationRepository(base)
    backend = RecoveryBackend()
    executor = _executor(
        repository,
        engine,
        backend,
        clock=MutableClock(NOW + timedelta(seconds=10)),
    )

    result = await executor.recover_once(claim)

    assert result.status is ExecutorStepStatus.RECOVERY_REQUIRED
    assert repository.confirm_calls == 1
    assert backend.execute_calls == []
    assert backend.reconcile_calls == []


@pytest.mark.parametrize(
    "failure",
    (
        TypeError("programming error"),
        asyncio.CancelledError("cancelled"),
        KeyboardInterrupt("test crash"),
    ),
)
@pytest.mark.asyncio
async def test_recovery_does_not_swallow_programming_cancellation_or_crash_errors(
    failure: BaseException,
) -> None:
    repository = InMemoryAttemptRepository()
    engine, _, claim = await _persist_action(
        repository, RecoveryPolicy.REPLAY_SAFE, started=True
    )
    backend = ExplodingRecoveryBackend(failure)
    executor = _executor(
        repository,
        engine,
        backend,
        clock=MutableClock(NOW + timedelta(seconds=10)),
    )

    with pytest.raises(type(failure)):
        await executor.recover_once(claim)


@pytest.mark.asyncio
async def test_startup_recovery_appends_audit_fact_and_is_effect_idempotent(
    tmp_path: Path,
) -> None:
    path = tmp_path / "recovery.sqlite3"
    before_restart = SQLiteAttemptRepository(path)
    engine, action, _ = await _persist_action(
        before_restart,
        RecoveryPolicy.REPLAY_SAFE,
        started=True,
        reclaim_started=False,
    )
    old = await before_restart.load(ATTEMPT_ID)
    assert old is not None
    old_bytes = tuple(event.model_dump_json() for event in old.events)

    repository = SQLiteAttemptRepository(path)
    clock = MutableClock(NOW + timedelta(seconds=10))
    recovery_engine = _reopened_engine(repository, clock=clock)
    backend = RecoveryBackend()
    executor = _executor(repository, recovery_engine, backend, clock=clock)
    coordinator = RecoveryCoordinator(
        repository=repository,
        engine=recovery_engine,
        executor=executor,
        clock=clock,
        worker_id="startup-recovery",
        lease_seconds=10,
    )

    first = await coordinator.recover_startup(max_actions=1)
    second = await coordinator.recover_startup(max_actions=1)

    assert first.recovered_attempt_ids == (ATTEMPT_ID,)
    assert not first.failed_attempt_ids
    assert second.terminal_attempt_ids == ()
    assert second.waiting_attempt_ids == (ATTEMPT_ID,)
    assert [call[0] for call in backend.execute_calls] == [action.action_id]
    loaded = await repository.load(ATTEMPT_ID)
    assert loaded is not None
    assert tuple(
        event.model_dump_json() for event in loaded.events[: len(old_bytes)]
    ) == old_bytes
    assert sum(
        event.event_type == "ATTEMPT_RECOVERY_REQUESTED"
        for event in loaded.events
    ) == 2
    assert sum(
        event.event_type == "ACTION_SUCCEEDED" for event in loaded.events
    ) == 1


@pytest.mark.asyncio
async def test_startup_recovery_respects_max_actions_without_waiting_or_looping(
) -> None:
    repository = InMemoryAttemptRepository()
    engine, _, _ = await _persist_action(
        repository, RecoveryPolicy.REPLAY_SAFE, started=True
    )
    clock = MutableClock(NOW + timedelta(seconds=10))
    backend = RecoveryBackend()
    executor = _executor(repository, engine, backend, clock=clock)
    coordinator = RecoveryCoordinator(
        repository=repository,
        engine=engine,
        executor=executor,
        clock=clock,
        worker_id="startup-recovery",
        lease_seconds=10,
    )

    summary = await asyncio.wait_for(
        coordinator.recover_startup(max_actions=0), timeout=0.1
    )

    assert summary.waiting_attempt_ids == (ATTEMPT_ID,)
    assert backend.execute_calls == []


@pytest.mark.asyncio
async def test_startup_partial_action_recovery_is_waiting_not_recovered() -> None:
    repository = InMemoryAttemptRepository()
    proposals = tuple(
        _proposal(ordinal=ordinal, batch_id="partial-recovery-batch")
        for ordinal in range(2)
    )
    engine, actions = await _persist_started_actions(repository, proposals)
    clock = MutableClock(NOW + timedelta(seconds=10))
    backend = RecoveryBackend()
    executor = _executor(repository, engine, backend, clock=clock)
    coordinator = RecoveryCoordinator(
        repository=repository,
        engine=engine,
        executor=executor,
        clock=clock,
        worker_id="partial-recovery",
        lease_seconds=10,
    )

    summary = await coordinator.recover_startup(max_actions=1)

    assert summary.recovered_attempt_ids == ()
    assert summary.waiting_attempt_ids == (ATTEMPT_ID,)
    loaded = await repository.load(ATTEMPT_ID)
    assert loaded is not None
    assert tuple(action.status for action in loaded.state.actions).count(
        ActionStatus.SUCCEEDED
    ) == 1
    assert tuple(action.status for action in loaded.state.actions).count(
        ActionStatus.STARTED
    ) == 1
    assert len(backend.execute_calls) == 1
    assert len(actions) == 2


@pytest.mark.asyncio
async def test_startup_inconclusive_reconciliation_is_waiting() -> None:
    repository = InMemoryAttemptRepository()
    engine, _, _ = await _persist_action(
        repository,
        RecoveryPolicy.RECONCILABLE,
        started=True,
        reclaim_started=False,
    )
    clock = MutableClock(NOW + timedelta(seconds=10))
    backend = RecoveryBackend(reconcile_result=None)
    executor = _executor(repository, engine, backend, clock=clock)
    coordinator = RecoveryCoordinator(
        repository=repository,
        engine=engine,
        executor=executor,
        clock=clock,
        worker_id="inconclusive-recovery",
        lease_seconds=10,
    )

    summary = await coordinator.recover_startup(max_actions=1)

    assert summary.recovered_attempt_ids == ()
    assert summary.waiting_attempt_ids == (ATTEMPT_ID,)
    loaded = await repository.load(ATTEMPT_ID)
    assert loaded is not None
    assert loaded.state.phase is AttemptPhase.WAITING_EXTERNAL


@pytest.mark.asyncio
async def test_startup_mixed_failure_and_success_is_failed_without_summary_error(
) -> None:
    repository = InMemoryAttemptRepository()
    missing_backend = _proposal(
        ordinal=0,
        batch_id="mixed-recovery-batch",
    ).model_copy(
        update={
            "action_type": ActionType.SEND_MESSAGE,
            "invocation_id": None,
        }
    )
    successful = _proposal(
        ordinal=1,
        batch_id="mixed-recovery-batch",
    )
    engine, _ = await _persist_started_actions(
        repository,
        (missing_backend, successful),
    )
    clock = MutableClock(NOW + timedelta(seconds=10))
    backend = RecoveryBackend()
    executor = _executor(repository, engine, backend, clock=clock)
    coordinator = RecoveryCoordinator(
        repository=repository,
        engine=engine,
        executor=executor,
        clock=clock,
        worker_id="mixed-recovery",
        lease_seconds=10,
    )

    summary = await coordinator.recover_startup(max_actions=2)

    assert summary.failed_attempt_ids == (ATTEMPT_ID,)
    assert summary.recovered_attempt_ids == ()
    assert summary.waiting_attempt_ids == ()
    assert summary.terminal_attempt_ids == ()
    assert len(backend.execute_calls) == 1
