from __future__ import annotations

import asyncio
import sqlite3
from collections.abc import Callable, Mapping
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest

from experiment_system.actions import (
    ActionFailedOutcome,
    ActionOutcome,
    ActionSucceededOutcome,
    ActionType,
)
from experiment_system.commands import (
    CancelAttempt,
    PauseAttempt,
    ReportActionOutcome,
    ReportActionStarted,
    ResumeAttempt,
    action_command_id,
)
from experiment_system.contract import Backend, CommandResult, ExecutionContext
from experiment_system.executor import (
    ActionExecutor,
    ExecutorConfigurationError,
    ExecutorInvariantError,
    ExecutorStepResult,
    ExecutorStepStatus,
)
from experiment_system.engine import AttemptEngine
from experiment_system.state import ActionStatus, AttemptPhase, ErrorSummary
from experiment_system.store import (
    AttemptRepository,
    InvalidCommit,
    OutboxActionStatus,
    RevisionConflict,
)
from experiment_system.stores import InMemoryAttemptRepository, SQLiteAttemptRepository
from experiment_system.stores import sqlite as sqlite_module
from tests.test_experiment_engine import (
    FixedClock,
    QueueIdFactory,
    RecordingArtifactVerifier,
    _artifact,
    _create_command,
    _decision_command,
    _engine,
    _proposal,
    _start_command,
)


NOW = datetime(2026, 7, 23, 9, 0, tzinfo=timezone.utc)


class MutableClock:
    def __init__(self, value: datetime = NOW) -> None:
        self.value = value
        self.calls = 0

    def now_utc(self) -> datetime:
        self.calls += 1
        return self.value


class ReloadingBackend:
    def __init__(
        self,
        *,
        repository: AttemptRepository,
        outcomes: Mapping[str, ActionOutcome],
        failure: BaseException | None = None,
    ) -> None:
        self._repository = repository
        self._outcomes = dict(outcomes)
        self._failure = failure
        self.calls: list[tuple[str, ExecutionContext]] = []

    async def execute(self, action, context: ExecutionContext) -> ActionOutcome:
        loaded = await self._repository.load(context.attempt_id)
        assert loaded is not None
        state_action = next(
            candidate
            for candidate in loaded.state.actions
            if candidate.action_id == action.action_id
        )
        assert state_action.status is ActionStatus.STARTED
        self.calls.append((action.action_id, context))
        if self._failure is not None:
            raise self._failure
        return self._outcomes[action.action_id]


class BlockingFirstBackend(ReloadingBackend):
    def __init__(self, **kwargs) -> None:
        super().__init__(**kwargs)
        self.first_call_started = asyncio.Event()
        self.release_first_call = asyncio.Event()

    async def execute(self, action, context: ExecutionContext) -> ActionOutcome:
        outcome = await super().execute(action, context)
        if len(self.calls) == 1:
            self.first_call_started.set()
            await self.release_first_call.wait()
        return outcome


class SignalingBackend(ReloadingBackend):
    def __init__(self, **kwargs) -> None:
        super().__init__(**kwargs)
        self.returned = False

    async def execute(self, action, context: ExecutionContext) -> ActionOutcome:
        outcome = await super().execute(action, context)
        self.returned = True
        return outcome


class PausingStartEngine:
    def __init__(self, delegate) -> None:
        self._delegate = delegate
        self.start_entered = asyncio.Event()
        self.resume_start = asyncio.Event()

    async def handle(self, command, *, delivery_claim=None) -> CommandResult:
        if isinstance(command, ReportActionStarted):
            self.start_entered.set()
            await self.resume_start.wait()
        if delivery_claim is None:
            return await self._delegate.handle(command)
        return await self._delegate.handle(
            command,
            delivery_claim=delivery_claim,
        )


class FaultOutcomeEngine:
    def __init__(self, delegate, failure: BaseException) -> None:
        self._delegate = delegate
        self._failure = failure

    async def handle(self, command, *, delivery_claim=None) -> CommandResult:
        if isinstance(command, ReportActionOutcome):
            raise self._failure
        if delivery_claim is None:
            return await self._delegate.handle(command)
        return await self._delegate.handle(
            command,
            delivery_claim=delivery_claim,
        )


class MismatchedCausalClaimRepository:
    def __init__(self, delegate: AttemptRepository) -> None:
        self._delegate = delegate

    def __getattr__(self, name: str):
        return getattr(self._delegate, name)

    async def claim_action(self, **kwargs):
        claimed = await self._delegate.claim_action(**kwargs)
        assert claimed is not None
        return claimed.model_copy(
            update={
                "action": claimed.action.model_copy(
                    update={"causal_parent_id": "mismatched-parent"}
                )
            }
        )


class RejectOutcomeEngine:
    def __init__(self, delegate, repository: AttemptRepository) -> None:
        self._delegate = delegate
        self._repository = repository

    async def handle(self, command, *, delivery_claim=None) -> CommandResult:
        if not isinstance(command, ReportActionOutcome):
            if delivery_claim is None:
                return await self._delegate.handle(command)
            return await self._delegate.handle(
                command,
                delivery_claim=delivery_claim,
            )
        loaded = await self._repository.load(command.attempt_id)
        assert loaded is not None
        return CommandResult(
            command_id=command.command_id,
            attempt_id=command.attempt_id,
            accepted=False,
            revision=loaded.state.revision,
            phase=loaded.state.phase,
            error=ErrorSummary(
                code="REVISION_CONFLICT",
                retryable=True,
                safe_message="The Attempt revision changed.",
            ),
        )


class RecordingOutcomeEngine:
    def __init__(self, delegate: AttemptEngine) -> None:
        self._delegate = delegate
        self.outcome_commands: list[ReportActionOutcome] = []

    async def handle(self, command, *, delivery_claim=None) -> CommandResult:
        if isinstance(command, ReportActionOutcome):
            self.outcome_commands.append(command)
        if delivery_claim is None:
            return await self._delegate.handle(command)
        return await self._delegate.handle(command, delivery_claim=delivery_claim)


@pytest.fixture(params=("memory", "sqlite"))
def repository(
    request: pytest.FixtureRequest,
    tmp_path: Path,
) -> AttemptRepository:
    if request.param == "memory":
        return InMemoryAttemptRepository()
    return SQLiteAttemptRepository(tmp_path / "executor.sqlite3")


@pytest.fixture(params=("memory", "sqlite"))
def repository_pair(
    request: pytest.FixtureRequest,
    tmp_path: Path,
) -> tuple[AttemptRepository, AttemptRepository]:
    if request.param == "memory":
        shared = InMemoryAttemptRepository()
        return shared, shared
    database_path = tmp_path / "executor-handoff.sqlite3"
    return (
        SQLiteAttemptRepository(database_path),
        SQLiteAttemptRepository(database_path),
    )


def _race_engine(
    repository: AttemptRepository,
    clock: MutableClock,
    *,
    skip_ids: int = 0,
) -> AttemptEngine:
    ids = QueueIdFactory()
    for _ in range(skip_ids):
        ids.new_uuid()
    return AttemptEngine(
        repository=repository,
        clock=clock,
        id_factory=ids,
        artifact_verifier=RecordingArtifactVerifier(),
        allowed_edges=frozenset({("engine", "worker-a")}),
    )


async def _accept_actions(
    repository: AttemptRepository,
    *,
    count: int = 1,
):
    engine, _, _, _, _ = _engine(repository=repository)
    created = await engine.handle(_create_command())
    started = await engine.handle(_start_command())
    assert created.accepted is True
    assert started.accepted is True
    proposals = tuple(
        _proposal(ordinal=index, batch_id="executor-batch" if count > 1 else None)
        for index in range(count)
    )
    decision = _decision_command(proposals[0]).model_copy(
        update={"proposals": proposals}
    )
    accepted = await engine.handle(decision)
    assert accepted.accepted is True
    loaded = await repository.load(_create_command().attempt_id)
    assert loaded is not None
    return engine, loaded


@pytest.mark.asyncio
async def test_claim_pause_race_never_starts_backend_and_preserves_lease(
    repository: AttemptRepository,
) -> None:
    engine, accepted = await _accept_actions(repository)
    action = accepted.state.actions[0]
    clock = MutableClock()
    backend = ReloadingBackend(
        repository=repository,
        outcomes={action.action_id: _success(action.action_id)},
    )
    pausing_engine = PausingStartEngine(engine)
    executor = ActionExecutor(
        backends={ActionType.INVOKE_AGENT: backend},
        repository=repository,
        engine=pausing_engine,
        clock=clock,
        lease_seconds=10,
    )
    run = asyncio.create_task(executor.run_once("race-worker"))
    await pausing_engine.start_entered.wait()

    paused = await engine.handle(
        PauseAttempt(
            schema_version=1,
            command_type="PAUSE_ATTEMPT",
            command_id=action_command_id(action.action_id, "pause-race"),
            attempt_id=accepted.state.attempt_id,
            expected_revision=accepted.state.revision,
        )
    )
    pausing_engine.resume_start.set()
    step = await run

    assert paused.accepted is True
    assert paused.phase is AttemptPhase.PAUSED
    assert step.status is ExecutorStepStatus.RECOVERY_REQUIRED
    assert backend.calls == []
    after_race = await repository.load(accepted.state.attempt_id)
    assert after_race is not None
    assert after_race.state.actions[0].status is ActionStatus.ACCEPTED
    clock.value = NOW + timedelta(seconds=9)
    assert (
        await repository.claim_action(
            worker_id="early-worker",
            now_utc=clock.now_utc(),
            lease_seconds=10,
        )
        is None
    )

    resumed = await engine.handle(
        ResumeAttempt(
            schema_version=1,
            command_type="RESUME_ATTEMPT",
            command_id=action_command_id(action.action_id, "resume-race"),
            attempt_id=accepted.state.attempt_id,
            expected_revision=paused.revision,
        )
    )
    clock.value = NOW + timedelta(seconds=10)
    reclaimed = await repository.claim_action(
        worker_id="reclaim-worker",
        now_utc=clock.now_utc(),
        lease_seconds=10,
    )

    assert resumed.accepted is True
    assert reclaimed is not None
    assert reclaimed.action.action_id == action.action_id


def _success(action_id: str) -> ActionSucceededOutcome:
    return ActionSucceededOutcome(
        status=ActionStatus.SUCCEEDED,
        action_id=action_id,
        result_ref=_artifact(f"executor-result-{action_id}"),
    )


@pytest.mark.parametrize(
    ("control_kind", "outcome_kind"),
    (("pause", "success"), ("cancel", "failure")),
)
@pytest.mark.asyncio
async def test_backend_outcome_retries_after_control_commits_during_execute(
    repository: AttemptRepository,
    control_kind: str,
    outcome_kind: str,
) -> None:
    engine, accepted = await _accept_actions(repository)
    action = accepted.state.actions[0]
    failure = ErrorSummary(
        code="BACKEND_REJECTED",
        retryable=False,
        safe_message="The Backend returned a known failure.",
    )
    outcome: ActionOutcome
    if outcome_kind == "success":
        outcome = _success(action.action_id)
        expected_status = ActionStatus.SUCCEEDED
        expected_event_type = "ACTION_SUCCEEDED"
    else:
        outcome = ActionFailedOutcome(
            status=ActionStatus.FAILED,
            action_id=action.action_id,
            error=failure,
        )
        expected_status = ActionStatus.FAILED
        expected_event_type = "ACTION_FAILED"
    backend = BlockingFirstBackend(
        repository=repository,
        outcomes={action.action_id: outcome},
    )
    recording_engine = RecordingOutcomeEngine(engine)
    executor = ActionExecutor(
        backends={ActionType.INVOKE_AGENT: backend},
        repository=repository,
        engine=recording_engine,
        clock=MutableClock(),
        lease_seconds=10,
    )
    run = asyncio.create_task(executor.run_once("blocked-control-worker"))
    await backend.first_call_started.wait()
    executing = await repository.load(accepted.state.attempt_id)
    assert executing is not None
    assert executing.state.actions[0].status is ActionStatus.STARTED

    if control_kind == "pause":
        control = PauseAttempt(
            schema_version=1,
            command_type="PAUSE_ATTEMPT",
            command_id=action_command_id(action.action_id, "pause-during-execute"),
            attempt_id=accepted.state.attempt_id,
            expected_revision=executing.state.revision,
        )
        expected_phase = AttemptPhase.PAUSED
    else:
        control = CancelAttempt(
            schema_version=1,
            command_type="CANCEL_ATTEMPT",
            command_id=action_command_id(action.action_id, "cancel-during-execute"),
            attempt_id=accepted.state.attempt_id,
            expected_revision=executing.state.revision,
        )
        expected_phase = AttemptPhase.CANCELLED
    controlled = await engine.handle(control)
    assert controlled.accepted is True
    backend.release_first_call.set()

    step = await run

    assert step.status is ExecutorStepStatus.COMPLETED
    assert len(backend.calls) == 1
    assert len(recording_engine.outcome_commands) == 2
    assert tuple(
        command.command_id for command in recording_engine.outcome_commands
    ) == (
        action_command_id(action.action_id, "outcome"),
        action_command_id(action.action_id, "outcome"),
    )
    assert tuple(
        command.expected_revision
        for command in recording_engine.outcome_commands
    ) == (executing.state.revision, controlled.revision)
    completed = await repository.load(accepted.state.attempt_id)
    assert completed is not None
    assert completed.state.phase is expected_phase
    assert completed.state.actions[0].status is expected_status
    assert sum(
        event.event_type == expected_event_type for event in completed.events
    ) == 1
    assert sum(event.event_type == "BUDGET_SETTLED" for event in completed.events) == 1
    assert tuple(
        record.command_id
        for record in completed.command_records
        if record.command_id == action_command_id(action.action_id, "outcome")
    ) == (action_command_id(action.action_id, "outcome"),)
    counter = completed.state.budget.resources[0]
    assert counter.reserved == 0
    assert counter.consumed == 2
    assert (
        await repository.claim_action(
            worker_id="post-control-worker",
            now_utc=NOW + timedelta(seconds=10),
            lease_seconds=10,
        )
        is None
    )


@pytest.mark.asyncio
async def test_run_once_commits_start_before_backend_and_removes_delivery(
    repository: AttemptRepository,
) -> None:
    engine, accepted = await _accept_actions(repository)
    action = accepted.state.actions[0]
    backend = ReloadingBackend(
        repository=repository,
        outcomes={action.action_id: _success(action.action_id)},
    )
    source_registry: dict[ActionType, Backend] = {
        ActionType.INVOKE_AGENT: backend
    }
    executor = ActionExecutor(
        backends=source_registry,
        repository=repository,
        engine=engine,
        clock=MutableClock(),
        lease_seconds=10,
    )
    source_registry.clear()

    completed = await executor.run_once("worker-a")
    idle = await executor.run_once("worker-a")

    assert completed == ExecutorStepResult(status=ExecutorStepStatus.COMPLETED)
    assert idle == ExecutorStepResult(status=ExecutorStepStatus.IDLE)
    assert len(backend.calls) == 1
    _, context = backend.calls[0]
    assert context == ExecutionContext(
        attempt_id=accepted.state.attempt_id,
        action_id=action.action_id,
        invocation_id=action.invocation_id,
        idempotency_key=action.idempotency_key,
        deadline_at=accepted.state.budget.deadline_at,
        cancellation_requested=False,
    )
    loaded = await repository.load(accepted.state.attempt_id)
    assert loaded is not None
    assert loaded.state.actions[0].status is ActionStatus.SUCCEEDED
    assert {
        record.command_id for record in loaded.command_records
    } >= {
        action_command_id(action.action_id, "started"),
        action_command_id(action.action_id, "outcome"),
    }


@pytest.mark.asyncio
async def test_run_once_handles_at_most_one_claim(
    repository: AttemptRepository,
) -> None:
    engine, accepted = await _accept_actions(repository, count=2)
    outcomes = {
        action.action_id: _success(action.action_id)
        for action in accepted.state.actions
    }
    backend = ReloadingBackend(repository=repository, outcomes=outcomes)
    executor = ActionExecutor(
        backends={ActionType.INVOKE_AGENT: backend},
        repository=repository,
        engine=engine,
        clock=MutableClock(),
        lease_seconds=10,
    )

    result = await executor.run_once("worker-a")

    assert result.status is ExecutorStepStatus.COMPLETED
    assert len(backend.calls) == 1
    loaded = await repository.load(accepted.state.attempt_id)
    assert loaded is not None
    assert tuple(action.status for action in loaded.state.actions).count(
        ActionStatus.SUCCEEDED
    ) == 1
    assert tuple(action.status for action in loaded.state.actions).count(
        ActionStatus.ACCEPTED
    ) == 1


@pytest.mark.asyncio
async def test_expired_started_claim_requires_recovery_without_backend_replay(
    repository: AttemptRepository,
) -> None:
    engine, accepted = await _accept_actions(repository)
    action = accepted.state.actions[0]
    first_claim = await repository.claim_action(
        worker_id="worker-a",
        now_utc=NOW,
        lease_seconds=10,
    )
    assert first_claim is not None
    started = await engine.handle(
        ReportActionStarted(
            schema_version=1,
            command_type="REPORT_ACTION_STARTED",
            command_id=action_command_id(action.action_id, "started"),
            attempt_id=accepted.state.attempt_id,
            expected_revision=accepted.state.revision,
            action_id=action.action_id,
        ),
        delivery_claim=first_claim.delivery_claim(),
    )
    assert started.accepted is True
    backend = ReloadingBackend(
        repository=repository,
        outcomes={action.action_id: _success(action.action_id)},
    )
    clock = MutableClock(NOW + timedelta(seconds=10))
    executor = ActionExecutor(
        backends={ActionType.INVOKE_AGENT: backend},
        repository=repository,
        engine=engine,
        clock=clock,
        lease_seconds=10,
    )

    result = await executor.run_once("worker-b")

    assert result.status is ExecutorStepStatus.RECOVERY_REQUIRED
    assert backend.calls == []
    assert (
        await repository.claim_action(
            worker_id="worker-c",
            now_utc=NOW + timedelta(seconds=19),
            lease_seconds=10,
        )
        is None
    )


@pytest.mark.asyncio
async def test_known_failed_outcome_commits_only_stable_safe_error(
    repository: AttemptRepository,
) -> None:
    engine, accepted = await _accept_actions(repository)
    action = accepted.state.actions[0]
    safe_error = ErrorSummary(
        code="MODEL_CAPACITY_UNAVAILABLE",
        retryable=True,
        safe_message="The execution backend is temporarily unavailable.",
    )
    backend = ReloadingBackend(
        repository=repository,
        outcomes={
            action.action_id: ActionFailedOutcome(
                status=ActionStatus.FAILED,
                action_id=action.action_id,
                error=safe_error,
            )
        },
    )
    executor = ActionExecutor(
        backends={ActionType.INVOKE_AGENT: backend},
        repository=repository,
        engine=engine,
        clock=MutableClock(),
        lease_seconds=10,
    )

    result = await executor.run_once("worker-a")

    assert result.status is ExecutorStepStatus.COMPLETED
    loaded = await repository.load(accepted.state.attempt_id)
    assert loaded is not None
    terminal = loaded.state.actions[0]
    assert terminal.status is ActionStatus.FAILED
    assert terminal.error == safe_error
    persisted = "".join(event.model_dump_json() for event in loaded.events)
    assert "Traceback" not in persisted
    assert "raw backend exception" not in persisted


@pytest.mark.parametrize(
    "failure_type",
    (RuntimeError, asyncio.CancelledError),
)
@pytest.mark.asyncio
async def test_backend_exceptions_escape_with_durable_started_state(
    repository: AttemptRepository,
    failure_type: Callable[[str], BaseException],
) -> None:
    engine, accepted = await _accept_actions(repository)
    action = accepted.state.actions[0]
    backend = ReloadingBackend(
        repository=repository,
        outcomes={},
        failure=failure_type("raw backend exception"),
    )
    executor = ActionExecutor(
        backends={ActionType.INVOKE_AGENT: backend},
        repository=repository,
        engine=engine,
        clock=MutableClock(),
        lease_seconds=10,
    )

    with pytest.raises(failure_type):
        await executor.run_once("worker-a")

    loaded = await repository.load(accepted.state.attempt_id)
    assert loaded is not None
    assert loaded.state.actions[0].status is ActionStatus.STARTED
    assert not any(
        record.command_id == action_command_id(action.action_id, "outcome")
        for record in loaded.command_records
    )
    reclaimed = await repository.claim_action(
        worker_id="worker-b",
        now_utc=NOW + timedelta(seconds=10),
        lease_seconds=5,
    )
    assert reclaimed is not None
    assert reclaimed.action_status is OutboxActionStatus.STARTED


@pytest.mark.asyncio
async def test_rejected_outcome_commit_requires_recovery_without_backend_retry(
    repository: AttemptRepository,
) -> None:
    engine, accepted = await _accept_actions(repository)
    action = accepted.state.actions[0]
    backend = ReloadingBackend(
        repository=repository,
        outcomes={action.action_id: _success(action.action_id)},
    )
    executor = ActionExecutor(
        backends={ActionType.INVOKE_AGENT: backend},
        repository=repository,
        engine=RejectOutcomeEngine(engine, repository),
        clock=MutableClock(),
        lease_seconds=10,
    )

    result = await executor.run_once("worker-a")

    assert result.status is ExecutorStepStatus.RECOVERY_REQUIRED
    assert len(backend.calls) == 1
    loaded = await repository.load(accepted.state.attempt_id)
    assert loaded is not None
    assert loaded.state.actions[0].status is ActionStatus.STARTED


@pytest.mark.asyncio
async def test_expired_accepted_handoff_allows_only_current_claimant_backend(
    repository_pair: tuple[AttemptRepository, AttemptRepository],
) -> None:
    repository_a, repository_b = repository_pair
    clock = MutableClock()
    engine_a = _race_engine(repository_a, clock)
    assert (await engine_a.handle(_create_command())).accepted is True
    assert (await engine_a.handle(_start_command())).accepted is True
    assert (
        await engine_a.handle(_decision_command(_proposal()))
    ).accepted is True
    accepted = await repository_a.load(_create_command().attempt_id)
    assert accepted is not None
    action = accepted.state.actions[0]
    engine_b = _race_engine(repository_b, clock, skip_ids=50)
    paused_engine_a = PausingStartEngine(engine_a)
    backend = BlockingFirstBackend(
        repository=repository_b,
        outcomes={action.action_id: _success(action.action_id)},
    )
    executor_a = ActionExecutor(
        backends={ActionType.INVOKE_AGENT: backend},
        repository=repository_a,
        engine=paused_engine_a,
        clock=clock,
        lease_seconds=10,
    )
    executor_b = ActionExecutor(
        backends={ActionType.INVOKE_AGENT: backend},
        repository=repository_b,
        engine=engine_b,
        clock=clock,
        lease_seconds=10,
    )

    task_a = asyncio.create_task(executor_a.run_once("worker-a"))
    await paused_engine_a.start_entered.wait()
    clock.value = NOW + timedelta(seconds=10)
    task_b = asyncio.create_task(executor_b.run_once("worker-b"))
    await backend.first_call_started.wait()
    paused_engine_a.resume_start.set()
    try:
        result_a = await task_a
    finally:
        backend.release_first_call.set()
    result_b = await task_b

    assert result_a.status is ExecutorStepStatus.RECOVERY_REQUIRED
    assert result_b.status is ExecutorStepStatus.COMPLETED
    assert len(backend.calls) == 1


@pytest.mark.asyncio
async def test_claimed_action_causal_parent_must_match_durable_state(
    repository: AttemptRepository,
) -> None:
    engine, accepted = await _accept_actions(repository)
    action = accepted.state.actions[0]
    backend = ReloadingBackend(
        repository=repository,
        outcomes={action.action_id: _success(action.action_id)},
    )
    executor = ActionExecutor(
        backends={ActionType.INVOKE_AGENT: backend},
        repository=MismatchedCausalClaimRepository(repository),
        engine=engine,
        clock=MutableClock(),
        lease_seconds=10,
    )

    with pytest.raises(ExecutorInvariantError):
        await executor.run_once("worker-a")

    assert backend.calls == []


@pytest.mark.asyncio
async def test_repository_outcome_exception_requires_recovery_after_one_backend_call(
    repository: AttemptRepository,
) -> None:
    engine, accepted = await _accept_actions(repository)
    action = accepted.state.actions[0]
    backend = ReloadingBackend(
        repository=repository,
        outcomes={action.action_id: _success(action.action_id)},
    )
    executor = ActionExecutor(
        backends={ActionType.INVOKE_AGENT: backend},
        repository=repository,
        engine=FaultOutcomeEngine(engine, RevisionConflict()),
        clock=MutableClock(),
        lease_seconds=10,
    )

    result = await executor.run_once("worker-a")

    assert result.status is ExecutorStepStatus.RECOVERY_REQUIRED
    assert len(backend.calls) == 1
    loaded = await repository.load(accepted.state.attempt_id)
    assert loaded is not None
    assert loaded.state.actions[0].status is ActionStatus.STARTED
    assert not any(
        record.command_id == action_command_id(action.action_id, "outcome")
        for record in loaded.command_records
    )
    reclaimed = await repository.claim_action(
        worker_id="worker-b",
        now_utc=NOW + timedelta(seconds=10),
        lease_seconds=5,
    )
    assert reclaimed is not None
    assert reclaimed.action_status is OutboxActionStatus.STARTED


@pytest.mark.asyncio
async def test_programming_error_after_backend_still_escapes(
    repository: AttemptRepository,
) -> None:
    engine, accepted = await _accept_actions(repository)
    action = accepted.state.actions[0]
    backend = ReloadingBackend(
        repository=repository,
        outcomes={action.action_id: _success(action.action_id)},
    )
    executor = ActionExecutor(
        backends={ActionType.INVOKE_AGENT: backend},
        repository=repository,
        engine=FaultOutcomeEngine(engine, ValueError("programming error")),
        clock=MutableClock(),
        lease_seconds=10,
    )

    with pytest.raises(ValueError, match="programming error"):
        await executor.run_once("worker-a")

    assert len(backend.calls) == 1


@pytest.mark.asyncio
@pytest.mark.parametrize("fault_site", ("delete", "begin"))
async def test_sqlite_operational_outcome_fault_keeps_command_id_replayable(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    fault_site: str,
) -> None:
    database_path = tmp_path / "outcome-operational.sqlite3"
    repository = SQLiteAttemptRepository(database_path)
    engine, accepted = await _accept_actions(repository)
    action = accepted.state.actions[0]
    backend_outcome = _success(action.action_id)
    backend = ReloadingBackend(
        repository=repository,
        outcomes={action.action_id: backend_outcome},
    )
    executor = ActionExecutor(
        backends={ActionType.INVOKE_AGENT: backend},
        repository=repository,
        engine=engine,
        clock=MutableClock(),
        lease_seconds=10,
    )
    real_connect = sqlite_module.sqlite3.connect
    real_commit_sync = repository._commit_sync
    fault_pending = True
    inside_outcome_commit = False
    outcome_command_id = action_command_id(action.action_id, "outcome")

    class OperationalFaultConnection:
        def __init__(self, connection: sqlite3.Connection) -> None:
            object.__setattr__(self, "_connection", connection)

        def __getattr__(self, name: str):
            return getattr(self._connection, name)

        def __setattr__(self, name: str, value) -> None:
            setattr(self._connection, name, value)

        def execute(self, sql: str, parameters=()):
            nonlocal fault_pending
            normalized = " ".join(sql.split()).upper()
            fail_at_begin = (
                fault_site == "begin"
                and inside_outcome_commit
                and normalized == "BEGIN IMMEDIATE"
            )
            fail_at_delete = (
                fault_site == "delete"
                and normalized.startswith("DELETE FROM ACTION_OUTBOX")
            )
            if fault_pending and (fail_at_begin or fail_at_delete):
                fault_pending = False
                raise sqlite3.OperationalError("injected outcome transaction fault")
            return self._connection.execute(sql, parameters)

    def faulting_connect(*args, **kwargs):
        return OperationalFaultConnection(real_connect(*args, **kwargs))

    def faulting_commit_sync(request):
        nonlocal inside_outcome_commit
        if request.command_id != outcome_command_id:
            return real_commit_sync(request)
        inside_outcome_commit = True
        try:
            return real_commit_sync(request)
        finally:
            inside_outcome_commit = False

    monkeypatch.setattr(sqlite_module.sqlite3, "connect", faulting_connect)
    monkeypatch.setattr(repository, "_commit_sync", faulting_commit_sync)
    first = await executor.run_once("worker-a")
    monkeypatch.setattr(sqlite_module.sqlite3, "connect", real_connect)
    monkeypatch.setattr(repository, "_commit_sync", real_commit_sync)

    assert first.status is ExecutorStepStatus.RECOVERY_REQUIRED
    assert len(backend.calls) == 1
    started = await repository.load(accepted.state.attempt_id)
    assert started is not None
    assert started.state.actions[0].status is ActionStatus.STARTED
    assert not any(
        record.command_id == action_command_id(action.action_id, "outcome")
        for record in started.command_records
    )

    replayed = await engine.handle(
        ReportActionOutcome(
            schema_version=1,
            command_type="REPORT_ACTION_OUTCOME",
            command_id=action_command_id(action.action_id, "outcome"),
            attempt_id=started.state.attempt_id,
            expected_revision=started.state.revision,
            action_id=action.action_id,
            outcome=backend_outcome,
        )
    )

    assert replayed.accepted is True
    assert len(backend.calls) == 1
    with sqlite3.connect(database_path) as connection:
        assert connection.execute(
            "SELECT COUNT(*) FROM action_outbox"
        ).fetchone()[0] == 0


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "fault_boundary",
    ("find_command", "load", "record_rejection"),
)
async def test_sqlite_post_backend_begin_fault_requires_recovery_and_allows_replay(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    fault_boundary: str,
) -> None:
    database_path = tmp_path / f"post-backend-{fault_boundary}.sqlite3"
    repository = SQLiteAttemptRepository(database_path)
    engine, accepted = await _accept_actions(repository)
    action = accepted.state.actions[0]
    outcome = _success(action.action_id)
    outcome_command_id = action_command_id(action.action_id, "outcome")
    backend = SignalingBackend(
        repository=repository,
        outcomes={action.action_id: outcome},
    )
    executor = ActionExecutor(
        backends={ActionType.INVOKE_AGENT: backend},
        repository=repository,
        engine=engine,
        clock=MutableClock(),
        lease_seconds=10,
    )
    sync_method_names = {
        "find_command": "_find_command_sync",
        "load": "_load_sync",
        "record_rejection": "_record_rejection_sync",
    }
    target_method_name = sync_method_names[fault_boundary]
    real_target_method = getattr(repository, target_method_name)
    real_commit_sync = repository._commit_sync
    real_connect = sqlite_module.sqlite3.connect
    inside_target_boundary = False
    fault_pending = True

    class BeginFaultConnection:
        def __init__(self, connection: sqlite3.Connection) -> None:
            object.__setattr__(self, "_connection", connection)

        def __getattr__(self, name: str):
            return getattr(self._connection, name)

        def __setattr__(self, name: str, value) -> None:
            setattr(self._connection, name, value)

        def execute(self, sql: str, parameters=()):
            nonlocal fault_pending
            normalized = " ".join(sql.split()).upper()
            if (
                fault_pending
                and inside_target_boundary
                and normalized == "BEGIN IMMEDIATE"
            ):
                fault_pending = False
                raise sqlite3.OperationalError("injected post-Backend begin fault")
            return self._connection.execute(sql, parameters)

    def faulting_connect(*args, **kwargs):
        return BeginFaultConnection(real_connect(*args, **kwargs))

    def faulting_target_method(*args, **kwargs):
        nonlocal inside_target_boundary
        if not backend.returned:
            return real_target_method(*args, **kwargs)
        inside_target_boundary = True
        try:
            return real_target_method(*args, **kwargs)
        finally:
            inside_target_boundary = False

    def reject_outcome_commit(request):
        if (
            fault_boundary == "record_rejection"
            and request.command_id == outcome_command_id
        ):
            raise InvalidCommit()
        return real_commit_sync(request)

    monkeypatch.setattr(sqlite_module.sqlite3, "connect", faulting_connect)
    monkeypatch.setattr(repository, target_method_name, faulting_target_method)
    monkeypatch.setattr(repository, "_commit_sync", reject_outcome_commit)
    first = await executor.run_once("worker-a")
    monkeypatch.setattr(sqlite_module.sqlite3, "connect", real_connect)
    monkeypatch.setattr(repository, target_method_name, real_target_method)
    monkeypatch.setattr(repository, "_commit_sync", real_commit_sync)

    assert fault_pending is False
    assert first.status is ExecutorStepStatus.RECOVERY_REQUIRED
    assert len(backend.calls) == 1
    started = await repository.load(accepted.state.attempt_id)
    assert started is not None
    assert started.state.actions[0].status is ActionStatus.STARTED
    assert not any(
        record.command_id == outcome_command_id
        for record in started.command_records
    )

    replayed = await engine.handle(
        ReportActionOutcome(
            schema_version=1,
            command_type="REPORT_ACTION_OUTCOME",
            command_id=outcome_command_id,
            attempt_id=started.state.attempt_id,
            expected_revision=started.state.revision,
            action_id=action.action_id,
            outcome=outcome,
        )
    )

    assert replayed.accepted is True
    assert len(backend.calls) == 1


@pytest.mark.asyncio
@pytest.mark.parametrize("fault_site", ("public", "transaction"))
async def test_sqlite_programming_error_after_backend_escapes_and_allows_replay(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    fault_site: str,
) -> None:
    database_path = tmp_path / f"programming-error-{fault_site}.sqlite3"
    repository = SQLiteAttemptRepository(database_path)
    engine, accepted = await _accept_actions(repository)
    action = accepted.state.actions[0]
    outcome = _success(action.action_id)
    outcome_command_id = action_command_id(action.action_id, "outcome")
    backend = SignalingBackend(
        repository=repository,
        outcomes={action.action_id: outcome},
    )
    executor = ActionExecutor(
        backends={ActionType.INVOKE_AGENT: backend},
        repository=repository,
        engine=engine,
        clock=MutableClock(),
        lease_seconds=10,
    )
    real_find_command_sync = repository._find_command_sync
    real_connect = sqlite_module.sqlite3.connect
    fault_pending = True

    class ProgrammingFaultConnection:
        def __init__(self, connection: sqlite3.Connection) -> None:
            object.__setattr__(self, "_connection", connection)

        def __getattr__(self, name: str):
            return getattr(self._connection, name)

        def __setattr__(self, name: str, value) -> None:
            setattr(self._connection, name, value)

        def execute(self, sql: str, parameters=()):
            nonlocal fault_pending
            normalized = " ".join(sql.split()).upper()
            if (
                fault_pending
                and fault_site == "transaction"
                and normalized.startswith("DELETE FROM ACTION_OUTBOX")
            ):
                fault_pending = False
                raise sqlite3.ProgrammingError("injected transaction defect")
            return self._connection.execute(sql, parameters)

    def faulting_connect(*args, **kwargs):
        return ProgrammingFaultConnection(real_connect(*args, **kwargs))

    def faulting_find_command_sync(attempt_id, command_id):
        nonlocal fault_pending
        if fault_pending and fault_site == "public" and backend.returned:
            fault_pending = False
            raise sqlite3.ProgrammingError("injected public boundary defect")
        return real_find_command_sync(attempt_id, command_id)

    monkeypatch.setattr(sqlite_module.sqlite3, "connect", faulting_connect)
    monkeypatch.setattr(
        repository,
        "_find_command_sync",
        faulting_find_command_sync,
    )
    with pytest.raises(sqlite3.ProgrammingError, match="injected"):
        await executor.run_once("worker-a")
    monkeypatch.setattr(sqlite_module.sqlite3, "connect", real_connect)
    monkeypatch.setattr(
        repository,
        "_find_command_sync",
        real_find_command_sync,
    )

    assert fault_pending is False
    assert len(backend.calls) == 1
    started = await repository.load(accepted.state.attempt_id)
    assert started is not None
    assert started.state.actions[0].status is ActionStatus.STARTED
    assert not any(
        record.command_id == outcome_command_id
        for record in started.command_records
    )

    replayed = await engine.handle(
        ReportActionOutcome(
            schema_version=1,
            command_type="REPORT_ACTION_OUTCOME",
            command_id=outcome_command_id,
            attempt_id=started.state.attempt_id,
            expected_revision=started.state.revision,
            action_id=action.action_id,
            outcome=outcome,
        )
    )

    assert replayed.accepted is True
    assert len(backend.calls) == 1


@pytest.mark.asyncio
async def test_missing_backend_fails_closed_before_action_start(
    repository: AttemptRepository,
) -> None:
    engine, accepted = await _accept_actions(repository)
    executor = ActionExecutor(
        backends={},
        repository=repository,
        engine=engine,
        clock=MutableClock(),
        lease_seconds=10,
    )

    with pytest.raises(ExecutorConfigurationError):
        await executor.run_once("worker-a")

    loaded = await repository.load(accepted.state.attempt_id)
    assert loaded is not None
    assert loaded.state.actions[0].status is ActionStatus.ACCEPTED


def test_executor_types_are_closed_immutable_and_validate_lease() -> None:
    assert {status.value for status in ExecutorStepStatus} == {
        "IDLE",
        "COMPLETED",
        "RECOVERY_REQUIRED",
    }
    result = ExecutorStepResult(status=ExecutorStepStatus.IDLE)
    with pytest.raises(Exception):
        result.status = ExecutorStepStatus.COMPLETED

    repository = InMemoryAttemptRepository()
    engine, _, _, _, _ = _engine(repository=repository)
    for invalid in (0, -1, float("inf"), float("nan"), True):
        with pytest.raises(ValueError, match="lease_seconds"):
            ActionExecutor(
                backends={},
                repository=repository,
                engine=engine,
                clock=FixedClock(),
                lease_seconds=invalid,
            )
