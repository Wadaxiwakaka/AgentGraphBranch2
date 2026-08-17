from __future__ import annotations

import asyncio
import json
from collections import deque
from datetime import datetime, timedelta, timezone
from hashlib import sha256
from uuid import UUID

import pytest

from experiment_system.actions import (
    ActionCancelledOutcome,
    ActionFailedOutcome,
    ActionProposal,
    ActionSucceededOutcome,
    ActionType,
    ActionUnknownOutcome,
    ResourceKind,
    ResourceRequest,
)
from experiment_system.commands import (
    ApplyStrategyDecision,
    CreateAttempt,
    FinishAttempt,
    ReportActionOutcome,
    ReportActionStarted,
    StartAttempt,
    action_command_id,
    strategy_action_id,
)
from experiment_system.contract import CommandResult, StrategyDirective
from experiment_system.engine import ArtifactVerificationError, AttemptEngine
from experiment_system.events import (
    ActionAccepted,
    ActionCancelled,
    ActionFailed,
    ActionOutcomeUnknown,
    ActionProposed,
    ActionRejected,
    ActionStarted,
    ActionSucceeded,
    AttemptFailed,
    AttemptInterrupted,
    AttemptPlanned,
    AttemptStarted,
    AttemptSucceeded,
    BudgetReservationEntry,
    BudgetReleased,
    BudgetReserved,
    BudgetSettled,
    ExternalInputRequested,
    InvocationCompleted,
    InvocationFailed,
    InvocationRequested,
    InvocationStarted,
    StrategyDecisionRecorded,
)
from experiment_system.reducer import StateTransitionError, apply_event, replay_events
from experiment_system.state import (
    ActionState,
    ActionStatus,
    ArtifactRef,
    AttemptPhase,
    BudgetState,
    ErrorSummary,
    ExternalRequestKind,
    InvocationStatus,
    RecoveryPolicy,
    ResourceBudget,
    StrategyStateEnvelope,
)
from experiment_system.stores.memory import InMemoryAttemptRepository
from experiment_system.store import ArtifactRegistration, CommitRequest


UTC_NOW = datetime(2026, 7, 23, 8, 30, tzinfo=timezone.utc)
ATTEMPT_ID = "attempt-1"
CREATE_COMMAND_ID = UUID("10000000-0000-0000-0000-000000000001")
START_COMMAND_ID = UUID("10000000-0000-0000-0000-000000000002")
DECISION_COMMAND_ID = UUID("10000000-0000-0000-0000-000000000003")
START_ACTION_COMMAND_ID = UUID("10000000-0000-0000-0000-000000000004")
OUTCOME_COMMAND_ID = UUID("10000000-0000-0000-0000-000000000005")
FINISH_COMMAND_ID = UUID("10000000-0000-0000-0000-000000000006")
FINISH_AFTER_OUTCOME_COMMAND_ID = UUID("10000000-0000-0000-0000-000000000007")


def _uuid(index: int) -> UUID:
    return UUID(f"00000000-0000-0000-0000-{index:012d}")


def _canonical_json(value: object) -> bytes:
    return json.dumps(
        value,
        ensure_ascii=False,
        allow_nan=False,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")


def _artifact(name: str, *, digest: str | None = None) -> ArtifactRef:
    return ArtifactRef(
        capture_class="full",
        content_hash=digest or sha256(name.encode("utf-8")).hexdigest(),
        media_type="application/json",
        byte_size=len(name),
        relative_path=f"attempts/{ATTEMPT_ID}/{name}.json",
    )


def _strategy(round_no: int = 0) -> StrategyStateEnvelope:
    value = {"round": round_no}
    payload = _canonical_json(value)
    return StrategyStateEnvelope(
        strategy_id="router",
        strategy_schema_version=1,
        value=value,
        content_hash=sha256(payload).hexdigest(),
        byte_size=len(payload),
    )


def _budget(*, reserved: int = 0, consumed: int = 0) -> BudgetState:
    return BudgetState(
        resources=(
            ResourceBudget(
                resource=ResourceKind.MODEL_CALLS.value,
                limit=10,
                reserved=reserved,
                consumed=consumed,
            ),
        ),
        deadline_at=UTC_NOW + timedelta(hours=1),
        max_call_depth=4,
        max_concurrent_actions=2,
    )


def _create_command(
    *,
    command_id: UUID = CREATE_COMMAND_ID,
    experiment_id: str = "experiment-1",
    manifest_ref: ArtifactRef | None = None,
) -> CreateAttempt:
    return CreateAttempt(
        schema_version=1,
        command_type="CREATE_ATTEMPT",
        command_id=command_id,
        attempt_id=ATTEMPT_ID,
        expected_revision=0,
        state_schema_version=1,
        experiment_id=experiment_id,
        trial_id="trial-1",
        strategy_id="router",
        manifest_ref=manifest_ref or _artifact("manifest"),
        strategy=_strategy(),
        budget=_budget(),
    )


def _start_command(*, expected_revision: int = 1) -> StartAttempt:
    return StartAttempt(
        schema_version=1,
        command_type="START_ATTEMPT",
        command_id=START_COMMAND_ID,
        attempt_id=ATTEMPT_ID,
        expected_revision=expected_revision,
    )


def _proposal(
    *,
    payload_ref: ArtifactRef | None = None,
    ordinal: int = 0,
    batch_id: str | None = None,
) -> ActionProposal:
    return ActionProposal(
        action_id=strategy_action_id(ATTEMPT_ID, "router", ordinal),
        action_type=ActionType.INVOKE_AGENT,
        actor="engine",
        target_ids=("worker-a",),
        invocation_id=f"invocation-{ordinal + 1}",
        causal_parent_id="previous-event",
        payload_ref=payload_ref or _artifact(f"action-payload-{ordinal}"),
        recovery_policy=RecoveryPolicy.REPLAY_SAFE,
        requested_timeout=30,
        batch_id=batch_id,
        call_depth=0,
        resource_requests=(
            ResourceRequest(resource=ResourceKind.MODEL_CALLS, amount=2),
        ),
    )


def _decision_command(
    proposal: ActionProposal | None = None,
    *,
    command_id: UUID = DECISION_COMMAND_ID,
    expected_revision: int = 2,
    trigger_sequence_no: int = 2,
    directive: StrategyDirective = StrategyDirective.CONTINUE,
    result_ref: ArtifactRef | None = None,
    error: ErrorSummary | None = None,
) -> ApplyStrategyDecision:
    return ApplyStrategyDecision(
        schema_version=1,
        command_type="APPLY_STRATEGY_DECISION",
        command_id=command_id,
        attempt_id=ATTEMPT_ID,
        expected_revision=expected_revision,
        trigger_sequence_no=trigger_sequence_no,
        strategy=_strategy(1),
        proposals=(proposal or _proposal(),) if directive is StrategyDirective.CONTINUE else (),
        directive=directive,
        result_ref=result_ref,
        error=error,
    )


def _reservation_entry(action: ActionState) -> BudgetReservationEntry:
    assert action.reservation_id is not None
    return BudgetReservationEntry(
        action_id=action.action_id,
        reservation_id=action.reservation_id,
        resource_requests=action.resource_requests,
    )


class FixedClock:
    def __init__(self) -> None:
        self.calls = 0

    def now_utc(self) -> datetime:
        self.calls += 1
        return UTC_NOW


class QueueIdFactory:
    def __init__(self) -> None:
        self.values = deque(_uuid(index) for index in range(1, 200))
        self.calls = 0

    def new_uuid(self) -> UUID:
        self.calls += 1
        return self.values.popleft()


class RecordingArtifactVerifier:
    def __init__(self, *, rejected_hashes: frozenset[str] = frozenset()) -> None:
        self.rejected_hashes = rejected_hashes
        self.calls: list[ArtifactRef] = []

    def verify(self, ref: ArtifactRef) -> None:
        self.calls.append(ref)
        if ref.content_hash in self.rejected_hashes:
            raise ArtifactVerificationError()


class YieldingLoadRepository(InMemoryAttemptRepository):
    def __init__(self) -> None:
        super().__init__()
        self.active_loads = 0
        self.peak_loads = 0

    async def load(self, attempt_id: str):
        self.active_loads += 1
        self.peak_loads = max(self.peak_loads, self.active_loads)
        try:
            loaded = await super().load(attempt_id)
            await asyncio.sleep(0)
            return loaded
        finally:
            self.active_loads -= 1


def _engine(
    *,
    allowed_edges: frozenset[tuple[str, str]] = frozenset(
        {("engine", "worker-a")}
    ),
    verifier: RecordingArtifactVerifier | None = None,
    repository: InMemoryAttemptRepository | None = None,
) -> tuple[
    AttemptEngine,
    InMemoryAttemptRepository,
    FixedClock,
    QueueIdFactory,
    RecordingArtifactVerifier,
]:
    actual_repository = repository or InMemoryAttemptRepository()
    clock = FixedClock()
    ids = QueueIdFactory()
    actual_verifier = verifier or RecordingArtifactVerifier()
    return (
        AttemptEngine(
            repository=actual_repository,
            clock=clock,
            id_factory=ids,
            artifact_verifier=actual_verifier,
            allowed_edges=allowed_edges,
        ),
        actual_repository,
        clock,
        ids,
        actual_verifier,
    )


async def _start_attempt(engine: AttemptEngine) -> None:
    created = await engine.handle(_create_command())
    started = await engine.handle(_start_command())
    assert created.accepted is True
    assert started.accepted is True


async def _accept_invocation(engine: AttemptEngine) -> str:
    await _start_attempt(engine)
    proposal = _proposal()
    result = await engine.handle(_decision_command(proposal))
    assert result.accepted is True
    return proposal.action_id


async def _start_invocation(
    engine: AttemptEngine,
    repository: InMemoryAttemptRepository,
) -> str:
    action_id = await _accept_invocation(engine)
    claimed = await repository.claim_action(
        worker_id="worker-start",
        now_utc=UTC_NOW,
        lease_seconds=10,
    )
    assert claimed is not None
    assert claimed.action.action_id == action_id
    result = await engine.handle(
        ReportActionStarted(
            schema_version=1,
            command_type="REPORT_ACTION_STARTED",
            command_id=START_ACTION_COMMAND_ID,
            attempt_id=ATTEMPT_ID,
            expected_revision=7,
            action_id=action_id,
        ),
        delivery_claim=claimed.delivery_claim(),
    )
    assert result.accepted is True
    return action_id


async def _commit_legacy_bare_unknown(
    repository: InMemoryAttemptRepository,
    *,
    action_id: str,
    error: ErrorSummary,
) -> ActionOutcomeUnknown:
    loaded = await repository.load(ATTEMPT_ID)
    assert loaded is not None
    command_id = OUTCOME_COMMAND_ID
    event = ActionOutcomeUnknown(
        schema_version=1,
        event_id=_uuid(198),
        trial_id=loaded.state.trial_id,
        attempt_id=ATTEMPT_ID,
        sequence_no=loaded.state.revision + 1,
        event_type="ACTION_OUTCOME_UNKNOWN",
        command_id=command_id,
        causal_parent_id=loaded.latest_event_id,
        logical_time=loaded.events[-1].logical_time + 1,
        wall_time_utc=UTC_NOW,
        action_id=action_id,
        outcome=ActionUnknownOutcome(
            status=ActionStatus.OUTCOME_UNKNOWN,
            action_id=action_id,
            error=error,
        ),
    )
    projected = replay_events((*loaded.events, event))
    await repository.commit(
        CommitRequest(
            command_id=command_id,
            request_hash=sha256(b"legacy-bare-unknown").hexdigest(),
            attempt_id=ATTEMPT_ID,
            expected_revision=loaded.state.revision,
            events=(event,),
            outbox_actions=(),
            artifact_registrations=(),
            completed_delivery_action_ids=(action_id,),
            result=CommandResult(
                command_id=command_id,
                attempt_id=ATTEMPT_ID,
                accepted=True,
                revision=projected.revision,
                phase=projected.phase,
            ),
            checkpoint=False,
        )
    )
    return event


@pytest.mark.asyncio
async def test_create_and_start_commit_contiguous_deterministic_metadata() -> None:
    engine, repository, _, _, verifier = _engine()

    created = await engine.handle(_create_command())
    started = await engine.handle(_start_command())

    assert created.accepted is True
    assert created.revision == 1
    assert created.phase is AttemptPhase.PLANNED
    assert started.accepted is True
    assert started.revision == 2
    assert started.phase is AttemptPhase.RUNNING
    events = await repository.list_events(ATTEMPT_ID)
    assert tuple(type(event) for event in events) == (
        AttemptPlanned,
        AttemptStarted,
    )
    assert tuple(event.sequence_no for event in events) == (1, 2)
    assert tuple(event.logical_time for event in events) == (1, 2)
    assert events[0].causal_parent_id is None
    assert events[1].causal_parent_id == events[0].event_id
    assert tuple(event.wall_time_utc for event in events) == (UTC_NOW, UTC_NOW)
    assert verifier.calls == [_create_command().manifest_ref]


@pytest.mark.asyncio
async def test_create_rejects_budget_reserved_without_reservation_facts() -> None:
    engine, repository, _, _, _ = _engine()
    command = _create_command().model_copy(
        update={"budget": _budget(reserved=2)}
    )

    result = await engine.handle(command)

    assert result.accepted is False
    assert result.error is not None
    assert result.error.code == "ILLEGAL_TRANSITION"
    assert await repository.load(ATTEMPT_ID) is None
    assert await repository.list_events(ATTEMPT_ID) == ()


@pytest.mark.asyncio
async def test_allowed_invocation_decision_commits_ordered_events_and_outbox() -> None:
    engine, repository, _, _, verifier = _engine()
    await _start_attempt(engine)
    proposal = _proposal()

    result = await engine.handle(_decision_command(proposal))

    assert result.accepted is True
    assert result.revision == 7
    loaded = await repository.load(ATTEMPT_ID)
    assert loaded is not None
    assert tuple(type(event) for event in loaded.events[-5:]) == (
        StrategyDecisionRecorded,
        ActionProposed,
        InvocationRequested,
        BudgetReserved,
        ActionAccepted,
    )
    transition_events = loaded.events[-5:]
    recorded = transition_events[0]
    assert isinstance(recorded, StrategyDecisionRecorded)
    assert recorded.trigger_sequence_no == 2
    assert recorded.proposals == (proposal,)
    assert recorded.directive is StrategyDirective.CONTINUE
    assert recorded.result_ref is None
    assert recorded.error is None
    assert all(
        child.causal_parent_id == parent.event_id
        for parent, child in zip(
            transition_events,
            transition_events[1:],
        )
    )
    proposed_event = loaded.events[-4]
    assert isinstance(proposed_event, ActionProposed)
    assert proposed_event.proposal.causal_parent_id == str(
        proposed_event.causal_parent_id
    )
    assert loaded.state.strategy == _strategy(1)
    assert loaded.state.actions[0].status is ActionStatus.ACCEPTED
    assert loaded.state.invocations[0].status is InvocationStatus.REQUESTED
    assert loaded.state.budget.resources[0].reserved == 2
    assert loaded.state.budget.resources[0].consumed == 0
    reserved_event = loaded.events[-2]
    accepted_event = loaded.events[-1]
    assert isinstance(reserved_event, BudgetReserved)
    assert isinstance(accepted_event, ActionAccepted)
    assert reserved_event.reservations == (
        BudgetReservationEntry(
            action_id=accepted_event.action.action_id,
            reservation_id=accepted_event.action.reservation_id,
            resource_requests=accepted_event.action.resource_requests,
        ),
    )
    claimed = await repository.claim_action(
        worker_id="worker-1",
        now_utc=UTC_NOW,
        lease_seconds=10,
    )
    assert claimed is not None
    assert claimed.action.action_id == proposal.action_id
    assert verifier.calls[-1] == proposal.payload_ref


@pytest.mark.asyncio
async def test_strategy_cursor_requires_new_eligible_committed_event_before_metadata() -> None:
    engine, repository, clock, ids, verifier = _engine()
    await _start_attempt(engine)
    baseline = (clock.calls, ids.calls, len(verifier.calls))

    planned_trigger = await engine.handle(
        _decision_command(command_id=_uuid(194), trigger_sequence_no=1)
    )

    assert planned_trigger.accepted is False
    assert planned_trigger.error is not None
    assert planned_trigger.error.code == "INVALID_STRATEGY_TRIGGER"
    assert (clock.calls, ids.calls, len(verifier.calls)) == baseline

    accepted = await engine.handle(_decision_command())
    assert accepted.accepted is True
    after_accepted = (clock.calls, ids.calls, len(verifier.calls))

    stale = await engine.handle(
        _decision_command(
            command_id=_uuid(195),
            expected_revision=7,
            trigger_sequence_no=2,
        ).model_copy(update={"strategy": _strategy(2)})
    )
    bookkeeping = await engine.handle(
        _decision_command(
            command_id=_uuid(196),
            expected_revision=7,
            trigger_sequence_no=7,
        ).model_copy(update={"strategy": _strategy(2)})
    )

    assert stale.accepted is False
    assert stale.error is not None
    assert stale.error.code == "INVALID_STRATEGY_TRIGGER"
    assert bookkeeping.accepted is False
    assert bookkeeping.error is not None
    assert bookkeeping.error.code == "INVALID_STRATEGY_TRIGGER"
    assert (clock.calls, ids.calls, len(verifier.calls)) == after_accepted
    assert len(await repository.list_events(ATTEMPT_ID)) == 7


@pytest.mark.asyncio
async def test_strategy_cursor_cannot_skip_oldest_unconsumed_trigger() -> None:
    engine, repository, clock, ids, verifier = _engine(allowed_edges=frozenset())
    await _start_attempt(engine)
    first = _proposal(ordinal=0, batch_id="rejected-batch")
    second = _proposal(ordinal=1, batch_id="rejected-batch")
    initial = _decision_command(first).model_copy(
        update={"proposals": (first, second)}
    )
    accepted = await engine.handle(initial)
    assert accepted.accepted is True
    loaded = await repository.load(ATTEMPT_ID)
    assert loaded is not None
    rejections = tuple(
        event for event in loaded.events if isinstance(event, ActionRejected)
    )
    assert len(rejections) == 2
    baseline = (clock.calls, ids.calls, len(verifier.calls), loaded.state.revision)

    skipped = await engine.handle(
        _decision_command(
            command_id=_uuid(203),
            expected_revision=loaded.state.revision,
            trigger_sequence_no=rejections[1].sequence_no,
        ).model_copy(update={"proposals": (), "strategy": _strategy(2)})
    )

    assert skipped.accepted is False
    assert skipped.error is not None
    assert skipped.error.code == "INVALID_STRATEGY_TRIGGER"
    after = await repository.load(ATTEMPT_ID)
    assert after is not None
    assert after.state.revision == baseline[3]
    assert (clock.calls, ids.calls, len(verifier.calls)) == baseline[:3]


@pytest.mark.asyncio
async def test_terminal_strategy_decision_prevents_later_strategy_decisions() -> None:
    engine, repository, _, _, _ = _engine()
    await _start_attempt(engine)
    first = _proposal(ordinal=0, batch_id="batch-finality")
    second = _proposal(ordinal=1, batch_id="batch-finality")
    initial = _decision_command(first).model_copy(
        update={"proposals": (first, second)}
    )
    accepted = await engine.handle(initial)
    assert accepted.accepted is True

    first_claim = await repository.claim_action(
        worker_id="worker-first",
        now_utc=UTC_NOW,
        lease_seconds=10,
    )
    assert first_claim is not None
    assert first_claim.action.action_id == first.action_id

    await engine.handle(
        ReportActionStarted(
            schema_version=1,
            command_type="REPORT_ACTION_STARTED",
            command_id=_uuid(197),
            attempt_id=ATTEMPT_ID,
            expected_revision=10,
            action_id=first.action_id,
        ),
        delivery_claim=first_claim.delivery_claim(),
    )
    await engine.handle(
        ReportActionOutcome(
            schema_version=1,
            command_type="REPORT_ACTION_OUTCOME",
            command_id=_uuid(198),
            attempt_id=ATTEMPT_ID,
            expected_revision=12,
            action_id=first.action_id,
            outcome=ActionSucceededOutcome(
                status=ActionStatus.SUCCEEDED,
                action_id=first.action_id,
                result_ref=_artifact("first-action-result"),
            ),
        )
    )
    terminal = await engine.handle(
        _decision_command(
            command_id=_uuid(199),
            expected_revision=15,
            trigger_sequence_no=14,
            directive=StrategyDirective.SUCCEED,
            result_ref=_artifact("attempt-result"),
        )
    )
    assert terminal.accepted is True

    second_claim = await repository.claim_action(
        worker_id="worker-second",
        now_utc=UTC_NOW,
        lease_seconds=10,
    )
    assert second_claim is not None
    assert second_claim.action.action_id == second.action_id

    await engine.handle(
        ReportActionStarted(
            schema_version=1,
            command_type="REPORT_ACTION_STARTED",
            command_id=_uuid(200),
            attempt_id=ATTEMPT_ID,
            expected_revision=16,
            action_id=second.action_id,
        ),
        delivery_claim=second_claim.delivery_claim(),
    )
    await engine.handle(
        ReportActionOutcome(
            schema_version=1,
            command_type="REPORT_ACTION_OUTCOME",
            command_id=_uuid(201),
            attempt_id=ATTEMPT_ID,
            expected_revision=18,
            action_id=second.action_id,
            outcome=ActionSucceededOutcome(
                status=ActionStatus.SUCCEEDED,
                action_id=second.action_id,
                result_ref=_artifact("second-action-result"),
            ),
        )
    )
    later = _decision_command(
        command_id=_uuid(202),
        expected_revision=21,
        trigger_sequence_no=20,
    ).model_copy(update={"proposals": (), "strategy": _strategy(2)})

    rejected = await engine.handle(later)

    assert rejected.accepted is False
    assert rejected.error is not None
    assert rejected.error.code == "ILLEGAL_TRANSITION"
    assert len(await repository.list_events(ATTEMPT_ID)) == 21


@pytest.mark.asyncio
async def test_forbidden_action_records_rejection_without_invocation_budget_or_outbox() -> None:
    engine, repository, _, _, _ = _engine(allowed_edges=frozenset())
    await _start_attempt(engine)

    result = await engine.handle(_decision_command())

    assert result.accepted is True
    loaded = await repository.load(ATTEMPT_ID)
    assert loaded is not None
    assert tuple(type(event) for event in loaded.events[-3:]) == (
        StrategyDecisionRecorded,
        ActionProposed,
        ActionRejected,
    )
    assert loaded.state.actions[0].status is ActionStatus.REJECTED
    assert loaded.state.invocations == ()
    assert loaded.state.budget.resources[0].reserved == 0
    assert (
        await repository.claim_action(
            worker_id="worker-1",
            now_utc=UTC_NOW,
            lease_seconds=10,
        )
        is None
    )


@pytest.mark.asyncio
async def test_action_start_and_success_outcome_order_and_settle_budget_once() -> None:
    engine, repository, clock, ids, verifier = _engine()
    action_id = await _start_invocation(engine, repository)
    loaded = await repository.load(ATTEMPT_ID)
    assert loaded is not None
    assert tuple(type(event) for event in loaded.events[-2:]) == (
        ActionStarted,
        InvocationStarted,
    )
    assert loaded.state.actions[0].status is ActionStatus.STARTED
    assert loaded.state.invocations[0].status is InvocationStatus.RUNNING
    result_ref = _artifact("action-result")
    command = ReportActionOutcome(
        schema_version=1,
        command_type="REPORT_ACTION_OUTCOME",
        command_id=OUTCOME_COMMAND_ID,
        attempt_id=ATTEMPT_ID,
        expected_revision=9,
        action_id=action_id,
        outcome=ActionSucceededOutcome(
            status=ActionStatus.SUCCEEDED,
            action_id=action_id,
            result_ref=result_ref,
        ),
    )

    first = await engine.handle(command)

    assert first.accepted is True
    assert first.revision == 12
    completed = await repository.load(ATTEMPT_ID)
    assert completed is not None
    assert tuple(type(event) for event in completed.events[-3:]) == (
        InvocationCompleted,
        ActionSucceeded,
        BudgetSettled,
    )
    assert completed.state.actions[0].status is ActionStatus.SUCCEEDED
    assert completed.state.invocations[0].status is InvocationStatus.COMPLETED
    assert completed.state.budget.resources[0].reserved == 0
    assert completed.state.budget.resources[0].consumed == 2
    settled_event = completed.events[-1]
    assert isinstance(settled_event, BudgetSettled)
    assert settled_event.reservation == _reservation_entry(
        completed.state.actions[0]
    )
    assert verifier.calls[-1] == result_ref
    counts = (clock.calls, ids.calls, len(verifier.calls), len(completed.events))

    duplicate = await engine.handle(command)

    assert duplicate == first
    reloaded = await repository.load(ATTEMPT_ID)
    assert reloaded is not None
    assert (clock.calls, ids.calls, len(verifier.calls), len(reloaded.events)) == counts


@pytest.mark.asyncio
async def test_unknown_outcome_waits_for_reconciliation_and_keeps_reservation() -> None:
    engine, repository, _, _, _ = _engine()
    action_id = await _start_invocation(engine, repository)
    error = ErrorSummary(
        code="OBSERVATION_LOST",
        retryable=False,
        safe_message="The external outcome could not be observed.",
    )

    result = await engine.handle(
        ReportActionOutcome(
            schema_version=1,
            command_type="REPORT_ACTION_OUTCOME",
            command_id=OUTCOME_COMMAND_ID,
            attempt_id=ATTEMPT_ID,
            expected_revision=9,
            action_id=action_id,
            outcome=ActionUnknownOutcome(
                status=ActionStatus.OUTCOME_UNKNOWN,
                action_id=action_id,
                error=error,
            ),
        )
    )

    assert result.accepted is True
    loaded = await repository.load(ATTEMPT_ID)
    assert loaded is not None
    assert tuple(type(event) for event in loaded.events[-2:]) == (
        ActionOutcomeUnknown,
        ExternalInputRequested,
    )
    assert loaded.state.phase is AttemptPhase.WAITING_EXTERNAL
    assert loaded.state.pending_external[0].request_kind is (
        ExternalRequestKind.OUTCOME_RECONCILIATION
    )
    assert loaded.state.pending_external[0].action_id == action_id
    assert loaded.state.actions[0].status is ActionStatus.OUTCOME_UNKNOWN
    assert loaded.state.invocations[0].status is InvocationStatus.RUNNING
    assert loaded.state.budget.resources[0].reserved == 2
    assert loaded.state.budget.resources[0].consumed == 0
    assert loaded.checkpoint_revisions[-1] == loaded.state.revision
    assert (
        await repository.claim_action(
            worker_id="worker-1",
            now_utc=UTC_NOW,
            lease_seconds=10,
        )
        is None
    )


@pytest.mark.asyncio
async def test_failed_outcome_orders_terminal_events_and_settles_only_once() -> None:
    engine, repository, clock, ids, verifier = _engine()
    action_id = await _start_invocation(engine, repository)
    error = ErrorSummary(
        code="BACKEND_FAILED",
        retryable=True,
        safe_message="The backend did not complete the Action.",
    )
    command = ReportActionOutcome(
        schema_version=1,
        command_type="REPORT_ACTION_OUTCOME",
        command_id=OUTCOME_COMMAND_ID,
        attempt_id=ATTEMPT_ID,
        expected_revision=9,
        action_id=action_id,
        outcome=ActionFailedOutcome(
            status=ActionStatus.FAILED,
            action_id=action_id,
            error=error,
        ),
    )

    first = await engine.handle(command)

    assert first.accepted is True
    loaded = await repository.load(ATTEMPT_ID)
    assert loaded is not None
    assert tuple(type(event) for event in loaded.events[-3:]) == (
        InvocationFailed,
        ActionFailed,
        BudgetSettled,
    )
    assert loaded.state.invocations[0].status is InvocationStatus.FAILED
    assert loaded.state.actions[0].status is ActionStatus.FAILED
    assert loaded.state.budget.resources[0].reserved == 0
    assert loaded.state.budget.resources[0].consumed == 2
    counts = (clock.calls, ids.calls, len(verifier.calls), len(loaded.events))

    duplicate = await engine.handle(command)

    assert duplicate == first
    reloaded = await repository.load(ATTEMPT_ID)
    assert reloaded is not None
    assert (clock.calls, ids.calls, len(verifier.calls), len(reloaded.events)) == counts


@pytest.mark.asyncio
async def test_finish_rejects_unknown_outcome_while_reservation_is_held() -> None:
    engine, repository, clock, ids, verifier = _engine()
    action_id = await _start_invocation(engine, repository)
    unknown = ErrorSummary(
        code="OBSERVATION_LOST",
        retryable=False,
        safe_message="The external outcome could not be observed.",
    )
    await _commit_legacy_bare_unknown(
        repository,
        action_id=action_id,
        error=unknown,
    )
    terminal_error = ErrorSummary(
        code="STRATEGY_FAILED",
        retryable=False,
        safe_message="The strategy could not resolve the Action outcome.",
    )
    terminal = await engine.handle(
        _decision_command(
            command_id=_uuid(190),
            expected_revision=10,
            trigger_sequence_no=10,
            directive=StrategyDirective.FAIL,
            error=terminal_error,
        )
    )
    assert terminal.accepted is True
    before = (
        clock.calls,
        ids.calls,
        len(verifier.calls),
        len(await repository.list_events(ATTEMPT_ID)),
    )

    result = await engine.handle(
        FinishAttempt(
            schema_version=1,
            command_type="FINISH_ATTEMPT",
            command_id=FINISH_COMMAND_ID,
            attempt_id=ATTEMPT_ID,
            expected_revision=11,
            error=terminal_error,
        )
    )

    assert result.accepted is False
    assert result.error is not None
    assert result.error.code == "UNRESOLVED_ACTION_OUTCOME"
    assert (
        clock.calls,
        ids.calls,
        len(verifier.calls),
        len(await repository.list_events(ATTEMPT_ID)),
    ) == before


@pytest.mark.parametrize("terminal_kind", ["succeeded", "interrupted"])
@pytest.mark.asyncio
async def test_repository_rejects_terminal_attempt_with_open_reservation(
    terminal_kind: str,
) -> None:
    engine, repository, _, _, _ = _engine()
    action_id = await _start_invocation(engine, repository)
    unknown = ErrorSummary(
        code="OBSERVATION_LOST",
        retryable=False,
        safe_message="The external outcome could not be observed.",
    )
    await _commit_legacy_bare_unknown(
        repository,
        action_id=action_id,
        error=unknown,
    )
    loaded = await repository.load(ATTEMPT_ID)
    assert loaded is not None
    command_id = _uuid(186 if terminal_kind == "succeeded" else 187)
    sequence_no = loaded.state.revision + 1
    result_ref = _artifact("attempt-result")
    common = {
        "schema_version": 1,
        "event_id": _uuid(188 if terminal_kind == "succeeded" else 189),
        "trial_id": loaded.state.trial_id,
        "attempt_id": ATTEMPT_ID,
        "sequence_no": sequence_no,
        "command_id": command_id,
        "causal_parent_id": loaded.latest_event_id,
        "logical_time": sequence_no,
        "wall_time_utc": UTC_NOW,
    }
    if terminal_kind == "succeeded":
        event = AttemptSucceeded(
            **common,
            event_type="ATTEMPT_SUCCEEDED",
            result_ref=result_ref,
        )
        phase = AttemptPhase.SUCCEEDED
        registrations = (ArtifactRegistration(ref=result_ref),)
    else:
        event = AttemptInterrupted(
            **common,
            event_type="ATTEMPT_INTERRUPTED",
            error=unknown,
        )
        phase = AttemptPhase.INTERRUPTED
        registrations = ()
    request = CommitRequest(
        command_id=command_id,
        request_hash=sha256(f"terminal-{terminal_kind}".encode()).hexdigest(),
        attempt_id=ATTEMPT_ID,
        expected_revision=loaded.state.revision,
        events=(event,),
        outbox_actions=(),
        artifact_registrations=registrations,
        completed_delivery_action_ids=(),
        result=CommandResult(
            command_id=command_id,
            attempt_id=ATTEMPT_ID,
            accepted=True,
            revision=sequence_no,
            phase=phase,
        ),
        checkpoint=True,
    )

    with pytest.raises(StateTransitionError) as caught:
        await repository.commit(request)
    assert caught.value.code == "invalid_budget_transition"

    reloaded = await repository.load(ATTEMPT_ID)
    assert reloaded is not None
    assert reloaded.events == loaded.events
    assert command_id not in {
        record.command_id for record in reloaded.command_records
    }
    assert reloaded.state.phase is AttemptPhase.RUNNING
    assert reloaded.state.actions[0].status is ActionStatus.OUTCOME_UNKNOWN


@pytest.mark.asyncio
async def test_duplicate_stale_and_command_reuse_reject_before_metadata() -> None:
    engine, repository, clock, ids, verifier = _engine()
    create = _create_command()
    created = await engine.handle(create)
    assert created.accepted is True
    counts = (clock.calls, ids.calls, len(verifier.calls))

    duplicate = await engine.handle(create)
    conflict = await engine.handle(
        _create_command(experiment_id="different-experiment")
    )
    stale = await engine.handle(_start_command(expected_revision=2))

    assert duplicate == created
    assert conflict.accepted is False
    assert conflict.error is not None
    assert conflict.error.code == "COMMAND_REUSE"
    assert stale.accepted is False
    assert stale.error is not None
    assert stale.error.code == "REVISION_CONFLICT"
    assert (clock.calls, ids.calls, len(verifier.calls)) == counts
    assert len(await repository.list_events(ATTEMPT_ID)) == 1

    stale_duplicate = await engine.handle(_start_command(expected_revision=2))
    assert stale_duplicate == stale
    valid_start = await engine.handle(_start_command())
    assert valid_start.accepted is True
    after_start = (
        clock.calls,
        ids.calls,
        len(verifier.calls),
        len(await repository.list_events(ATTEMPT_ID)),
    )

    stale_reuse = await engine.handle(_start_command(expected_revision=2))
    cross_type_reuse = await engine.handle(
        FinishAttempt(
            schema_version=1,
            command_type="FINISH_ATTEMPT",
            command_id=START_COMMAND_ID,
            attempt_id=ATTEMPT_ID,
            expected_revision=2,
            result_ref=_artifact("reused-command-result"),
        )
    )

    assert stale_reuse.accepted is False
    assert stale_reuse.error is not None
    assert stale_reuse.error.code == "COMMAND_REUSE"
    assert cross_type_reuse.accepted is False
    assert cross_type_reuse.error is not None
    assert cross_type_reuse.error.code == "COMMAND_REUSE"
    assert (
        clock.calls,
        ids.calls,
        len(verifier.calls),
        len(await repository.list_events(ATTEMPT_ID)),
    ) == after_start


@pytest.mark.asyncio
async def test_action_revision_conflict_allows_same_stable_command_id_retry() -> None:
    engine, repository, clock, ids, verifier = _engine()
    action_id = await _accept_invocation(engine)
    claimed = await repository.claim_action(
        worker_id="worker-revision",
        now_utc=UTC_NOW,
        lease_seconds=10,
    )
    assert claimed is not None
    assert claimed.action.action_id == action_id
    command_id = action_command_id(action_id, "started")
    stale = ReportActionStarted(
        schema_version=1,
        command_type="REPORT_ACTION_STARTED",
        command_id=command_id,
        attempt_id=ATTEMPT_ID,
        expected_revision=6,
        action_id=action_id,
    )
    before = (
        clock.calls,
        ids.calls,
        len(verifier.calls),
        len(await repository.list_events(ATTEMPT_ID)),
    )

    conflict = await engine.handle(
        stale,
        delivery_claim=claimed.delivery_claim(),
    )
    started = await engine.handle(
        stale.model_copy(update={"expected_revision": 7}),
        delivery_claim=claimed.delivery_claim(),
    )

    assert conflict.accepted is False
    assert conflict.error is not None
    assert conflict.error.code == "REVISION_CONFLICT"
    assert started.accepted is True
    assert started.revision == 9
    assert (
        clock.calls - before[0],
        ids.calls - before[1],
        len(verifier.calls) - before[2],
        len(await repository.list_events(ATTEMPT_ID)) - before[3],
    ) == (1, 2, 0, 2)


@pytest.mark.asyncio
async def test_missing_attempt_rejection_reserves_command_identity() -> None:
    engine, repository, clock, ids, verifier = _engine()
    command_id = _uuid(206)
    missing = StartAttempt(
        schema_version=1,
        command_type="START_ATTEMPT",
        command_id=command_id,
        attempt_id=ATTEMPT_ID,
        expected_revision=1,
    )

    first = await engine.handle(missing)
    reused = await engine.handle(
        _create_command(command_id=command_id)
    )
    duplicate = await engine.handle(missing)

    assert first.accepted is False
    assert first.error is not None
    assert first.error.code == "ATTEMPT_NOT_FOUND"
    assert duplicate == first
    assert reused.accepted is False
    assert reused.error is not None
    assert reused.error.code == "COMMAND_REUSE"
    assert (clock.calls, ids.calls, len(verifier.calls)) == (0, 0, 0)
    assert await repository.list_events(ATTEMPT_ID) == ()


@pytest.mark.asyncio
async def test_concurrent_duplicate_command_consumes_metadata_once() -> None:
    repository = YieldingLoadRepository()
    engine, _, clock, ids, verifier = _engine(repository=repository)
    command = _create_command()

    first, second = await asyncio.gather(
        engine.handle(command),
        engine.handle(command),
    )

    assert first == second
    assert first.accepted is True
    assert repository.peak_loads == 1
    assert clock.calls == 1
    assert ids.calls == 1
    assert verifier.calls == [command.manifest_ref]
    assert len(await repository.list_events(ATTEMPT_ID)) == 1


@pytest.mark.asyncio
async def test_concurrent_duplicate_across_engines_consumes_metadata_once() -> None:
    repository = YieldingLoadRepository()
    first_engine, _, first_clock, first_ids, first_verifier = _engine(
        repository=repository
    )
    second_engine, _, second_clock, second_ids, second_verifier = _engine(
        repository=repository
    )
    command = _create_command()

    first, second = await asyncio.gather(
        first_engine.handle(command),
        second_engine.handle(command),
    )

    assert first == second
    assert first.accepted is True
    assert repository.peak_loads == 1
    assert first_clock.calls + second_clock.calls == 1
    assert first_ids.calls + second_ids.calls == 1
    assert len(first_verifier.calls) + len(second_verifier.calls) == 1
    assert len(await repository.list_events(ATTEMPT_ID)) == 1


@pytest.mark.asyncio
async def test_concurrent_commands_for_different_attempts_are_not_globally_serialized() -> None:
    repository = YieldingLoadRepository()
    engine, _, clock, ids, verifier = _engine(repository=repository)
    first_command = _create_command(command_id=_uuid(180)).model_copy(
        update={"attempt_id": "attempt-a", "trial_id": "trial-a"}
    )
    second_command = _create_command(command_id=_uuid(181)).model_copy(
        update={"attempt_id": "attempt-b", "trial_id": "trial-b"}
    )

    first, second = await asyncio.gather(
        engine.handle(first_command),
        engine.handle(second_command),
    )

    assert first.accepted is True
    assert second.accepted is True
    assert repository.peak_loads == 2
    assert clock.calls == 2
    assert ids.calls == 2
    assert verifier.calls == [
        first_command.manifest_ref,
        second_command.manifest_ref,
    ]
    assert len(await repository.list_events("attempt-a")) == 1
    assert len(await repository.list_events("attempt-b")) == 1


@pytest.mark.asyncio
async def test_duplicate_strategy_action_ids_reject_before_metadata() -> None:
    engine, repository, clock, ids, verifier = _engine()
    await _start_attempt(engine)
    proposal = _proposal()
    duplicate = _decision_command().model_copy(
        update={"proposals": (proposal, proposal)}
    )
    before = (
        clock.calls,
        ids.calls,
        len(verifier.calls),
        len(await repository.list_events(ATTEMPT_ID)),
    )

    result = await engine.handle(duplicate)

    assert result.accepted is False
    assert result.error is not None
    assert result.error.code == "DUPLICATE_ACTION_ID"
    assert (
        clock.calls,
        ids.calls,
        len(verifier.calls),
        len(await repository.list_events(ATTEMPT_ID)),
    ) == before


@pytest.mark.parametrize(
    ("identity_kind", "command_id", "expected_code"),
    [
        pytest.param(
            "action",
            _uuid(182),
            "DUPLICATE_ACTION_ID",
            id="existing-action",
        ),
        pytest.param(
            "invocation",
            _uuid(183),
            "DUPLICATE_INVOCATION_ID",
            id="existing-invocation",
        ),
    ],
)
@pytest.mark.asyncio
async def test_strategy_identity_reuse_rejects_before_metadata(
    identity_kind: str,
    command_id: UUID,
    expected_code: str,
) -> None:
    engine, repository, clock, ids, verifier = _engine()
    existing_action_id = await _start_invocation(engine, repository)
    await engine.handle(
        ReportActionOutcome(
            schema_version=1,
            command_type="REPORT_ACTION_OUTCOME",
            command_id=OUTCOME_COMMAND_ID,
            attempt_id=ATTEMPT_ID,
            expected_revision=9,
            action_id=existing_action_id,
            outcome=ActionSucceededOutcome(
                status=ActionStatus.SUCCEEDED,
                action_id=existing_action_id,
                result_ref=_artifact("completed-action-result"),
            ),
        )
    )
    loaded = await repository.load(ATTEMPT_ID)
    assert loaded is not None
    proposal = _proposal(ordinal=1)
    if identity_kind == "action":
        proposal = proposal.model_copy(update={"action_id": existing_action_id})
    else:
        proposal = proposal.model_copy(
            update={"invocation_id": loaded.state.invocations[0].invocation_id}
        )
    command = _decision_command(proposal, command_id=command_id).model_copy(
        update={
            "expected_revision": 12,
            "trigger_sequence_no": 11,
            "strategy": _strategy(2),
        }
    )
    before = (
        clock.calls,
        ids.calls,
        len(verifier.calls),
        len(loaded.events),
    )

    result = await engine.handle(command)

    assert result.accepted is False
    assert result.error is not None
    assert result.error.code == expected_code
    reloaded = await repository.load(ATTEMPT_ID)
    assert reloaded is not None
    assert (
        clock.calls,
        ids.calls,
        len(verifier.calls),
        len(reloaded.events),
    ) == before


@pytest.mark.asyncio
async def test_duplicate_invocation_ids_in_batch_reject_before_metadata() -> None:
    engine, repository, clock, ids, verifier = _engine()
    await _start_attempt(engine)
    first = _proposal(ordinal=0, batch_id="batch-1")
    second = _proposal(ordinal=1, batch_id="batch-1").model_copy(
        update={"invocation_id": first.invocation_id}
    )
    command = _decision_command(first, command_id=_uuid(184)).model_copy(
        update={"proposals": (first, second)}
    )
    before = (
        clock.calls,
        ids.calls,
        len(verifier.calls),
        len(await repository.list_events(ATTEMPT_ID)),
    )

    result = await engine.handle(command)

    assert result.accepted is False
    assert result.error is not None
    assert result.error.code == "DUPLICATE_INVOCATION_ID"
    assert (
        clock.calls,
        ids.calls,
        len(verifier.calls),
        len(await repository.list_events(ATTEMPT_ID)),
    ) == before


@pytest.mark.asyncio
async def test_retry_may_reference_an_existing_action_id() -> None:
    engine, repository, _, _, _ = _engine()
    existing_action_id = await _start_invocation(engine, repository)
    await engine.handle(
        ReportActionOutcome(
            schema_version=1,
            command_type="REPORT_ACTION_OUTCOME",
            command_id=OUTCOME_COMMAND_ID,
            attempt_id=ATTEMPT_ID,
            expected_revision=9,
            action_id=existing_action_id,
            outcome=ActionSucceededOutcome(
                status=ActionStatus.SUCCEEDED,
                action_id=existing_action_id,
                result_ref=_artifact("completed-action-result"),
            ),
        )
    )
    retry = _proposal(ordinal=1).model_copy(
        update={"retry_of_action_id": existing_action_id}
    )
    command = _decision_command(retry, command_id=_uuid(185)).model_copy(
        update={
            "expected_revision": 12,
            "trigger_sequence_no": 11,
            "strategy": _strategy(2),
        }
    )

    result = await engine.handle(command)

    assert result.accepted is True
    loaded = await repository.load(ATTEMPT_ID)
    assert loaded is not None
    assert loaded.state.actions[-1].action_id == retry.action_id
    assert loaded.state.actions[-1].retry_of_action_id == existing_action_id


@pytest.mark.parametrize(
    "batch_ids",
    [
        pytest.param((None, None), id="missing"),
        pytest.param(("batch-a", "batch-b"), id="mixed"),
    ],
)
@pytest.mark.asyncio
async def test_parallel_decision_requires_one_explicit_batch_before_metadata(
    batch_ids: tuple[str | None, str | None],
) -> None:
    engine, repository, clock, ids, verifier = _engine()
    await _start_attempt(engine)
    first = _proposal(ordinal=0, batch_id=batch_ids[0])
    second = _proposal(ordinal=1, batch_id=batch_ids[1])
    command = _decision_command(first).model_copy(
        update={"proposals": (first, second)}
    )
    before = (
        clock.calls,
        ids.calls,
        len(verifier.calls),
        len(await repository.list_events(ATTEMPT_ID)),
    )

    result = await engine.handle(command)

    assert result.accepted is False
    assert result.error is not None
    assert result.error.code == "INVALID_ACTION_BATCH"
    assert (
        clock.calls,
        ids.calls,
        len(verifier.calls),
        len(await repository.list_events(ATTEMPT_ID)),
    ) == before


@pytest.mark.asyncio
async def test_parallel_decision_accepts_one_shared_batch_atomically() -> None:
    engine, repository, _, _, _ = _engine()
    await _start_attempt(engine)
    first = _proposal(ordinal=0, batch_id="batch-1")
    second = _proposal(ordinal=1, batch_id="batch-1")

    result = await engine.handle(
        _decision_command(first).model_copy(
            update={"proposals": (first, second)}
        )
    )

    assert result.accepted is True
    assert result.revision == 10
    loaded = await repository.load(ATTEMPT_ID)
    assert loaded is not None
    assert tuple(action.status for action in loaded.state.actions) == (
        ActionStatus.ACCEPTED,
        ActionStatus.ACCEPTED,
    )
    assert tuple(action.batch_id for action in loaded.state.actions) == (
        "batch-1",
        "batch-1",
    )
    reserved = next(
        event for event in loaded.events if isinstance(event, BudgetReserved)
    )
    assert tuple(entry.action_id for entry in reserved.reservations) == (
        first.action_id,
        second.action_id,
    )
    assert loaded.state.budget.resources[0].reserved == 4


@pytest.mark.asyncio
async def test_parallel_batch_rejects_every_member_when_topology_rejects_one() -> None:
    engine, repository, _, _, _ = _engine()
    await _start_attempt(engine)
    first = _proposal(ordinal=0, batch_id="batch-1")
    second = _proposal(ordinal=1, batch_id="batch-1").model_copy(
        update={"target_ids": ("worker-b",)}
    )

    result = await engine.handle(
        _decision_command(first).model_copy(
            update={"proposals": (first, second)}
        )
    )

    assert result.accepted is True
    loaded = await repository.load(ATTEMPT_ID)
    assert loaded is not None
    assert tuple(action.status for action in loaded.state.actions) == (
        ActionStatus.REJECTED,
        ActionStatus.REJECTED,
    )
    assert not any(isinstance(event, BudgetReserved) for event in loaded.events)
    assert loaded.state.budget.resources[0].reserved == 0
    assert (
        await repository.claim_action(
            worker_id="worker-1",
            now_utc=UTC_NOW,
            lease_seconds=10,
        )
        is None
    )


@pytest.mark.asyncio
async def test_artifact_failure_rejects_before_clock_uuid_and_any_event() -> None:
    manifest = _artifact("missing-manifest")
    verifier = RecordingArtifactVerifier(
        rejected_hashes=frozenset({manifest.content_hash or ""})
    )
    engine, repository, clock, ids, _ = _engine(verifier=verifier)

    command = _create_command(manifest_ref=manifest)
    result = await engine.handle(command)
    duplicate = await engine.handle(command)
    reused = await engine.handle(
        _create_command(
            command_id=command.command_id,
            experiment_id="different-experiment",
        )
    )

    assert result.accepted is False
    assert result.error is not None
    assert result.error.code == "ARTIFACT_VERIFICATION_FAILED"
    assert duplicate == result
    assert reused.accepted is False
    assert reused.error is not None
    assert reused.error.code == "COMMAND_REUSE"
    assert verifier.calls == [manifest]
    assert clock.calls == 0
    assert ids.calls == 0
    assert await repository.list_events(ATTEMPT_ID) == ()


@pytest.mark.asyncio
async def test_decision_artifact_failure_is_atomic_and_precedes_metadata() -> None:
    payload_ref = _artifact("missing-action-payload")
    verifier = RecordingArtifactVerifier(
        rejected_hashes=frozenset({payload_ref.content_hash or ""})
    )
    engine, repository, clock, ids, _ = _engine(verifier=verifier)
    await _start_attempt(engine)
    before = (clock.calls, ids.calls, len(await repository.list_events(ATTEMPT_ID)))

    result = await engine.handle(_decision_command(_proposal(payload_ref=payload_ref)))

    assert result.accepted is False
    assert result.error is not None
    assert result.error.code == "ARTIFACT_VERIFICATION_FAILED"
    assert (clock.calls, ids.calls, len(await repository.list_events(ATTEMPT_ID))) == before


@pytest.mark.asyncio
async def test_finish_requires_committed_terminal_decision_then_uses_exact_result() -> None:
    engine, repository, clock, ids, _ = _engine()
    action_id = await _accept_invocation(engine)
    result_ref = _artifact("attempt-result")
    premature = FinishAttempt(
        schema_version=1,
        command_type="FINISH_ATTEMPT",
        command_id=FINISH_COMMAND_ID,
        attempt_id=ATTEMPT_ID,
        expected_revision=7,
        result_ref=result_ref,
    )
    before = (clock.calls, ids.calls, len(await repository.list_events(ATTEMPT_ID)))

    rejected = await engine.handle(premature)

    assert rejected.accepted is False
    assert rejected.error is not None
    assert rejected.error.code == "STRATEGY_COMPLETION_REQUIRED"
    assert (clock.calls, ids.calls, len(await repository.list_events(ATTEMPT_ID))) == before

    claimed = await repository.claim_action(
        worker_id="worker-finish",
        now_utc=UTC_NOW,
        lease_seconds=10,
    )
    assert claimed is not None
    assert claimed.action.action_id == action_id

    await engine.handle(
        ReportActionStarted(
            schema_version=1,
            command_type="REPORT_ACTION_STARTED",
            command_id=START_ACTION_COMMAND_ID,
            attempt_id=ATTEMPT_ID,
            expected_revision=7,
            action_id=action_id,
        ),
        delivery_claim=claimed.delivery_claim(),
    )
    await engine.handle(
        ReportActionOutcome(
            schema_version=1,
            command_type="REPORT_ACTION_OUTCOME",
            command_id=OUTCOME_COMMAND_ID,
            attempt_id=ATTEMPT_ID,
            expected_revision=9,
            action_id=action_id,
            outcome=ActionSucceededOutcome(
                status=ActionStatus.SUCCEEDED,
                action_id=action_id,
                result_ref=_artifact("action-result"),
            ),
        )
    )
    assert await engine.handle(premature) == rejected
    terminal = await engine.handle(
        _decision_command(
            command_id=_uuid(191),
            expected_revision=12,
            trigger_sequence_no=11,
            directive=StrategyDirective.SUCCEED,
            result_ref=result_ref,
        )
    )
    assert terminal.accepted is True
    mismatch = await engine.handle(
        FinishAttempt(
            schema_version=1,
            command_type="FINISH_ATTEMPT",
            command_id=_uuid(192),
            attempt_id=ATTEMPT_ID,
            expected_revision=13,
            result_ref=_artifact("different-attempt-result"),
        )
    )
    assert mismatch.accepted is False
    assert mismatch.error is not None
    assert mismatch.error.code == "STRATEGY_COMPLETION_MISMATCH"
    finished = await engine.handle(
        premature.model_copy(
            update={
                "command_id": FINISH_AFTER_OUTCOME_COMMAND_ID,
                "expected_revision": 13,
            }
        )
    )

    assert finished.accepted is True
    assert finished.phase is AttemptPhase.SUCCEEDED
    loaded = await repository.load(ATTEMPT_ID)
    assert loaded is not None
    assert isinstance(loaded.events[-1], AttemptSucceeded)
    assert loaded.state.result_ref == result_ref


@pytest.mark.asyncio
async def test_finish_with_safe_error_commits_failed_attempt() -> None:
    engine, repository, _, _, _ = _engine()
    await _start_attempt(engine)
    error = ErrorSummary(
        code="STRATEGY_FAILED",
        retryable=False,
        safe_message="The strategy did not produce a result.",
    )

    decision = await engine.handle(
        _decision_command(
            command_id=_uuid(193),
            expected_revision=2,
            trigger_sequence_no=2,
            directive=StrategyDirective.FAIL,
            error=error,
        )
    )
    assert decision.accepted is True

    result = await engine.handle(
        FinishAttempt(
            schema_version=1,
            command_type="FINISH_ATTEMPT",
            command_id=FINISH_COMMAND_ID,
            attempt_id=ATTEMPT_ID,
            expected_revision=3,
            error=error,
        )
    )

    assert result.accepted is True
    assert result.phase is AttemptPhase.FAILED
    loaded = await repository.load(ATTEMPT_ID)
    assert loaded is not None
    assert isinstance(loaded.events[-1], AttemptFailed)
    assert loaded.state.terminal_error == error


@pytest.mark.asyncio
async def test_budget_release_reducer_checks_exact_counter_transition() -> None:
    engine, repository, _, _, _ = _engine()
    await _accept_invocation(engine)
    loaded = await repository.load(ATTEMPT_ID)
    assert loaded is not None
    reservation = _reservation_entry(loaded.state.actions[0])
    cancellation_error = ErrorSummary(
        code="DISPATCH_CANCELLED",
        retryable=False,
        safe_message="The Action was cancelled before dispatch.",
    )
    cancelled_event_id = _uuid(190)
    cancelled = apply_event(
        loaded.state,
        ActionCancelled(
            schema_version=1,
            event_id=cancelled_event_id,
            trial_id="trial-1",
            attempt_id=ATTEMPT_ID,
            sequence_no=8,
            event_type="ACTION_CANCELLED",
            command_id=FINISH_COMMAND_ID,
            causal_parent_id=loaded.latest_event_id,
            logical_time=8,
            wall_time_utc=UTC_NOW,
            action_id=loaded.state.actions[0].action_id,
            outcome=ActionCancelledOutcome(
                status=ActionStatus.CANCELLED,
                action_id=loaded.state.actions[0].action_id,
                error=cancellation_error,
            ),
        ),
    )
    release_event_id = _uuid(191)
    released = apply_event(
        cancelled,
        BudgetReleased(
            schema_version=1,
            event_id=release_event_id,
            trial_id="trial-1",
            attempt_id=ATTEMPT_ID,
            sequence_no=9,
            event_type="BUDGET_RELEASED",
            command_id=DECISION_COMMAND_ID,
            causal_parent_id=cancelled_event_id,
            logical_time=9,
            wall_time_utc=UTC_NOW,
            budget=_budget(),
            reservation=reservation,
        ),
    )

    assert released.budget == _budget()
    assert released.actions[0].status is ActionStatus.CANCELLED

    with pytest.raises(StateTransitionError) as caught:
        apply_event(
            cancelled,
            BudgetSettled(
                schema_version=1,
                event_id=_uuid(192),
                trial_id="trial-1",
                attempt_id=ATTEMPT_ID,
                sequence_no=9,
                event_type="BUDGET_SETTLED",
                command_id=DECISION_COMMAND_ID,
                causal_parent_id=cancelled_event_id,
                logical_time=9,
                wall_time_utc=UTC_NOW,
                budget=_budget(reserved=0, consumed=1),
                reservation=reservation,
            ),
        )
    assert caught.value.code == "invalid_budget_transition"


@pytest.mark.asyncio
async def test_unknown_outcome_cannot_release_held_reservation() -> None:
    engine, repository, _, _, _ = _engine()
    action_id = await _start_invocation(engine, repository)
    unknown = ErrorSummary(
        code="OBSERVATION_LOST",
        retryable=False,
        safe_message="The external outcome could not be observed.",
    )
    await _commit_legacy_bare_unknown(
        repository,
        action_id=action_id,
        error=unknown,
    )
    loaded = await repository.load(ATTEMPT_ID)
    assert loaded is not None
    assert loaded.state.budget.resources[0].reserved == 2

    with pytest.raises(StateTransitionError) as caught:
        apply_event(
            loaded.state,
            BudgetReleased(
                schema_version=1,
                event_id=_uuid(193),
                trial_id="trial-1",
                attempt_id=ATTEMPT_ID,
                sequence_no=11,
                event_type="BUDGET_RELEASED",
                command_id=FINISH_COMMAND_ID,
                causal_parent_id=loaded.latest_event_id,
                logical_time=11,
                wall_time_utc=UTC_NOW,
                budget=_budget(),
                reservation=_reservation_entry(loaded.state.actions[0]),
            ),
        )
    assert caught.value.code == "invalid_budget_transition"


@pytest.mark.parametrize("close_kind", ["settled", "released"])
@pytest.mark.asyncio
async def test_replay_rejects_duplicate_budget_close_for_same_reservation(
    close_kind: str,
) -> None:
    engine, repository, _, _, _ = _engine()
    await _start_attempt(engine)
    first = _proposal(ordinal=0, batch_id="batch-1")
    second = _proposal(ordinal=1, batch_id="batch-1")
    decision = _decision_command(first).model_copy(
        update={"proposals": (first, second)}
    )
    accepted = await engine.handle(decision)
    assert accepted.accepted is True
    assert accepted.revision == 10
    loaded = await repository.load(ATTEMPT_ID)
    assert loaded is not None
    reservation = _reservation_entry(loaded.state.actions[0])

    if close_kind == "released":
        cancellation_id = _uuid(194)
        cancellation_error = ErrorSummary(
            code="DISPATCH_CANCELLED",
            retryable=False,
            safe_message="The Action was cancelled before dispatch.",
        )
        cancellation = ActionCancelled(
            schema_version=1,
            event_id=cancellation_id,
            trial_id="trial-1",
            attempt_id=ATTEMPT_ID,
            sequence_no=11,
            event_type="ACTION_CANCELLED",
            command_id=FINISH_COMMAND_ID,
            causal_parent_id=loaded.latest_event_id,
            logical_time=11,
            wall_time_utc=UTC_NOW,
            action_id=first.action_id,
            outcome=ActionCancelledOutcome(
                status=ActionStatus.CANCELLED,
                action_id=first.action_id,
                error=cancellation_error,
            ),
        )
        first_close_id = _uuid(195)
        first_close = BudgetReleased(
            schema_version=1,
            event_id=first_close_id,
            trial_id="trial-1",
            attempt_id=ATTEMPT_ID,
            sequence_no=12,
            event_type="BUDGET_RELEASED",
            command_id=FINISH_COMMAND_ID,
            causal_parent_id=cancellation_id,
            logical_time=12,
            wall_time_utc=UTC_NOW,
            budget=_budget(reserved=2),
            reservation=reservation,
        )
        second_close = BudgetReleased(
            schema_version=1,
            event_id=_uuid(197),
            trial_id="trial-1",
            attempt_id=ATTEMPT_ID,
            sequence_no=13,
            event_type="BUDGET_RELEASED",
            command_id=FINISH_COMMAND_ID,
            causal_parent_id=first_close_id,
            logical_time=13,
            wall_time_utc=UTC_NOW,
            budget=_budget(),
            reservation=reservation,
        )
        stream = loaded.events + (cancellation, first_close, second_close)
    else:
        claimed = await repository.claim_action(
            worker_id="worker-settle",
            now_utc=UTC_NOW,
            lease_seconds=10,
        )
        assert claimed is not None
        assert claimed.action.action_id == first.action_id
        await engine.handle(
            ReportActionStarted(
                schema_version=1,
                command_type="REPORT_ACTION_STARTED",
                command_id=START_ACTION_COMMAND_ID,
                attempt_id=ATTEMPT_ID,
                expected_revision=10,
                action_id=first.action_id,
            ),
            delivery_claim=claimed.delivery_claim(),
        )
        await engine.handle(
            ReportActionOutcome(
                schema_version=1,
                command_type="REPORT_ACTION_OUTCOME",
                command_id=OUTCOME_COMMAND_ID,
                attempt_id=ATTEMPT_ID,
                expected_revision=12,
                action_id=first.action_id,
                outcome=ActionSucceededOutcome(
                    status=ActionStatus.SUCCEEDED,
                    action_id=first.action_id,
                    result_ref=_artifact("action-result"),
                ),
            )
        )
        completed = await repository.load(ATTEMPT_ID)
        assert completed is not None
        original_close = completed.events[-1]
        assert isinstance(original_close, BudgetSettled)
        second_close = BudgetSettled(
            schema_version=1,
            event_id=_uuid(196),
            trial_id="trial-1",
            attempt_id=ATTEMPT_ID,
            sequence_no=16,
            event_type="BUDGET_SETTLED",
            command_id=FINISH_COMMAND_ID,
            causal_parent_id=completed.latest_event_id,
            logical_time=16,
            wall_time_utc=UTC_NOW,
            budget=_budget(consumed=4),
            reservation=reservation,
        )
        stream = completed.events + (second_close,)

    with pytest.raises(StateTransitionError) as caught:
        replay_events(stream)
    assert caught.value.code == "invalid_budget_transition"


@pytest.mark.asyncio
async def test_programming_errors_are_not_wrapped_as_command_rejections() -> None:
    class BuggyVerifier(RecordingArtifactVerifier):
        def verify(self, ref: ArtifactRef) -> None:
            raise RuntimeError("programming defect")

    engine, repository, _, _, _ = _engine(verifier=BuggyVerifier())

    with pytest.raises(RuntimeError, match="programming defect"):
        await engine.handle(_create_command())

    assert await repository.list_events(ATTEMPT_ID) == ()
