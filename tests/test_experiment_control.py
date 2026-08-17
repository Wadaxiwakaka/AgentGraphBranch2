from __future__ import annotations

from datetime import datetime, timedelta, timezone
from pathlib import Path
from uuid import NAMESPACE_URL, UUID, uuid5
import sqlite3

import pytest

from experiment_system.actions import (
    ActionFailedOutcome,
    ActionSucceededOutcome,
    ActionType,
    ActionUnknownOutcome,
    ExternalInputRequirement,
)
from experiment_system.commands import (
    ApplyStrategyDecision,
    CancelAttempt,
    ExpireAttempt,
    FinishAttempt,
    PauseAttempt,
    ReportActionOutcome,
    ReportActionStarted,
    ResumeAttempt,
    SubmitExternalInput,
    outcome_reconciliation_request_id,
)
from experiment_system.contract import StrategyDirective
from experiment_system.engine import AttemptEngine
from experiment_system.events import (
    ActionOutcomeReconciled,
    BudgetSettled,
    BudgetUncertainSettled,
)
from experiment_system.reducer import StateTransitionError, replay_events
from experiment_system.state import (
    ActionStatus,
    AttemptPhase,
    ArtifactRef,
    ErrorSummary,
    ExternalRequestKind,
    ExternalResponseKind,
    ResourceKind,
    ResourceRequest,
)
from experiment_system.store import AttemptRepository
from experiment_system.stores import (
    InMemoryAttemptRepository,
    SQLiteAttemptRepository,
)
from tests.test_experiment_engine import (
    ATTEMPT_ID,
    QueueIdFactory,
    RecordingArtifactVerifier,
    UTC_NOW,
    _artifact,
    _create_command,
    _decision_command,
    _proposal,
    _start_command,
)


DEADLINE = UTC_NOW + timedelta(hours=1)


class MutableClock:
    def __init__(self, value: datetime = UTC_NOW) -> None:
        self.value = value

    def now_utc(self) -> datetime:
        return self.value


@pytest.fixture(params=("memory", "sqlite"))
def repository(
    request: pytest.FixtureRequest,
    tmp_path: Path,
) -> AttemptRepository:
    if request.param == "memory":
        return InMemoryAttemptRepository()
    return SQLiteAttemptRepository(tmp_path / "control.sqlite3")


def _engine(
    repository: AttemptRepository,
    *,
    clock: MutableClock | None = None,
    skip_ids: int = 0,
    id_factory: QueueIdFactory | None = None,
    artifact_verifier: RecordingArtifactVerifier | None = None,
    allowed_edges: frozenset[tuple[str, str]] | None = None,
) -> AttemptEngine:
    ids = id_factory or QueueIdFactory()
    for _ in range(skip_ids):
        ids.new_uuid()
    return AttemptEngine(
        repository=repository,
        clock=clock or MutableClock(),
        id_factory=ids,
        artifact_verifier=artifact_verifier or RecordingArtifactVerifier(),
        allowed_edges=(
            frozenset({("engine", "worker-a")})
            if allowed_edges is None
            else allowed_edges
        ),
    )


async def _start_attempt(engine: AttemptEngine) -> None:
    assert (await engine.handle(_create_command())).accepted is True
    assert (await engine.handle(_start_command())).accepted is True


async def _accept_actions(
    engine: AttemptEngine,
    repository: AttemptRepository,
    *,
    count: int,
) -> tuple[str, ...]:
    await _start_attempt(engine)
    proposals = tuple(
        _proposal(
            ordinal=index,
            batch_id="control-batch" if count > 1 else None,
        )
        for index in range(count)
    )
    command = _decision_command(proposals[0]).model_copy(
        update={"proposals": proposals}
    )
    assert (await engine.handle(command)).accepted is True
    loaded = await repository.load(ATTEMPT_ID)
    assert loaded is not None
    return tuple(action.action_id for action in loaded.state.actions)


async def _start_first_action(
    engine: AttemptEngine,
    repository: AttemptRepository,
) -> str:
    loaded = await repository.load(ATTEMPT_ID)
    assert loaded is not None
    claim = await repository.claim_action(
        worker_id="control-worker",
        now_utc=UTC_NOW,
        lease_seconds=10,
    )
    assert claim is not None
    result = await engine.handle(
        ReportActionStarted(
            schema_version=1,
            command_type="REPORT_ACTION_STARTED",
            command_id=UUID("20000000-0000-0000-0000-000000000001"),
            attempt_id=ATTEMPT_ID,
            expected_revision=loaded.state.revision,
            action_id=claim.action.action_id,
        ),
        delivery_claim=claim.delivery_claim(),
    )
    assert result.accepted is True
    return claim.action.action_id


async def _start_next_action(
    engine: AttemptEngine,
    repository: AttemptRepository,
    *,
    suffix: int,
) -> str:
    claim = await repository.claim_action(
        worker_id=f"control-worker-{suffix}",
        now_utc=UTC_NOW,
        lease_seconds=10,
    )
    assert claim is not None
    loaded = await repository.load(ATTEMPT_ID)
    assert loaded is not None
    result = await engine.handle(
        ReportActionStarted(
            schema_version=1,
            command_type="REPORT_ACTION_STARTED",
            command_id=UUID(f"21000000-0000-0000-0000-{suffix:012d}"),
            attempt_id=ATTEMPT_ID,
            expected_revision=loaded.state.revision,
            action_id=claim.action.action_id,
        ),
        delivery_claim=claim.delivery_claim(),
    )
    assert result.accepted is True
    return claim.action.action_id


def _pause(expected_revision: int, *, suffix: int = 1) -> PauseAttempt:
    return PauseAttempt(
        schema_version=1,
        command_type="PAUSE_ATTEMPT",
        command_id=UUID(f"30000000-0000-0000-0000-{suffix:012d}"),
        attempt_id=ATTEMPT_ID,
        expected_revision=expected_revision,
    )


def _resume(expected_revision: int, *, suffix: int = 1) -> ResumeAttempt:
    return ResumeAttempt(
        schema_version=1,
        command_type="RESUME_ATTEMPT",
        command_id=UUID(f"40000000-0000-0000-0000-{suffix:012d}"),
        attempt_id=ATTEMPT_ID,
        expected_revision=expected_revision,
    )


def _cancel(expected_revision: int, *, suffix: int = 1) -> CancelAttempt:
    return CancelAttempt(
        schema_version=1,
        command_type="CANCEL_ATTEMPT",
        command_id=UUID(f"50000000-0000-0000-0000-{suffix:012d}"),
        attempt_id=ATTEMPT_ID,
        expected_revision=expected_revision,
    )


def _expire(
    expected_revision: int,
    *,
    deadline_at: datetime = DEADLINE,
    suffix: int = 1,
) -> ExpireAttempt:
    return ExpireAttempt(
        schema_version=1,
        command_type="EXPIRE_ATTEMPT",
        command_id=UUID(f"60000000-0000-0000-0000-{suffix:012d}"),
        attempt_id=ATTEMPT_ID,
        expected_revision=expected_revision,
        deadline_at=deadline_at,
    )


def _external_decision(
    *,
    request_id: str,
    request_kind: ExternalRequestKind,
    proposal: object | None,
    payload_ref: ArtifactRef | None = None,
) -> ApplyStrategyDecision:
    proposals = () if proposal is None else (proposal,)
    action_id = None if proposal is None else proposal.action_id
    base = _decision_command(_proposal())
    return ApplyStrategyDecision.model_validate(
        {
            **base.model_dump(mode="python"),
            "proposals": proposals,
            "external_requirements": (
                ExternalInputRequirement(
                    request_id=request_id,
                    request_kind=request_kind,
                    action_id=action_id,
                    payload_ref=payload_ref,
                ),
            ),
        }
    )


def _external_response(
    *,
    expected_revision: int,
    request_id: str,
    response_kind: ExternalResponseKind,
    suffix: int,
    response_ref: ArtifactRef | None = None,
    error: ErrorSummary | None = None,
) -> SubmitExternalInput:
    return SubmitExternalInput(
        schema_version=1,
        command_type="SUBMIT_EXTERNAL_INPUT",
        command_id=UUID(f"80000000-0000-0000-0000-{suffix:012d}"),
        attempt_id=ATTEMPT_ID,
        expected_revision=expected_revision,
        request_id=request_id,
        response_kind=response_kind,
        response_ref=response_ref,
        error=error,
    )


def _outbox_count(repository: AttemptRepository) -> int:
    if isinstance(repository, InMemoryAttemptRepository):
        return len(repository._outbox)
    assert isinstance(repository, SQLiteAttemptRepository)
    with sqlite3.connect(repository._database_path) as connection:
        return connection.execute("SELECT COUNT(*) FROM action_outbox").fetchone()[0]


@pytest.mark.asyncio
async def test_approval_waits_durably_then_accepts_exactly_once(
    repository: AttemptRepository,
) -> None:
    engine = _engine(repository)
    await _start_attempt(engine)
    proposal = _proposal()
    decision = _external_decision(
        request_id="approval-1",
        request_kind=ExternalRequestKind.ACTION_APPROVAL,
        proposal=proposal,
        payload_ref=_artifact("approval-request"),
    )

    waiting_result = await engine.handle(decision)
    waiting = await repository.load(ATTEMPT_ID)
    assert waiting is not None
    assert waiting_result.accepted is True
    assert waiting.state.phase is AttemptPhase.WAITING_EXTERNAL
    assert waiting.state.actions[0].status is ActionStatus.PROPOSED
    assert waiting.state.invocations == ()
    assert waiting.state.budget.resources[0].reserved == 0
    assert tuple(event.event_type for event in waiting.events[-3:]) == (
        "STRATEGY_DECISION_RECORDED",
        "ACTION_PROPOSED",
        "EXTERNAL_INPUT_REQUESTED",
    )
    assert waiting.checkpoint_revisions == (waiting.state.revision,)
    assert _outbox_count(repository) == 0

    approve = _external_response(
        expected_revision=waiting.state.revision,
        request_id="approval-1",
        response_kind=ExternalResponseKind.APPROVE,
        suffix=1,
    )
    approved_result = await engine.handle(approve)
    duplicate = await engine.handle(approve)
    approved = await repository.load(ATTEMPT_ID)
    assert approved is not None
    assert approved_result.accepted is True
    assert duplicate == approved_result
    assert approved.state.phase is AttemptPhase.RUNNING
    assert approved.state.pending_external == ()
    assert approved.state.actions[0].action_id == proposal.action_id
    assert approved.state.actions[0].status is ActionStatus.ACCEPTED
    assert approved.state.budget.resources[0].reserved == 2
    assert tuple(event.event_type for event in approved.events[-5:]) == (
        "EXTERNAL_INPUT_RECEIVED",
        "EXTERNAL_INPUT_APPROVED",
        "INVOCATION_REQUESTED",
        "BUDGET_RESERVED",
        "ACTION_ACCEPTED",
    )
    assert approved.checkpoint_revisions == (
        waiting.state.revision,
        approved.state.revision,
    )
    assert _outbox_count(repository) == 1


@pytest.mark.asyncio
async def test_sqlite_reopen_preserves_approval_request_then_accepts(
    tmp_path: Path,
) -> None:
    database_path = tmp_path / "external-approval-reopen.sqlite3"
    repository = SQLiteAttemptRepository(database_path)
    engine = _engine(repository)
    await _start_attempt(engine)
    proposal = _proposal()
    assert (
        await engine.handle(
            _external_decision(
                request_id="approval-reopen",
                request_kind=ExternalRequestKind.ACTION_APPROVAL,
                proposal=proposal,
                payload_ref=_artifact("approval-reopen-request"),
            )
        )
    ).accepted

    reopened = SQLiteAttemptRepository(database_path)
    restored = await reopened.load(ATTEMPT_ID)
    assert restored is not None
    assert restored.state.phase is AttemptPhase.WAITING_EXTERNAL
    assert restored.state.pending_external[0].request_id == "approval-reopen"
    assert restored.state.actions[0].status is ActionStatus.PROPOSED
    assert restored.checkpoint_revisions == (restored.state.revision,)
    reopened_engine = _engine(reopened, skip_ids=60)

    result = await reopened_engine.handle(
        _external_response(
            expected_revision=restored.state.revision,
            request_id="approval-reopen",
            response_kind=ExternalResponseKind.APPROVE,
            suffix=65,
        )
    )

    assert result.accepted is True
    accepted = await SQLiteAttemptRepository(database_path).load(ATTEMPT_ID)
    assert accepted is not None
    assert accepted.state.phase is AttemptPhase.RUNNING
    assert accepted.state.pending_external == ()
    assert accepted.state.actions[0].action_id == proposal.action_id
    assert accepted.state.actions[0].status is ActionStatus.ACCEPTED
    assert _outbox_count(reopened) == 1


@pytest.mark.asyncio
async def test_approval_rejection_never_reserves_or_creates_outbox(
    repository: AttemptRepository,
) -> None:
    engine = _engine(repository)
    await _start_attempt(engine)
    decision = _external_decision(
        request_id="approval-reject",
        request_kind=ExternalRequestKind.ACTION_APPROVAL,
        proposal=_proposal(),
    )
    assert (await engine.handle(decision)).accepted is True
    waiting = await repository.load(ATTEMPT_ID)
    assert waiting is not None

    rejected_result = await engine.handle(
        _external_response(
            expected_revision=waiting.state.revision,
            request_id="approval-reject",
            response_kind=ExternalResponseKind.REJECT,
            suffix=2,
        )
    )
    rejected = await repository.load(ATTEMPT_ID)
    assert rejected is not None
    assert rejected_result.accepted is True
    assert rejected.state.phase is AttemptPhase.RUNNING
    assert rejected.state.actions[0].status is ActionStatus.REJECTED
    assert rejected.state.invocations == ()
    assert rejected.state.budget.resources[0].reserved == 0
    assert tuple(event.event_type for event in rejected.events[-3:]) == (
        "EXTERNAL_INPUT_RECEIVED",
        "EXTERNAL_INPUT_REJECTED",
        "ACTION_REJECTED",
    )
    assert _outbox_count(repository) == 0


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("violation", "expected_code"),
    (
        ("request_id", "ILLEGAL_TRANSITION"),
        ("response_kind", "ILLEGAL_TRANSITION"),
        ("revision", "REVISION_CONFLICT"),
    ),
)
async def test_invalid_external_response_has_zero_transition_side_effects(
    repository: AttemptRepository,
    violation: str,
    expected_code: str,
) -> None:
    ids = QueueIdFactory()
    engine = _engine(repository, id_factory=ids)
    await _start_attempt(engine)
    assert (
        await engine.handle(
            _external_decision(
                request_id="input-invalid",
                request_kind=ExternalRequestKind.ADDITIONAL_INPUT,
                proposal=None,
            )
        )
    ).accepted
    waiting = await repository.load(ATTEMPT_ID)
    assert waiting is not None
    request_id = "input-other" if violation == "request_id" else "input-invalid"
    response_kind = (
        ExternalResponseKind.APPROVE
        if violation == "response_kind"
        else ExternalResponseKind.PROVIDE_INPUT
    )
    response_ref = (
        None
        if response_kind is ExternalResponseKind.APPROVE
        else _artifact("invalid-response")
    )
    expected_revision = (
        waiting.state.revision - 1
        if violation == "revision"
        else waiting.state.revision
    )
    command = _external_response(
        expected_revision=expected_revision,
        request_id=request_id,
        response_kind=response_kind,
        response_ref=response_ref,
        suffix={"request_id": 51, "response_kind": 52, "revision": 53}[
            violation
        ],
    )
    before = (
        waiting.events,
        waiting.state.revision,
        waiting.checkpoint_revisions,
        _outbox_count(repository),
        ids.calls,
    )

    result = await engine.handle(command)

    assert result.accepted is False
    assert result.error is not None
    assert result.error.code == expected_code
    reloaded = await repository.load(ATTEMPT_ID)
    assert reloaded is not None
    assert (
        reloaded.events,
        reloaded.state.revision,
        reloaded.checkpoint_revisions,
        _outbox_count(repository),
        ids.calls,
    ) == before


@pytest.mark.asyncio
@pytest.mark.parametrize("boundary", ("request", "response"))
async def test_external_artifact_verifier_failure_has_zero_transition_side_effects(
    repository: AttemptRepository,
    boundary: str,
) -> None:
    rejected_ref = _artifact(f"rejected-{boundary}")
    ids = QueueIdFactory()
    verifier = RecordingArtifactVerifier(
        rejected_hashes=frozenset({rejected_ref.content_hash})
    )
    engine = _engine(
        repository,
        id_factory=ids,
        artifact_verifier=verifier,
    )
    await _start_attempt(engine)
    if boundary == "request":
        command = _external_decision(
            request_id="verify-request",
            request_kind=ExternalRequestKind.ADDITIONAL_INPUT,
            proposal=None,
            payload_ref=rejected_ref,
        )
    else:
        assert (
            await engine.handle(
                _external_decision(
                    request_id="verify-response",
                    request_kind=ExternalRequestKind.ADDITIONAL_INPUT,
                    proposal=None,
                )
            )
        ).accepted
        waiting = await repository.load(ATTEMPT_ID)
        assert waiting is not None
        command = _external_response(
            expected_revision=waiting.state.revision,
            request_id="verify-response",
            response_kind=ExternalResponseKind.PROVIDE_INPUT,
            response_ref=rejected_ref,
            suffix=54,
        )
    before_loaded = await repository.load(ATTEMPT_ID)
    assert before_loaded is not None
    before = (
        before_loaded.events,
        before_loaded.state.revision,
        before_loaded.checkpoint_revisions,
        _outbox_count(repository),
        ids.calls,
    )

    result = await engine.handle(command)

    assert result.accepted is False
    assert result.error is not None
    assert result.error.code == "ARTIFACT_VERIFICATION_FAILED"
    reloaded = await repository.load(ATTEMPT_ID)
    assert reloaded is not None
    assert (
        reloaded.events,
        reloaded.state.revision,
        reloaded.checkpoint_revisions,
        _outbox_count(repository),
        ids.calls,
    ) == before


async def _enter_concurrency_guard_wait(
    repository: AttemptRepository,
    clock: MutableClock,
) -> tuple[AttemptEngine, object]:
    rejecting_engine = _engine(
        repository,
        clock=clock,
        allowed_edges=frozenset(),
    )
    await _start_attempt(rejecting_engine)
    rejected_proposals = tuple(
        _proposal(ordinal=ordinal, batch_id="guard-rejected-batch")
        for ordinal in range(2)
    )
    first_decision = _decision_command(rejected_proposals[0]).model_copy(
        update={"proposals": rejected_proposals}
    )
    assert (await rejecting_engine.handle(first_decision)).accepted
    rejected = await repository.load(ATTEMPT_ID)
    assert rejected is not None
    rejection_triggers = tuple(
        event
        for event in rejected.events
        if event.event_type == "ACTION_REJECTED"
    )
    assert len(rejection_triggers) == 2

    engine = _engine(repository, clock=clock, skip_ids=60)
    active_proposals = tuple(
        _proposal(ordinal=ordinal + 2, batch_id="guard-active-batch")
        for ordinal in range(2)
    )
    active_decision = ApplyStrategyDecision(
        schema_version=1,
        command_type="APPLY_STRATEGY_DECISION",
        command_id=UUID("83000000-0000-0000-0000-000000000001"),
        attempt_id=ATTEMPT_ID,
        expected_revision=rejected.state.revision,
        trigger_sequence_no=rejection_triggers[0].sequence_no,
        strategy=rejected.state.strategy,
        proposals=active_proposals,
        directive=StrategyDirective.CONTINUE,
    )
    assert (await engine.handle(active_decision)).accepted
    active = await repository.load(ATTEMPT_ID)
    assert active is not None
    assert sum(
        action.status is ActionStatus.ACCEPTED for action in active.state.actions
    ) == 2
    approval = _external_decision(
        request_id="approval-concurrency",
        request_kind=ExternalRequestKind.ACTION_APPROVAL,
        proposal=_proposal(ordinal=4),
    ).model_copy(
        update={
            "command_id": UUID("83000000-0000-0000-0000-000000000002"),
            "expected_revision": active.state.revision,
            "trigger_sequence_no": rejection_triggers[1].sequence_no,
            "strategy": active.state.strategy,
        }
    )
    assert (await engine.handle(approval)).accepted
    waiting = await repository.load(ATTEMPT_ID)
    assert waiting is not None
    return engine, waiting


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("guard", "expected_code"),
    (
        ("deadline", "ATTEMPT_DEADLINE_EXPIRED"),
        ("topology", "TOPOLOGY_EDGE_FORBIDDEN"),
        ("budget", "BUDGET_EXHAUSTED"),
        ("concurrency", "MAX_CONCURRENCY_EXCEEDED"),
    ),
)
async def test_approval_reruns_all_guards_before_acceptance(
    repository: AttemptRepository,
    guard: str,
    expected_code: str,
) -> None:
    clock = MutableClock()
    if guard == "concurrency":
        engine, waiting = await _enter_concurrency_guard_wait(repository, clock)
    else:
        engine = _engine(repository, clock=clock)
        await _start_attempt(engine)
        proposal = _proposal()
        if guard == "budget":
            proposal = proposal.model_copy(
                update={
                    "resource_requests": (
                        ResourceRequest(
                            resource=ResourceKind.MODEL_CALLS,
                            amount=11,
                        ),
                    )
                }
            )
        assert (
            await engine.handle(
                _external_decision(
                    request_id=f"approval-{guard}",
                    request_kind=ExternalRequestKind.ACTION_APPROVAL,
                    proposal=proposal,
                )
            )
        ).accepted
        waiting = await repository.load(ATTEMPT_ID)
        assert waiting is not None
        if guard == "topology":
            engine = _engine(
                repository,
                clock=clock,
                skip_ids=60,
                allowed_edges=frozenset(),
            )
        elif guard == "deadline":
            clock.value = DEADLINE
    baseline_outbox = _outbox_count(repository)
    request = waiting.state.pending_external[0]

    result = await engine.handle(
        _external_response(
            expected_revision=waiting.state.revision,
            request_id=request.request_id,
            response_kind=ExternalResponseKind.APPROVE,
            suffix={
                "deadline": 61,
                "topology": 62,
                "budget": 63,
                "concurrency": 64,
            }[guard],
        )
    )

    assert result.accepted is True
    loaded = await repository.load(ATTEMPT_ID)
    assert loaded is not None
    assert loaded.state.phase is AttemptPhase.RUNNING
    assert loaded.state.pending_external == ()
    gated = next(
        action
        for action in loaded.state.actions
        if action.action_id == request.action_id
    )
    assert gated.status is ActionStatus.REJECTED
    assert gated.reservation_id is None
    assert gated.idempotency_key is None
    assert gated.error is not None
    assert gated.error.code == expected_code
    assert tuple(event.event_type for event in loaded.events[-3:]) == (
        "EXTERNAL_INPUT_RECEIVED",
        "EXTERNAL_INPUT_APPROVED",
        "ACTION_REJECTED",
    )
    assert _outbox_count(repository) == baseline_outbox


@pytest.mark.asyncio
async def test_additional_input_is_one_registered_strategy_trigger(
    repository: AttemptRepository,
) -> None:
    engine = _engine(repository)
    await _start_attempt(engine)
    request_ref = _artifact("additional-request")
    response_ref = _artifact("additional-response")
    decision = _external_decision(
        request_id="input-1",
        request_kind=ExternalRequestKind.ADDITIONAL_INPUT,
        proposal=None,
        payload_ref=request_ref,
    )
    assert (await engine.handle(decision)).accepted is True
    waiting = await repository.load(ATTEMPT_ID)
    assert waiting is not None

    provided = await engine.handle(
        _external_response(
            expected_revision=waiting.state.revision,
            request_id="input-1",
            response_kind=ExternalResponseKind.PROVIDE_INPUT,
            response_ref=response_ref,
            suffix=3,
        )
    )
    received = await repository.load(ATTEMPT_ID)
    assert received is not None
    assert provided.accepted is True
    assert received.state.phase is AttemptPhase.RUNNING
    assert tuple(event.event_type for event in received.events[-2:]) == (
        "EXTERNAL_INPUT_RECEIVED",
        "EXTERNAL_INPUT_APPROVED",
    )
    assert {registration.ref for registration in received.artifact_registrations}.issuperset(
        {request_ref, response_ref}
    )

    received_event = received.events[-2]
    follow_up = ApplyStrategyDecision(
        schema_version=1,
        command_type="APPLY_STRATEGY_DECISION",
        command_id=UUID("80000000-0000-0000-0000-000000000004"),
        attempt_id=ATTEMPT_ID,
        expected_revision=received.state.revision,
        trigger_sequence_no=received_event.sequence_no,
        strategy=received.state.strategy,
        proposals=(),
        directive=StrategyDirective.CONTINUE,
    )
    consumed = await engine.handle(follow_up)
    assert consumed.accepted is True
    after = await repository.load(ATTEMPT_ID)
    assert after is not None
    assert sum(
        event.event_type == "STRATEGY_DECISION_RECORDED"
        and event.trigger_sequence_no == received_event.sequence_no
        for event in after.events
    ) == 1


@pytest.mark.asyncio
async def test_resolved_external_request_id_cannot_be_reused(
    repository: AttemptRepository,
) -> None:
    ids = QueueIdFactory()
    engine = _engine(repository, id_factory=ids)
    await _start_attempt(engine)
    first = _external_decision(
        request_id="input-reused",
        request_kind=ExternalRequestKind.ADDITIONAL_INPUT,
        proposal=None,
    )
    assert (await engine.handle(first)).accepted
    waiting = await repository.load(ATTEMPT_ID)
    assert waiting is not None
    assert (
        await engine.handle(
            _external_response(
                expected_revision=waiting.state.revision,
                request_id="input-reused",
                response_kind=ExternalResponseKind.PROVIDE_INPUT,
                response_ref=_artifact("input-reused-response"),
                suffix=35,
            )
        )
    ).accepted
    resolved = await repository.load(ATTEMPT_ID)
    assert resolved is not None
    trigger = resolved.events[-2]
    reused = _external_decision(
        request_id="input-reused",
        request_kind=ExternalRequestKind.ADDITIONAL_INPUT,
        proposal=None,
    ).model_copy(
        update={
            "command_id": UUID("82000000-0000-0000-0000-000000000035"),
            "expected_revision": resolved.state.revision,
            "trigger_sequence_no": trigger.sequence_no,
            "strategy": resolved.state.strategy,
        }
    )
    before = (
        resolved.events,
        resolved.checkpoint_revisions,
        _outbox_count(repository),
        ids.calls,
    )

    result = await engine.handle(reused)

    assert result.accepted is False
    assert result.error is not None
    assert result.error.code == "DUPLICATE_EXTERNAL_REQUEST_ID"
    reloaded = await repository.load(ATTEMPT_ID)
    assert reloaded is not None
    assert (
        reloaded.events,
        reloaded.checkpoint_revisions,
        _outbox_count(repository),
        ids.calls,
    ) == before


@pytest.mark.asyncio
@pytest.mark.parametrize("collision_origin", ("prior_request", "same_decision"))
async def test_proposal_admission_reserves_reconciliation_request_id(
    repository: AttemptRepository,
    collision_origin: str,
) -> None:
    ids = QueueIdFactory()
    engine = _engine(repository, id_factory=ids)
    await _start_attempt(engine)
    proposal = _proposal(ordinal=1)
    reconciliation_id = outcome_reconciliation_request_id(
        ATTEMPT_ID,
        proposal.action_id,
    )

    if collision_origin == "prior_request":
        assert (
            await engine.handle(
                _external_decision(
                    request_id=reconciliation_id,
                    request_kind=ExternalRequestKind.ADDITIONAL_INPUT,
                    proposal=None,
                )
            )
        ).accepted
        waiting = await repository.load(ATTEMPT_ID)
        assert waiting is not None
        assert (
            await engine.handle(
                _external_response(
                    expected_revision=waiting.state.revision,
                    request_id=reconciliation_id,
                    response_kind=ExternalResponseKind.PROVIDE_INPUT,
                    response_ref=_artifact("reserved-reconciliation-response"),
                    suffix=81,
                )
            )
        ).accepted
        ready = await repository.load(ATTEMPT_ID)
        assert ready is not None
        trigger = ready.events[-2]
        command = _decision_command(proposal).model_copy(
            update={
                "command_id": UUID("87000000-0000-0000-0000-000000000001"),
                "expected_revision": ready.state.revision,
                "trigger_sequence_no": trigger.sequence_no,
                "strategy": ready.state.strategy,
            }
        )
    else:
        ready = await repository.load(ATTEMPT_ID)
        assert ready is not None
        command = _external_decision(
            request_id=reconciliation_id,
            request_kind=ExternalRequestKind.ACTION_APPROVAL,
            proposal=proposal,
        )

    before = (
        ready.events,
        ready.state.revision,
        ready.checkpoint_revisions,
        _outbox_count(repository),
        ids.calls,
    )

    result = await engine.handle(command)

    assert result.accepted is False
    assert result.error is not None
    assert result.error.code == "DUPLICATE_EXTERNAL_REQUEST_ID"
    reloaded = await repository.load(ATTEMPT_ID)
    assert reloaded is not None
    assert (
        reloaded.events,
        reloaded.state.revision,
        reloaded.checkpoint_revisions,
        _outbox_count(repository),
        ids.calls,
    ) == before


@pytest.mark.asyncio
async def test_replay_rejects_request_id_consumed_before_proposal_admission(
    repository: AttemptRepository,
) -> None:
    engine = _engine(repository)
    await _start_attempt(engine)
    proposal = _proposal(ordinal=1)
    assert (
        await engine.handle(
            _external_decision(
                request_id="ordinary-prior-input",
                request_kind=ExternalRequestKind.ADDITIONAL_INPUT,
                proposal=None,
            )
        )
    ).accepted
    waiting = await repository.load(ATTEMPT_ID)
    assert waiting is not None
    assert (
        await engine.handle(
            _external_response(
                expected_revision=waiting.state.revision,
                request_id="ordinary-prior-input",
                response_kind=ExternalResponseKind.PROVIDE_INPUT,
                response_ref=_artifact("ordinary-prior-response"),
                suffix=88,
            )
        )
    ).accepted
    ready = await repository.load(ATTEMPT_ID)
    assert ready is not None
    trigger = ready.events[-2]
    assert (
        await engine.handle(
            _decision_command(proposal).model_copy(
                update={
                    "command_id": UUID(
                        "87000000-0000-0000-0000-000000000002"
                    ),
                    "expected_revision": ready.state.revision,
                    "trigger_sequence_no": trigger.sequence_no,
                    "strategy": ready.state.strategy,
                }
            )
        )
    ).accepted
    completed = await repository.load(ATTEMPT_ID)
    assert completed is not None
    reserved_id = outcome_reconciliation_request_id(
        ATTEMPT_ID,
        proposal.action_id,
    )
    tampered = []
    for event in completed.events:
        if (
            event.event_type == "STRATEGY_DECISION_RECORDED"
            and event.external_requirements
        ):
            requirement = event.external_requirements[0].model_copy(
                update={"request_id": reserved_id}
            )
            event = event.model_copy(
                update={"external_requirements": (requirement,)}
            )
        elif event.event_type == "EXTERNAL_INPUT_REQUESTED":
            event = event.model_copy(
                update={
                    "request": event.request.model_copy(
                        update={"request_id": reserved_id}
                    )
                }
            )
        elif event.event_type in {
            "EXTERNAL_INPUT_RECEIVED",
            "EXTERNAL_INPUT_APPROVED",
        }:
            event = event.model_copy(update={"request_id": reserved_id})
        tampered.append(event)

    with pytest.raises(StateTransitionError) as caught:
        replay_events(tampered)

    assert caught.value.code == "duplicate_external_request_id"


def _unknown_outcome(action_id: str) -> ActionUnknownOutcome:
    return ActionUnknownOutcome(
        status=ActionStatus.OUTCOME_UNKNOWN,
        action_id=action_id,
        error=ErrorSummary(
            code="OUTCOME_UNCERTAIN",
            retryable=False,
            safe_message="The Action outcome could not be observed safely.",
        ),
    )


async def _report_unknown(
    engine: AttemptEngine,
    repository: AttemptRepository,
    *,
    action_id: str,
    suffix: int,
) -> None:
    loaded = await repository.load(ATTEMPT_ID)
    assert loaded is not None
    result = await engine.handle(
        ReportActionOutcome(
            schema_version=1,
            command_type="REPORT_ACTION_OUTCOME",
            command_id=UUID(f"81000000-0000-0000-0000-{suffix:012d}"),
            attempt_id=ATTEMPT_ID,
            expected_revision=loaded.state.revision,
            action_id=action_id,
            outcome=_unknown_outcome(action_id),
        )
    )
    assert result.accepted is True


async def _enter_reconciliation_wait_with_sibling(
    engine: AttemptEngine,
    repository: AttemptRepository,
    *,
    sibling_started: bool,
) -> tuple[str, str]:
    action_ids = await _accept_actions(engine, repository, count=2)
    unknown_action_id = await _start_first_action(engine, repository)
    sibling_action_id = next(
        action_id for action_id in action_ids if action_id != unknown_action_id
    )
    if sibling_started:
        claim = await repository.claim_action(
            worker_id="sibling-worker",
            now_utc=UTC_NOW,
            lease_seconds=10,
        )
        assert claim is not None
        assert claim.action.action_id == sibling_action_id
        loaded = await repository.load(ATTEMPT_ID)
        assert loaded is not None
        started = await engine.handle(
            ReportActionStarted(
                schema_version=1,
                command_type="REPORT_ACTION_STARTED",
                command_id=UUID("84000000-0000-0000-0000-000000000001"),
                attempt_id=ATTEMPT_ID,
                expected_revision=loaded.state.revision,
                action_id=sibling_action_id,
            ),
            delivery_claim=claim.delivery_claim(),
        )
        assert started.accepted
    await _report_unknown(
        engine,
        repository,
        action_id=unknown_action_id,
        suffix=71,
    )
    waiting = await repository.load(ATTEMPT_ID)
    assert waiting is not None
    assert waiting.state.phase is AttemptPhase.WAITING_EXTERNAL
    return unknown_action_id, sibling_action_id


async def _enter_reconciliation_wait_with_two_siblings(
    engine: AttemptEngine,
    repository: AttemptRepository,
    *,
    started_siblings: int,
) -> tuple[str, tuple[str, str]]:
    create = _create_command()
    create = create.model_copy(
        update={
            "budget": create.budget.model_copy(
                update={"max_concurrent_actions": 3}
            )
        }
    )
    assert (await engine.handle(create)).accepted
    assert (await engine.handle(_start_command())).accepted
    proposals = tuple(
        _proposal(ordinal=index, batch_id="three-action-control")
        for index in range(3)
    )
    decision = _decision_command(proposals[0]).model_copy(
        update={"proposals": proposals}
    )
    assert (await engine.handle(decision)).accepted
    unknown_action_id = await _start_next_action(
        engine,
        repository,
        suffix=96,
    )
    started_ids = tuple(
        [
            await _start_next_action(
                engine,
                repository,
                suffix=97 + index,
            )
            for index in range(started_siblings)
        ]
    )
    loaded = await repository.load(ATTEMPT_ID)
    assert loaded is not None
    sibling_ids = tuple(
        action.action_id
        for action in loaded.state.actions
        if action.action_id != unknown_action_id
    )
    assert len(sibling_ids) == 2
    assert set(started_ids).issubset(set(sibling_ids))
    await _report_unknown(
        engine,
        repository,
        action_id=unknown_action_id,
        suffix=99,
    )
    waiting = await repository.load(ATTEMPT_ID)
    assert waiting is not None
    assert waiting.state.phase is AttemptPhase.WAITING_EXTERNAL
    return unknown_action_id, sibling_ids


@pytest.mark.asyncio
@pytest.mark.parametrize("requested_phase", ("running", "pause", "cancel"))
async def test_unknown_outcome_enters_deterministic_reconciliation_wait(
    repository: AttemptRepository,
    requested_phase: str,
) -> None:
    engine = _engine(repository)
    await _accept_actions(engine, repository, count=1)
    action_id = await _start_first_action(engine, repository)
    started = await repository.load(ATTEMPT_ID)
    assert started is not None
    if requested_phase == "pause":
        assert (await engine.handle(_pause(started.state.revision, suffix=31))).accepted
    elif requested_phase == "cancel":
        assert (await engine.handle(_cancel(started.state.revision, suffix=31))).accepted

    await _report_unknown(
        engine,
        repository,
        action_id=action_id,
        suffix={"running": 1, "pause": 2, "cancel": 3}[requested_phase],
    )
    waiting = await repository.load(ATTEMPT_ID)
    assert waiting is not None
    assert waiting.state.phase is AttemptPhase.WAITING_EXTERNAL
    assert waiting.state.actions[0].status is ActionStatus.OUTCOME_UNKNOWN
    assert waiting.state.budget.resources[0].reserved == 2
    assert waiting.state.budget.resources[0].consumed == 0
    assert len(waiting.state.pending_external) == 1
    request = waiting.state.pending_external[0]
    assert request.request_kind is ExternalRequestKind.OUTCOME_RECONCILIATION
    assert request.action_id == action_id
    namespace = uuid5(
        NAMESPACE_URL,
        "agentgraph:orchestration:outcome-reconciliation-request:v1",
    )
    name = f"{len(ATTEMPT_ID)}:{ATTEMPT_ID}{len(action_id)}:{action_id}"
    assert request.request_id == str(uuid5(namespace, name))
    assert tuple(event.event_type for event in waiting.events[-2:]) == (
        "ACTION_OUTCOME_UNKNOWN",
        "EXTERNAL_INPUT_REQUESTED",
    )
    assert waiting.checkpoint_revisions[-1] == waiting.state.revision
    assert _outbox_count(repository) == 0

    requested_event = waiting.events[-1]
    forged_request = requested_event.model_copy(
        update={
            "request": requested_event.request.model_copy(
                update={"request_id": "forged-reconciliation-id"}
            )
        }
    )
    with pytest.raises(StateTransitionError) as caught:
        replay_events((*waiting.events[:-1], forged_request))
    assert caught.value.code == "invalid_external_transition"


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("response_kind", "reconciled_status"),
    (
        (ExternalResponseKind.CONFIRM_SUCCEEDED, ActionStatus.SUCCEEDED),
        (ExternalResponseKind.CONFIRM_FAILED, ActionStatus.FAILED),
    ),
)
async def test_reconciliation_confirmation_settles_once_and_preserves_unknown(
    repository: AttemptRepository,
    response_kind: ExternalResponseKind,
    reconciled_status: ActionStatus,
) -> None:
    engine = _engine(repository)
    await _accept_actions(engine, repository, count=1)
    action_id = await _start_first_action(engine, repository)
    await _report_unknown(engine, repository, action_id=action_id, suffix=11)
    waiting = await repository.load(ATTEMPT_ID)
    assert waiting is not None
    request_id = waiting.state.pending_external[0].request_id
    result_ref = (
        _artifact("confirmed-success")
        if response_kind is ExternalResponseKind.CONFIRM_SUCCEEDED
        else None
    )
    error = (
        ErrorSummary(
            code="CONFIRMED_FAILED",
            retryable=False,
            safe_message="The Action was confirmed failed.",
        )
        if response_kind is ExternalResponseKind.CONFIRM_FAILED
        else None
    )
    response = _external_response(
        expected_revision=waiting.state.revision,
        request_id=request_id,
        response_kind=response_kind,
        response_ref=result_ref,
        error=error,
        suffix=12,
    )

    confirmed = await engine.handle(response)
    duplicate = await engine.handle(response)
    loaded = await repository.load(ATTEMPT_ID)
    assert loaded is not None
    assert confirmed.accepted is True
    assert duplicate == confirmed
    assert loaded.state.phase is AttemptPhase.RUNNING
    action = loaded.state.actions[0]
    assert action.status is ActionStatus.OUTCOME_UNKNOWN
    assert action.reconciled_status is reconciled_status
    assert action.reconciled_result_ref == result_ref
    assert action.reconciled_error == error
    assert loaded.state.budget.resources[0].reserved == 0
    assert loaded.state.budget.resources[0].consumed == 2
    invocation_event = (
        "INVOCATION_COMPLETED"
        if reconciled_status is ActionStatus.SUCCEEDED
        else "INVOCATION_FAILED"
    )
    assert tuple(event.event_type for event in loaded.events[-5:]) == (
        "EXTERNAL_INPUT_RECEIVED",
        "EXTERNAL_INPUT_APPROVED",
        invocation_event,
        "ACTION_OUTCOME_RECONCILED",
        "BUDGET_SETTLED",
    )
    assert sum(event.event_type == "BUDGET_SETTLED" for event in loaded.events) == 1

    reconciled_event = next(
        event
        for event in loaded.events
        if event.event_type == "ACTION_OUTCOME_RECONCILED"
    )
    if reconciled_status is ActionStatus.SUCCEEDED:
        forged_outcome = ActionSucceededOutcome(
            status=ActionStatus.SUCCEEDED,
            action_id=action_id,
            result_ref=_artifact("forged-confirmed-success"),
        )
    else:
        forged_outcome = ActionFailedOutcome(
            status=ActionStatus.FAILED,
            action_id=action_id,
            error=ErrorSummary(
                code="FORGED_CONFIRMED_FAILURE",
                retryable=False,
                safe_message="This failure was not in the external response.",
            ),
        )
    tampered_events = tuple(
        event.model_copy(update={"outcome": forged_outcome})
        if event.event_id == reconciled_event.event_id
        else event
        for event in loaded.events
    )
    with pytest.raises(StateTransitionError) as caught:
        replay_events(tampered_events)
    assert caught.value.code == "invalid_external_transition"

    invocation_index = next(
        index
        for index, event in enumerate(loaded.events)
        if event.event_type == invocation_event
    )
    without_invocation = list(loaded.events)
    without_invocation.pop(invocation_index)
    for index in range(invocation_index, len(without_invocation)):
        event = without_invocation[index]
        without_invocation[index] = event.model_copy(
            update={
                "sequence_no": event.sequence_no - 1,
                "logical_time": event.logical_time - 1,
                "causal_parent_id": without_invocation[index - 1].event_id,
            }
        )
    with pytest.raises(StateTransitionError) as caught:
        replay_events(without_invocation)
    assert caught.value.code == "invalid_external_transition"


@pytest.mark.asyncio
async def test_replay_rejects_standalone_reconciliation_after_legacy_bare_unknown(
    repository: AttemptRepository,
) -> None:
    engine = _engine(repository)
    await _accept_actions(engine, repository, count=1)
    action_id = await _start_first_action(engine, repository)
    await _report_unknown(engine, repository, action_id=action_id, suffix=101)
    waiting = await repository.load(ATTEMPT_ID)
    assert waiting is not None
    assert waiting.events[-2].event_type == "ACTION_OUTCOME_UNKNOWN"
    assert waiting.events[-1].event_type == "EXTERNAL_INPUT_REQUESTED"
    legacy_events = waiting.events[:-1]
    legacy_state = replay_events(legacy_events)
    action = legacy_state.actions[0]
    command_id = UUID("89000000-0000-0000-0000-000000000101")
    reconciled = ActionOutcomeReconciled(
        schema_version=1,
        event_type="ACTION_OUTCOME_RECONCILED",
        event_id=UUID("89000000-0000-0000-0000-000000000102"),
        trial_id=legacy_state.trial_id,
        attempt_id=ATTEMPT_ID,
        sequence_no=legacy_state.revision + 1,
        command_id=command_id,
        causal_parent_id=legacy_events[-1].event_id,
        logical_time=legacy_events[-1].logical_time + 1,
        wall_time_utc=UTC_NOW,
        action_id=action_id,
        outcome=ActionSucceededOutcome(
            status=ActionStatus.SUCCEEDED,
            action_id=action_id,
            result_ref=_artifact("forged-standalone-reconciliation"),
        ),
    )
    settled = BudgetSettled(
        schema_version=1,
        event_type="BUDGET_SETTLED",
        event_id=UUID("89000000-0000-0000-0000-000000000103"),
        trial_id=legacy_state.trial_id,
        attempt_id=ATTEMPT_ID,
        sequence_no=legacy_state.revision + 2,
        command_id=command_id,
        causal_parent_id=reconciled.event_id,
        logical_time=legacy_events[-1].logical_time + 2,
        wall_time_utc=UTC_NOW,
        budget=AttemptEngine._settled_budget(legacy_state.budget, action),
        reservation=AttemptEngine._reservation_entry(action),
    )

    with pytest.raises(StateTransitionError) as caught:
        replay_events((*legacy_events, reconciled, settled))

    assert caught.value.code == "invalid_external_transition"


@pytest.mark.asyncio
async def test_later_decision_cannot_poison_reconciliation_action_shape(
    repository: AttemptRepository,
) -> None:
    engine = _engine(repository)
    await _accept_actions(engine, repository, count=1)
    action_id = await _start_first_action(engine, repository)
    await _report_unknown(engine, repository, action_id=action_id, suffix=91)
    waiting = await repository.load(ATTEMPT_ID)
    assert waiting is not None
    assert (
        await engine.handle(
            _external_response(
                expected_revision=waiting.state.revision,
                request_id=waiting.state.pending_external[0].request_id,
                response_kind=ExternalResponseKind.CONFIRM_SUCCEEDED,
                response_ref=_artifact("shape-poison-result"),
                suffix=91,
            )
        )
    ).accepted
    confirmed = await repository.load(ATTEMPT_ID)
    assert confirmed is not None

    invocation_index = next(
        index
        for index, event in enumerate(confirmed.events)
        if event.event_type == "INVOCATION_COMPLETED"
    )
    without_invocation = list(confirmed.events)
    without_invocation.pop(invocation_index)
    for index in range(invocation_index, len(without_invocation)):
        event = without_invocation[index]
        without_invocation[index] = event.model_copy(
            update={
                "sequence_no": event.sequence_no - 1,
                "logical_time": event.logical_time - 1,
                "causal_parent_id": without_invocation[index - 1].event_id,
            }
        )
    reconciled = next(
        event
        for event in without_invocation
        if event.event_type == "ACTION_OUTCOME_RECONCILED"
    )
    poison_proposal = _proposal().model_copy(
        update={
            "action_id": action_id,
            "action_type": ActionType.SEND_MESSAGE,
            "invocation_id": None,
            "causal_parent_id": str(without_invocation[-1].event_id),
        }
    )
    source_decision = next(
        event
        for event in confirmed.events
        if event.event_type == "STRATEGY_DECISION_RECORDED"
    )
    later_decision = source_decision.model_copy(
        update={
            "event_id": UUID("89000000-0000-0000-0000-000000000091"),
            "attempt_id": "forged-future-attempt",
            "sequence_no": without_invocation[-1].sequence_no + 1,
            "command_id": UUID("89000000-0000-0000-0000-000000000092"),
            "causal_parent_id": without_invocation[-1].event_id,
            "logical_time": without_invocation[-1].logical_time + 1,
            "trigger_sequence_no": reconciled.sequence_no,
            "strategy": confirmed.state.strategy,
            "proposals": (poison_proposal,),
            "external_requirements": (),
            "directive": StrategyDirective.CONTINUE,
            "result_ref": None,
            "error": None,
        }
    )

    with pytest.raises(StateTransitionError) as caught:
        replay_events((*without_invocation, later_decision))

    assert caught.value.code == "invalid_external_transition"


@pytest.mark.asyncio
async def test_expiry_allows_reconciled_unknown_sibling_while_waiting(
    repository: AttemptRepository,
) -> None:
    clock = MutableClock()
    engine = _engine(repository, clock=clock)
    await _accept_actions(engine, repository, count=1)
    action_id = await _start_first_action(engine, repository)
    await _report_unknown(engine, repository, action_id=action_id, suffix=92)
    reconciliation_wait = await repository.load(ATTEMPT_ID)
    assert reconciliation_wait is not None
    assert (
        await engine.handle(
            _external_response(
                expected_revision=reconciliation_wait.state.revision,
                request_id=reconciliation_wait.state.pending_external[0].request_id,
                response_kind=ExternalResponseKind.CONFIRM_SUCCEEDED,
                response_ref=_artifact("reconciled-before-expiry"),
                suffix=92,
            )
        )
    ).accepted
    reconciled = await repository.load(ATTEMPT_ID)
    assert reconciled is not None
    trigger = next(
        event
        for event in reversed(reconciled.events)
        if event.event_type == "ACTION_OUTCOME_RECONCILED"
    )
    additional = _external_decision(
        request_id="input-after-reconciliation",
        request_kind=ExternalRequestKind.ADDITIONAL_INPUT,
        proposal=None,
    ).model_copy(
        update={
            "command_id": UUID("89000000-0000-0000-0000-000000000093"),
            "expected_revision": reconciled.state.revision,
            "trigger_sequence_no": trigger.sequence_no,
            "strategy": reconciled.state.strategy,
        }
    )
    assert (await engine.handle(additional)).accepted
    waiting = await repository.load(ATTEMPT_ID)
    assert waiting is not None
    clock.value = DEADLINE

    expired = await engine.handle(_expire(waiting.state.revision, suffix=92))

    assert expired.accepted is True
    assert expired.phase is AttemptPhase.TIMED_OUT
    completed = await repository.load(ATTEMPT_ID)
    assert completed is not None
    assert completed.state.pending_external == ()
    assert completed.state.actions[0].status is ActionStatus.OUTCOME_UNKNOWN
    assert completed.state.actions[0].reconciled_status is ActionStatus.SUCCEEDED
    assert tuple(event.event_type for event in completed.events[-2:]) == (
        "EXTERNAL_INPUT_EXPIRED",
        "ATTEMPT_TIMED_OUT",
    )


@pytest.mark.asyncio
async def test_confirmed_unknown_can_complete_attempt(
    repository: AttemptRepository,
) -> None:
    engine = _engine(repository)
    await _accept_actions(engine, repository, count=1)
    action_id = await _start_first_action(engine, repository)
    await _report_unknown(engine, repository, action_id=action_id, suffix=36)
    waiting = await repository.load(ATTEMPT_ID)
    assert waiting is not None
    assert (
        await engine.handle(
            _external_response(
                expected_revision=waiting.state.revision,
                request_id=waiting.state.pending_external[0].request_id,
                response_kind=ExternalResponseKind.CONFIRM_SUCCEEDED,
                response_ref=_artifact("finish-reconciled-action"),
                suffix=36,
            )
        )
    ).accepted
    reconciled = await repository.load(ATTEMPT_ID)
    assert reconciled is not None
    trigger = next(
        event
        for event in reversed(reconciled.events)
        if event.event_type == "ACTION_OUTCOME_RECONCILED"
    )
    result_ref = _artifact("finish-reconciled-attempt")
    terminal_decision = ApplyStrategyDecision(
        schema_version=1,
        command_type="APPLY_STRATEGY_DECISION",
        command_id=UUID("82000000-0000-0000-0000-000000000036"),
        attempt_id=ATTEMPT_ID,
        expected_revision=reconciled.state.revision,
        trigger_sequence_no=trigger.sequence_no,
        strategy=reconciled.state.strategy,
        proposals=(),
        directive=StrategyDirective.SUCCEED,
        result_ref=result_ref,
    )
    decision_result = await engine.handle(terminal_decision)
    assert decision_result.accepted

    finished = await engine.handle(
        FinishAttempt(
            schema_version=1,
            command_type="FINISH_ATTEMPT",
            command_id=UUID("82000000-0000-0000-0000-000000000037"),
            attempt_id=ATTEMPT_ID,
            expected_revision=decision_result.revision,
            result_ref=result_ref,
        )
    )

    assert finished.accepted is True
    assert finished.phase is AttemptPhase.SUCCEEDED
    loaded = await repository.load(ATTEMPT_ID)
    assert loaded is not None
    assert loaded.state.actions[0].status is ActionStatus.OUTCOME_UNKNOWN
    assert loaded.state.actions[0].reconciled_status is ActionStatus.SUCCEEDED


@pytest.mark.asyncio
async def test_reconciliation_abandon_conservatively_settles_and_interrupts(
    repository: AttemptRepository,
) -> None:
    engine = _engine(repository)
    await _accept_actions(engine, repository, count=1)
    action_id = await _start_first_action(engine, repository)
    await _report_unknown(engine, repository, action_id=action_id, suffix=21)
    waiting = await repository.load(ATTEMPT_ID)
    assert waiting is not None
    response = _external_response(
        expected_revision=waiting.state.revision,
        request_id=waiting.state.pending_external[0].request_id,
        response_kind=ExternalResponseKind.ABANDON,
        suffix=22,
    )

    abandoned = await engine.handle(response)
    duplicate = await engine.handle(response)
    loaded = await repository.load(ATTEMPT_ID)
    assert loaded is not None
    assert abandoned.accepted is True
    assert duplicate == abandoned
    assert loaded.state.phase is AttemptPhase.INTERRUPTED
    assert loaded.state.actions[0].status is ActionStatus.OUTCOME_UNKNOWN
    assert loaded.state.actions[0].reconciled_status is None
    assert loaded.state.budget.resources[0].reserved == 0
    assert loaded.state.budget.resources[0].consumed == 2
    assert tuple(event.event_type for event in loaded.events[-4:]) == (
        "EXTERNAL_INPUT_RECEIVED",
        "EXTERNAL_INPUT_REJECTED",
        "BUDGET_UNCERTAIN_SETTLED",
        "ATTEMPT_INTERRUPTED",
    )
    assert sum(
        event.event_type == "BUDGET_UNCERTAIN_SETTLED"
        for event in loaded.events
    ) == 1


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "transaction_kind",
    ("additional_input", "approval_rejection", "reconciliation_abandon"),
)
async def test_replay_rejects_truncated_external_transaction_batch(
    repository: AttemptRepository,
    transaction_kind: str,
) -> None:
    engine = _engine(repository)
    if transaction_kind == "additional_input":
        await _start_attempt(engine)
        assert (
            await engine.handle(
                _external_decision(
                    request_id="truncated-additional",
                    request_kind=ExternalRequestKind.ADDITIONAL_INPUT,
                    proposal=None,
                )
            )
        ).accepted
        waiting = await repository.load(ATTEMPT_ID)
        assert waiting is not None
        assert (
            await engine.handle(
                _external_response(
                    expected_revision=waiting.state.revision,
                    request_id="truncated-additional",
                    response_kind=ExternalResponseKind.PROVIDE_INPUT,
                    response_ref=_artifact("truncated-additional-response"),
                    suffix=82,
                )
            )
        ).accepted
    elif transaction_kind == "approval_rejection":
        await _start_attempt(engine)
        assert (
            await engine.handle(
                _external_decision(
                    request_id="truncated-approval",
                    request_kind=ExternalRequestKind.ACTION_APPROVAL,
                    proposal=_proposal(),
                )
            )
        ).accepted
        waiting = await repository.load(ATTEMPT_ID)
        assert waiting is not None
        assert (
            await engine.handle(
                _external_response(
                    expected_revision=waiting.state.revision,
                    request_id="truncated-approval",
                    response_kind=ExternalResponseKind.REJECT,
                    suffix=83,
                )
            )
        ).accepted
    else:
        await _accept_actions(engine, repository, count=1)
        action_id = await _start_first_action(engine, repository)
        await _report_unknown(engine, repository, action_id=action_id, suffix=84)
        waiting = await repository.load(ATTEMPT_ID)
        assert waiting is not None
        assert (
            await engine.handle(
                _external_response(
                    expected_revision=waiting.state.revision,
                    request_id=waiting.state.pending_external[0].request_id,
                    response_kind=ExternalResponseKind.ABANDON,
                    suffix=85,
                )
            )
        ).accepted
    completed = await repository.load(ATTEMPT_ID)
    assert completed is not None

    with pytest.raises(StateTransitionError) as caught:
        replay_events(completed.events[:-1])

    assert caught.value.code == "invalid_external_transition"


@pytest.mark.asyncio
@pytest.mark.parametrize("boundary_event", ("received", "expired"))
async def test_external_boundary_event_must_parent_immediately_prior_event(
    repository: AttemptRepository,
    boundary_event: str,
) -> None:
    engine = _engine(repository)
    await _start_attempt(engine)
    request_id = f"wrong-parent-{boundary_event}"
    assert (
        await engine.handle(
            _external_decision(
                request_id=request_id,
                request_kind=ExternalRequestKind.ADDITIONAL_INPUT,
                proposal=None,
            )
        )
    ).accepted
    waiting = await repository.load(ATTEMPT_ID)
    assert waiting is not None
    if boundary_event == "received":
        assert (
            await engine.handle(
                _external_response(
                    expected_revision=waiting.state.revision,
                    request_id=request_id,
                    response_kind=ExternalResponseKind.PROVIDE_INPUT,
                    response_ref=_artifact("wrong-parent-response"),
                    suffix=86,
                )
            )
        ).accepted
        event_type = "EXTERNAL_INPUT_RECEIVED"
    else:
        assert (
            await engine.handle(_cancel(waiting.state.revision, suffix=87))
        ).accepted
        event_type = "EXTERNAL_INPUT_EXPIRED"
    completed = await repository.load(ATTEMPT_ID)
    assert completed is not None
    boundary = next(
        event for event in completed.events if event.event_type == event_type
    )
    tampered = tuple(
        event.model_copy(
            update={"causal_parent_id": completed.events[0].event_id}
        )
        if event.event_id == boundary.event_id
        else event
        for event in completed.events
    )

    with pytest.raises(StateTransitionError) as caught:
        replay_events(tampered)

    assert caught.value.code == "invalid_external_transition"


@pytest.mark.asyncio
async def test_replay_rejects_noncontiguous_command_id_reuse(
    repository: AttemptRepository,
) -> None:
    engine = _engine(repository)
    await _start_attempt(engine)
    request_id = "noncontiguous-command-reuse"
    assert (
        await engine.handle(
            _external_decision(
                request_id=request_id,
                request_kind=ExternalRequestKind.ADDITIONAL_INPUT,
                proposal=None,
            )
        )
    ).accepted
    waiting = await repository.load(ATTEMPT_ID)
    assert waiting is not None
    response = _external_response(
        expected_revision=waiting.state.revision,
        request_id=request_id,
        response_kind=ExternalResponseKind.PROVIDE_INPUT,
        response_ref=_artifact("noncontiguous-command-response"),
        suffix=104,
    )
    assert (await engine.handle(response)).accepted
    completed = await repository.load(ATTEMPT_ID)
    assert completed is not None
    root_command_id = completed.events[0].command_id
    tampered = tuple(
        event.model_copy(update={"command_id": root_command_id})
        if event.command_id == response.command_id
        else event
        for event in completed.events
    )

    with pytest.raises(StateTransitionError) as caught:
        replay_events(tampered)

    assert caught.value.code == "duplicate_command_id"


@pytest.mark.asyncio
@pytest.mark.parametrize("authorization", ("bare", "approved"))
async def test_uncertain_settlement_requires_reconciliation_rejection_or_expiry(
    repository: AttemptRepository,
    authorization: str,
) -> None:
    engine = _engine(repository)
    await _accept_actions(engine, repository, count=1)
    action_id = await _start_first_action(engine, repository)
    await _report_unknown(engine, repository, action_id=action_id, suffix=72)
    waiting = await repository.load(ATTEMPT_ID)
    assert waiting is not None
    action = waiting.state.actions[0]
    if authorization == "bare":
        prefix = waiting.events[:-1]
        preceding = prefix[-1]
    else:
        assert (
            await engine.handle(
                _external_response(
                    expected_revision=waiting.state.revision,
                    request_id=waiting.state.pending_external[0].request_id,
                    response_kind=ExternalResponseKind.CONFIRM_SUCCEEDED,
                    response_ref=_artifact("forged-uncertain-confirmation"),
                    suffix=72,
                )
            )
        ).accepted
        confirmed = await repository.load(ATTEMPT_ID)
        assert confirmed is not None
        approved_index = next(
            index
            for index, event in enumerate(confirmed.events)
            if event.event_type == "EXTERNAL_INPUT_APPROVED"
        )
        prefix = confirmed.events[: approved_index + 1]
        preceding = prefix[-1]
    forged = BudgetUncertainSettled(
        schema_version=1,
        event_type="BUDGET_UNCERTAIN_SETTLED",
        event_id=UUID(
            "85000000-0000-0000-0000-000000000001"
            if authorization == "bare"
            else "85000000-0000-0000-0000-000000000002"
        ),
        trial_id=waiting.state.trial_id,
        attempt_id=ATTEMPT_ID,
        sequence_no=preceding.sequence_no + 1,
        command_id=preceding.command_id,
        causal_parent_id=preceding.event_id,
        logical_time=preceding.logical_time + 1,
        wall_time_utc=preceding.wall_time_utc,
        budget=AttemptEngine._settled_budget(waiting.state.budget, action),
        reservation=AttemptEngine._reservation_entry(action),
    )

    with pytest.raises(StateTransitionError) as caught:
        replay_events((*prefix, forged))

    assert caught.value.code == "invalid_budget_transition"


@pytest.mark.asyncio
async def test_reconciliation_abandon_cancels_accepted_sibling_and_interrupts(
    repository: AttemptRepository,
) -> None:
    engine = _engine(repository)
    unknown_action_id, sibling_action_id = await _enter_reconciliation_wait_with_sibling(
        engine,
        repository,
        sibling_started=False,
    )
    waiting = await repository.load(ATTEMPT_ID)
    assert waiting is not None
    command = _external_response(
        expected_revision=waiting.state.revision,
        request_id=waiting.state.pending_external[0].request_id,
        response_kind=ExternalResponseKind.ABANDON,
        suffix=74,
    )

    result = await engine.handle(command)

    assert result.accepted is True
    assert result.phase is AttemptPhase.INTERRUPTED
    loaded = await repository.load(ATTEMPT_ID)
    assert loaded is not None
    actions = {action.action_id: action for action in loaded.state.actions}
    assert actions[unknown_action_id].status is ActionStatus.OUTCOME_UNKNOWN
    assert actions[sibling_action_id].status is ActionStatus.CANCELLED
    assert loaded.state.budget.resources[0].reserved == 0
    assert loaded.state.budget.resources[0].consumed == 2
    assert tuple(event.event_type for event in loaded.events[-6:]) == (
        "EXTERNAL_INPUT_RECEIVED",
        "EXTERNAL_INPUT_REJECTED",
        "BUDGET_UNCERTAIN_SETTLED",
        "ACTION_CANCELLED",
        "BUDGET_RELEASED",
        "ATTEMPT_INTERRUPTED",
    )
    assert _outbox_count(repository) == 0


@pytest.mark.asyncio
async def test_reconciliation_abandon_rejects_started_sibling_without_events(
    repository: AttemptRepository,
) -> None:
    ids = QueueIdFactory()
    engine = _engine(repository, id_factory=ids)
    await _enter_reconciliation_wait_with_sibling(
        engine,
        repository,
        sibling_started=True,
    )
    waiting = await repository.load(ATTEMPT_ID)
    assert waiting is not None
    command = _external_response(
        expected_revision=waiting.state.revision,
        request_id=waiting.state.pending_external[0].request_id,
        response_kind=ExternalResponseKind.ABANDON,
        suffix=73,
    )
    before = (
        waiting.events,
        waiting.state.revision,
        waiting.checkpoint_revisions,
        _outbox_count(repository),
        ids.calls,
    )

    result = await engine.handle(command)

    assert result.accepted is False
    assert result.error is not None
    assert result.error.code == "UNSAFE_IN_FLIGHT_ACTIONS"
    reloaded = await repository.load(ATTEMPT_ID)
    assert reloaded is not None
    assert (
        reloaded.events,
        reloaded.state.revision,
        reloaded.checkpoint_revisions,
        _outbox_count(repository),
        ids.calls,
    ) == before


@pytest.mark.asyncio
async def test_waiting_expiry_rejects_started_sibling_without_events(
    repository: AttemptRepository,
) -> None:
    ids = QueueIdFactory()
    clock = MutableClock()
    engine = _engine(repository, id_factory=ids, clock=clock)
    await _enter_reconciliation_wait_with_sibling(
        engine,
        repository,
        sibling_started=True,
    )
    waiting = await repository.load(ATTEMPT_ID)
    assert waiting is not None
    clock.value = DEADLINE
    before = (
        waiting.events,
        waiting.state.revision,
        waiting.checkpoint_revisions,
        _outbox_count(repository),
        ids.calls,
    )

    result = await engine.handle(
        _expire(waiting.state.revision, suffix=89)
    )

    assert result.accepted is False
    assert result.error is not None
    assert result.error.code == "UNSAFE_IN_FLIGHT_ACTIONS"
    reloaded = await repository.load(ATTEMPT_ID)
    assert reloaded is not None
    assert (
        reloaded.events,
        reloaded.state.revision,
        reloaded.checkpoint_revisions,
        _outbox_count(repository),
        ids.calls,
    ) == before


@pytest.mark.asyncio
async def test_waiting_cancel_with_started_sibling_checkpoints_without_terminal_lie(
    repository: AttemptRepository,
) -> None:
    engine = _engine(repository)
    _, sibling_action_id = await _enter_reconciliation_wait_with_sibling(
        engine,
        repository,
        sibling_started=True,
    )
    waiting = await repository.load(ATTEMPT_ID)
    assert waiting is not None

    result = await engine.handle(_cancel(waiting.state.revision, suffix=75))

    assert result.accepted is True
    assert result.phase is AttemptPhase.CANCEL_REQUESTED
    loaded = await repository.load(ATTEMPT_ID)
    assert loaded is not None
    assert loaded.state.phase is AttemptPhase.CANCEL_REQUESTED
    assert loaded.state.pending_external == ()
    sibling = next(
        action
        for action in loaded.state.actions
        if action.action_id == sibling_action_id
    )
    assert sibling.status is ActionStatus.STARTED
    assert tuple(event.event_type for event in loaded.events[-4:]) == (
        "EXTERNAL_INPUT_EXPIRED",
        "BUDGET_UNCERTAIN_SETTLED",
        "CANCEL_REQUESTED",
        "ACTION_CANCELLATION_REQUESTED",
    )
    assert loaded.checkpoint_revisions[-1] == loaded.state.revision
    assert not any(event.event_type == "ATTEMPT_CANCELLED" for event in loaded.events[-4:])


@pytest.mark.asyncio
async def test_deferred_cancel_terminalizes_after_uncertain_reconciliation_settlement(
    repository: AttemptRepository,
) -> None:
    engine = _engine(repository)
    unknown_action_id, sibling_ids = (
        await _enter_reconciliation_wait_with_two_siblings(
            engine,
            repository,
            started_siblings=1,
        )
    )
    waiting = await repository.load(ATTEMPT_ID)
    assert waiting is not None
    assert (await engine.handle(_cancel(waiting.state.revision, suffix=102))).accepted
    cancelling = await repository.load(ATTEMPT_ID)
    assert cancelling is not None
    assert cancelling.state.phase is AttemptPhase.CANCEL_REQUESTED
    started_action_id = next(
        action_id
        for action_id in sibling_ids
        if next(
            action
            for action in cancelling.state.actions
            if action.action_id == action_id
        ).status
        is ActionStatus.STARTED
    )
    uncertain = next(
        action
        for action in cancelling.state.actions
        if action.action_id == unknown_action_id
    )
    assert uncertain.status is ActionStatus.OUTCOME_UNKNOWN
    assert uncertain.reconciled_status is None
    assert any(
        event.event_type == "BUDGET_UNCERTAIN_SETTLED"
        and event.reservation.action_id == unknown_action_id
        for event in cancelling.events
    )

    completed = await engine.handle(
        ReportActionOutcome(
            schema_version=1,
            command_type="REPORT_ACTION_OUTCOME",
            command_id=UUID("89000000-0000-0000-0000-000000000104"),
            attempt_id=ATTEMPT_ID,
            expected_revision=cancelling.state.revision,
            action_id=started_action_id,
            outcome=ActionSucceededOutcome(
                status=ActionStatus.SUCCEEDED,
                action_id=started_action_id,
                result_ref=_artifact("deferred-cancel-last-outcome"),
            ),
        )
    )

    assert completed.accepted is True
    assert completed.phase is AttemptPhase.CANCELLED
    terminal = await repository.load(ATTEMPT_ID)
    assert terminal is not None
    assert terminal.state.phase is AttemptPhase.CANCELLED
    assert terminal.events[-1].event_type == "ATTEMPT_CANCELLED"
    assert terminal.state.budget.resources[0].reserved == 0


@pytest.mark.asyncio
async def test_deferred_pause_completes_after_reconciled_unknown_sibling(
    repository: AttemptRepository,
) -> None:
    engine = _engine(repository)
    _, sibling_action_id = await _enter_reconciliation_wait_with_sibling(
        engine,
        repository,
        sibling_started=True,
    )
    waiting = await repository.load(ATTEMPT_ID)
    assert waiting is not None
    assert (
        await engine.handle(
            _external_response(
                expected_revision=waiting.state.revision,
                request_id=waiting.state.pending_external[0].request_id,
                response_kind=ExternalResponseKind.CONFIRM_SUCCEEDED,
                response_ref=_artifact("reconciled-before-deferred-pause"),
                suffix=103,
            )
        )
    ).accepted
    reconciled = await repository.load(ATTEMPT_ID)
    assert reconciled is not None
    assert (
        await engine.handle(_pause(reconciled.state.revision, suffix=103))
    ).accepted
    pausing = await repository.load(ATTEMPT_ID)
    assert pausing is not None
    assert pausing.state.phase is AttemptPhase.PAUSE_REQUESTED

    completed = await engine.handle(
        ReportActionOutcome(
            schema_version=1,
            command_type="REPORT_ACTION_OUTCOME",
            command_id=UUID("89000000-0000-0000-0000-000000000105"),
            attempt_id=ATTEMPT_ID,
            expected_revision=pausing.state.revision,
            action_id=sibling_action_id,
            outcome=ActionSucceededOutcome(
                status=ActionStatus.SUCCEEDED,
                action_id=sibling_action_id,
                result_ref=_artifact("deferred-pause-last-outcome"),
            ),
        )
    )

    assert completed.accepted is True
    assert completed.phase is AttemptPhase.PAUSED
    paused = await repository.load(ATTEMPT_ID)
    assert paused is not None
    assert paused.state.phase is AttemptPhase.PAUSED
    assert paused.events[-1].event_type == "ATTEMPT_PAUSED"


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "tamper_kind",
    ("omit_accepted_cleanup", "duplicate_started_request"),
)
async def test_replay_rejects_incomplete_waiting_cancel_coverage(
    repository: AttemptRepository,
    tamper_kind: str,
) -> None:
    engine = _engine(repository)
    started_siblings = 1 if tamper_kind == "omit_accepted_cleanup" else 2
    _, sibling_ids = await _enter_reconciliation_wait_with_two_siblings(
        engine,
        repository,
        started_siblings=started_siblings,
    )
    waiting = await repository.load(ATTEMPT_ID)
    assert waiting is not None
    command = _cancel(waiting.state.revision, suffix=96)
    assert (await engine.handle(command)).accepted
    completed = await repository.load(ATTEMPT_ID)
    assert completed is not None
    tampered = list(completed.events)

    if tamper_kind == "omit_accepted_cleanup":
        accepted_id = next(
            action_id
            for action_id in sibling_ids
            if next(
                action
                for action in waiting.state.actions
                if action.action_id == action_id
            ).status
            is ActionStatus.ACCEPTED
        )
        removed_ids = {
            event.event_id
            for event in tampered
            if event.command_id == command.command_id
            and event.event_type in {"ACTION_CANCELLED", "BUDGET_RELEASED"}
            and (
                getattr(event, "action_id", None) == accepted_id
                or getattr(getattr(event, "reservation", None), "action_id", None)
                == accepted_id
            )
        }
        assert len(removed_ids) == 2
        rebuilt = []
        removed = 0
        for event in tampered:
            if event.event_id in removed_ids:
                removed += 1
                continue
            if removed:
                event = event.model_copy(
                    update={
                        "sequence_no": event.sequence_no - removed,
                        "logical_time": event.logical_time - removed,
                        "causal_parent_id": rebuilt[-1].event_id,
                    }
                )
            rebuilt.append(event)
        tampered = rebuilt
    else:
        request_indexes = [
            index
            for index, event in enumerate(tampered)
            if event.command_id == command.command_id
            and event.event_type == "ACTION_CANCELLATION_REQUESTED"
        ]
        assert len(request_indexes) == 2
        first = tampered[request_indexes[0]]
        second_index = request_indexes[1]
        tampered[second_index] = tampered[second_index].model_copy(
            update={"action_id": first.action_id}
        )

    with pytest.raises(StateTransitionError) as caught:
        replay_events(tampered)

    assert caught.value.code == "invalid_external_transition"


@pytest.mark.asyncio
@pytest.mark.parametrize("outcome_kind", ("success", "failure"))
async def test_started_sibling_can_settle_during_reconciliation_wait(
    repository: AttemptRepository,
    outcome_kind: str,
) -> None:
    engine = _engine(repository)
    unknown_action_id, sibling_action_id = await _enter_reconciliation_wait_with_sibling(
        engine,
        repository,
        sibling_started=True,
    )
    waiting = await repository.load(ATTEMPT_ID)
    assert waiting is not None
    pending = waiting.state.pending_external[0]
    if outcome_kind == "success":
        outcome = ActionSucceededOutcome(
            status=ActionStatus.SUCCEEDED,
            action_id=sibling_action_id,
            result_ref=_artifact("waiting-sibling-success"),
        )
        invocation_event = "INVOCATION_COMPLETED"
        action_event = "ACTION_SUCCEEDED"
        sibling_status = ActionStatus.SUCCEEDED
    else:
        outcome = ActionFailedOutcome(
            status=ActionStatus.FAILED,
            action_id=sibling_action_id,
            error=ErrorSummary(
                code="WAITING_SIBLING_FAILED",
                retryable=False,
                safe_message="The sibling Action failed while reconciliation was pending.",
            ),
        )
        invocation_event = "INVOCATION_FAILED"
        action_event = "ACTION_FAILED"
        sibling_status = ActionStatus.FAILED

    settled = await engine.handle(
        ReportActionOutcome(
            schema_version=1,
            command_type="REPORT_ACTION_OUTCOME",
            command_id=UUID(
                "86000000-0000-0000-0000-000000000001"
                if outcome_kind == "success"
                else "86000000-0000-0000-0000-000000000002"
            ),
            attempt_id=ATTEMPT_ID,
            expected_revision=waiting.state.revision,
            action_id=sibling_action_id,
            outcome=outcome,
        )
    )

    assert settled.accepted is True
    assert settled.phase is AttemptPhase.WAITING_EXTERNAL
    observed = await repository.load(ATTEMPT_ID)
    assert observed is not None
    assert observed.state.pending_external == (pending,)
    actions = {action.action_id: action for action in observed.state.actions}
    assert actions[unknown_action_id].status is ActionStatus.OUTCOME_UNKNOWN
    assert actions[sibling_action_id].status is sibling_status
    assert observed.state.budget.resources[0].reserved == 2
    assert observed.state.budget.resources[0].consumed == 2
    assert tuple(event.event_type for event in observed.events[-3:]) == (
        invocation_event,
        action_event,
        "BUDGET_SETTLED",
    )
    assert observed.checkpoint_revisions[-1] == observed.state.revision
    assert _outbox_count(repository) == 0

    abandoned = await engine.handle(
        _external_response(
            expected_revision=observed.state.revision,
            request_id=pending.request_id,
            response_kind=ExternalResponseKind.ABANDON,
            suffix=76 if outcome_kind == "success" else 77,
        )
    )
    assert abandoned.accepted is True
    assert abandoned.phase is AttemptPhase.INTERRUPTED
    terminal = await repository.load(ATTEMPT_ID)
    assert terminal is not None
    assert terminal.state.budget.resources[0].reserved == 0
    assert terminal.state.budget.resources[0].consumed == 4


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "request_kind",
    (
        ExternalRequestKind.ACTION_APPROVAL,
        ExternalRequestKind.ADDITIONAL_INPUT,
    ),
)
async def test_started_action_can_settle_during_any_external_wait(
    repository: AttemptRepository,
    request_kind: ExternalRequestKind,
) -> None:
    engine = _engine(repository)
    action_ids = await _accept_actions(engine, repository, count=2)
    first_action_id = await _start_first_action(engine, repository)
    started = await repository.load(ATTEMPT_ID)
    assert started is not None
    assert (
        await engine.handle(
            ReportActionOutcome(
                schema_version=1,
                command_type="REPORT_ACTION_OUTCOME",
                command_id=UUID("88000000-0000-0000-0000-000000000001"),
                attempt_id=ATTEMPT_ID,
                expected_revision=started.state.revision,
                action_id=first_action_id,
                outcome=ActionSucceededOutcome(
                    status=ActionStatus.SUCCEEDED,
                    action_id=first_action_id,
                    result_ref=_artifact("pre-wait-sibling-trigger"),
                ),
            )
        )
    ).accepted
    trigger_ready = await repository.load(ATTEMPT_ID)
    assert trigger_ready is not None
    trigger = next(
        event
        for event in reversed(trigger_ready.events)
        if event.event_type == "ACTION_SUCCEEDED"
    )
    claim = await repository.claim_action(
        worker_id="waiting-kind-worker",
        now_utc=UTC_NOW,
        lease_seconds=10,
    )
    assert claim is not None
    sibling_action_id = next(
        action_id for action_id in action_ids if action_id != first_action_id
    )
    assert claim.action.action_id == sibling_action_id
    assert (
        await engine.handle(
            ReportActionStarted(
                schema_version=1,
                command_type="REPORT_ACTION_STARTED",
                command_id=UUID("88000000-0000-0000-0000-000000000002"),
                attempt_id=ATTEMPT_ID,
                expected_revision=trigger_ready.state.revision,
                action_id=sibling_action_id,
            ),
            delivery_claim=claim.delivery_claim(),
        )
    ).accepted
    sibling_started = await repository.load(ATTEMPT_ID)
    assert sibling_started is not None
    proposal = (
        _proposal(ordinal=2)
        if request_kind is ExternalRequestKind.ACTION_APPROVAL
        else None
    )
    decision = _external_decision(
        request_id=f"waiting-{request_kind.value.lower()}",
        request_kind=request_kind,
        proposal=proposal,
    ).model_copy(
        update={
            "command_id": UUID("88000000-0000-0000-0000-000000000003"),
            "expected_revision": sibling_started.state.revision,
            "trigger_sequence_no": trigger.sequence_no,
            "strategy": sibling_started.state.strategy,
        }
    )
    assert (await engine.handle(decision)).accepted
    waiting = await repository.load(ATTEMPT_ID)
    assert waiting is not None
    pending = waiting.state.pending_external

    settled = await engine.handle(
        ReportActionOutcome(
            schema_version=1,
            command_type="REPORT_ACTION_OUTCOME",
            command_id=UUID("88000000-0000-0000-0000-000000000004"),
            attempt_id=ATTEMPT_ID,
            expected_revision=waiting.state.revision,
            action_id=sibling_action_id,
            outcome=ActionSucceededOutcome(
                status=ActionStatus.SUCCEEDED,
                action_id=sibling_action_id,
                result_ref=_artifact(
                    f"waiting-{request_kind.value.lower()}-result"
                ),
            ),
        )
    )

    assert settled.accepted is True
    assert settled.phase is AttemptPhase.WAITING_EXTERNAL
    observed = await repository.load(ATTEMPT_ID)
    assert observed is not None
    assert observed.state.pending_external == pending
    sibling = next(
        action
        for action in observed.state.actions
        if action.action_id == sibling_action_id
    )
    assert sibling.status is ActionStatus.SUCCEEDED
    assert tuple(event.event_type for event in observed.events[-3:]) == (
        "INVOCATION_COMPLETED",
        "ACTION_SUCCEEDED",
        "BUDGET_SETTLED",
    )
    assert _outbox_count(repository) == 0


@pytest.mark.asyncio
async def test_second_unknown_during_reconciliation_wait_has_zero_side_effects(
    repository: AttemptRepository,
) -> None:
    ids = QueueIdFactory()
    engine = _engine(repository, id_factory=ids)
    _, sibling_action_id = await _enter_reconciliation_wait_with_sibling(
        engine,
        repository,
        sibling_started=True,
    )
    waiting = await repository.load(ATTEMPT_ID)
    assert waiting is not None
    before = (
        waiting.events,
        waiting.state.revision,
        waiting.checkpoint_revisions,
        _outbox_count(repository),
        ids.calls,
    )

    result = await engine.handle(
        ReportActionOutcome(
            schema_version=1,
            command_type="REPORT_ACTION_OUTCOME",
            command_id=UUID("86000000-0000-0000-0000-000000000003"),
            attempt_id=ATTEMPT_ID,
            expected_revision=waiting.state.revision,
            action_id=sibling_action_id,
            outcome=_unknown_outcome(sibling_action_id),
        )
    )

    assert result.accepted is False
    assert result.error is not None
    assert result.error.code == "ILLEGAL_TRANSITION"
    reloaded = await repository.load(ATTEMPT_ID)
    assert reloaded is not None
    assert (
        reloaded.events,
        reloaded.state.revision,
        reloaded.checkpoint_revisions,
        _outbox_count(repository),
        ids.calls,
    ) == before


@pytest.mark.asyncio
@pytest.mark.parametrize("request_kind", ("approval", "reconciliation"))
@pytest.mark.parametrize("control", ("cancel", "expire"))
async def test_waiting_control_cleans_external_request_and_terminalizes(
    repository: AttemptRepository,
    request_kind: str,
    control: str,
) -> None:
    clock = MutableClock()
    engine = _engine(repository, clock=clock)
    if request_kind == "approval":
        await _start_attempt(engine)
        assert (
            await engine.handle(
                _external_decision(
                    request_id="cleanup-approval",
                    request_kind=ExternalRequestKind.ACTION_APPROVAL,
                    proposal=_proposal(),
                )
            )
        ).accepted
    else:
        await _accept_actions(engine, repository, count=1)
        action_id = await _start_first_action(engine, repository)
        await _report_unknown(engine, repository, action_id=action_id, suffix=41)
    waiting = await repository.load(ATTEMPT_ID)
    assert waiting is not None
    assert waiting.state.phase is AttemptPhase.WAITING_EXTERNAL
    clock.value = DEADLINE

    command = (
        _cancel(waiting.state.revision, suffix=42)
        if control == "cancel"
        else _expire(waiting.state.revision, suffix=42)
    )
    result = await engine.handle(command)

    assert result.accepted is True
    expected_phase = (
        AttemptPhase.CANCELLED
        if control == "cancel"
        else AttemptPhase.TIMED_OUT
    )
    assert result.phase is expected_phase
    loaded = await repository.load(ATTEMPT_ID)
    assert loaded is not None
    assert loaded.state.phase is expected_phase
    assert loaded.state.pending_external == ()
    action = loaded.state.actions[0]
    expected_prefix: tuple[str, ...]
    if request_kind == "approval":
        assert action.status is ActionStatus.REJECTED
        assert loaded.state.budget.resources[0].reserved == 0
        assert loaded.state.budget.resources[0].consumed == 0
        expected_prefix = ("EXTERNAL_INPUT_EXPIRED", "ACTION_REJECTED")
    else:
        assert action.status is ActionStatus.OUTCOME_UNKNOWN
        assert action.reconciled_status is None
        assert loaded.state.budget.resources[0].reserved == 0
        assert loaded.state.budget.resources[0].consumed == 2
        expected_prefix = (
            "EXTERNAL_INPUT_EXPIRED",
            "BUDGET_UNCERTAIN_SETTLED",
        )
    expected_suffix = (
        expected_prefix + ("CANCEL_REQUESTED", "ATTEMPT_CANCELLED")
        if control == "cancel"
        else expected_prefix + ("ATTEMPT_TIMED_OUT",)
    )
    assert tuple(event.event_type for event in loaded.events[-len(expected_suffix) :]) == (
        expected_suffix
    )
    assert loaded.checkpoint_revisions[-1] == loaded.state.revision
    assert _outbox_count(repository) == 0


@pytest.mark.asyncio
async def test_pause_without_started_action_checkpoints_then_resumes(
    repository: AttemptRepository,
) -> None:
    engine = _engine(repository)
    await _start_attempt(engine)
    pause = _pause(2)

    paused = await engine.handle(pause)
    duplicate = await engine.handle(pause)
    stale = await engine.handle(_pause(2, suffix=2))

    assert paused.accepted is True
    assert paused.revision == 4
    assert paused.phase is AttemptPhase.PAUSED
    assert duplicate == paused
    assert stale.accepted is False
    assert stale.error is not None
    assert stale.error.code == "REVISION_CONFLICT"
    loaded = await repository.load(ATTEMPT_ID)
    assert loaded is not None
    assert tuple(event.event_type for event in loaded.events[-2:]) == (
        "PAUSE_REQUESTED",
        "ATTEMPT_PAUSED",
    )
    assert loaded.checkpoint_revisions == (4,)

    resumed = await engine.handle(_resume(4))

    assert resumed.accepted is True
    assert resumed.revision == 5
    assert resumed.phase is AttemptPhase.RUNNING
    reloaded = await repository.load(ATTEMPT_ID)
    assert reloaded is not None
    assert reloaded.events[-1].event_type == "ATTEMPT_RESUMED"
    assert reloaded.checkpoint_revisions == (4,)


@pytest.mark.asyncio
async def test_pause_preserves_accepted_delivery_identity_until_resume(
    repository: AttemptRepository,
) -> None:
    engine = _engine(repository)
    action_ids = await _accept_actions(engine, repository, count=1)
    loaded = await repository.load(ATTEMPT_ID)
    assert loaded is not None

    paused = await engine.handle(_pause(loaded.state.revision))

    assert paused.phase is AttemptPhase.PAUSED
    assert (
        await repository.claim_action(
            worker_id="paused-worker",
            now_utc=UTC_NOW,
            lease_seconds=10,
        )
        is None
    )
    paused_loaded = await repository.load(ATTEMPT_ID)
    assert paused_loaded is not None
    assert paused_loaded.state.actions[0].action_id == action_ids[0]
    assert paused_loaded.state.actions[0].status is ActionStatus.ACCEPTED

    resumed = await engine.handle(_resume(paused.revision))
    claim = await repository.claim_action(
        worker_id="resumed-worker",
        now_utc=UTC_NOW,
        lease_seconds=10,
    )

    assert resumed.phase is AttemptPhase.RUNNING
    assert claim is not None
    assert claim.action.action_id == action_ids[0]
    assert claim.action.reservation_id == paused_loaded.state.actions[0].reservation_id
    assert claim.action.idempotency_key == paused_loaded.state.actions[0].idempotency_key


@pytest.mark.asyncio
async def test_started_outcome_completes_deferred_pause_in_same_commit(
    repository: AttemptRepository,
) -> None:
    engine = _engine(repository)
    await _accept_actions(engine, repository, count=1)
    action_id = await _start_first_action(engine, repository)
    started = await repository.load(ATTEMPT_ID)
    assert started is not None

    requested = await engine.handle(_pause(started.state.revision))

    assert requested.accepted is True
    assert requested.phase is AttemptPhase.PAUSE_REQUESTED
    after_request = await repository.load(ATTEMPT_ID)
    assert after_request is not None
    assert after_request.events[-1].event_type == "PAUSE_REQUESTED"
    assert after_request.checkpoint_revisions == ()
    rejected_resume = await engine.handle(_resume(requested.revision, suffix=2))
    assert rejected_resume.accepted is False
    assert rejected_resume.error is not None
    assert rejected_resume.error.code == "ILLEGAL_TRANSITION"
    after_rejected_resume = await repository.load(ATTEMPT_ID)
    assert after_rejected_resume is not None
    assert after_rejected_resume.events == after_request.events
    assert after_rejected_resume.checkpoint_revisions == ()

    outcome = await engine.handle(
        ReportActionOutcome(
            schema_version=1,
            command_type="REPORT_ACTION_OUTCOME",
            command_id=UUID("70000000-0000-0000-0000-000000000001"),
            attempt_id=ATTEMPT_ID,
            expected_revision=requested.revision,
            action_id=action_id,
            outcome=ActionSucceededOutcome(
                status=ActionStatus.SUCCEEDED,
                action_id=action_id,
                result_ref=_artifact("deferred-pause-result"),
            ),
        )
    )

    assert outcome.accepted is True
    assert outcome.phase is AttemptPhase.PAUSED
    settled = await repository.load(ATTEMPT_ID)
    assert settled is not None
    assert tuple(event.event_type for event in settled.events[-4:]) == (
        "INVOCATION_COMPLETED",
        "ACTION_SUCCEEDED",
        "BUDGET_SETTLED",
        "ATTEMPT_PAUSED",
    )
    assert settled.checkpoint_revisions == (settled.state.revision,)
    assert (
        await repository.claim_action(
            worker_id="after-outcome",
            now_utc=UTC_NOW + timedelta(seconds=10),
            lease_seconds=10,
        )
        is None
    )


@pytest.mark.asyncio
async def test_deferred_pause_preserves_accepted_action_beside_settled_action(
    repository: AttemptRepository,
) -> None:
    engine = _engine(repository)
    action_ids = await _accept_actions(engine, repository, count=2)
    started_action_id = await _start_first_action(engine, repository)
    before_pause = await repository.load(ATTEMPT_ID)
    assert before_pause is not None
    preserved = next(
        action
        for action in before_pause.state.actions
        if action.action_id != started_action_id
    )
    requested = await engine.handle(_pause(before_pause.state.revision))

    observed = await engine.handle(
        ReportActionOutcome(
            schema_version=1,
            command_type="REPORT_ACTION_OUTCOME",
            command_id=UUID("70000000-0000-0000-0000-000000000002"),
            attempt_id=ATTEMPT_ID,
            expected_revision=requested.revision,
            action_id=started_action_id,
            outcome=ActionSucceededOutcome(
                status=ActionStatus.SUCCEEDED,
                action_id=started_action_id,
                result_ref=_artifact("mixed-deferred-pause-result"),
            ),
        )
    )

    assert observed.accepted is True
    assert observed.phase is AttemptPhase.PAUSED
    paused = await repository.load(ATTEMPT_ID)
    assert paused is not None
    accepted = next(
        action
        for action in paused.state.actions
        if action.action_id == preserved.action_id
    )
    assert tuple(action.action_id for action in paused.state.actions) == action_ids
    assert accepted.status is ActionStatus.ACCEPTED
    assert accepted.reservation_id == preserved.reservation_id
    assert accepted.idempotency_key == preserved.idempotency_key
    assert paused.events[-1].event_type == "ATTEMPT_PAUSED"
    assert paused.checkpoint_revisions[-1] == paused.state.revision
    assert (
        await repository.claim_action(
            worker_id="mixed-paused-worker",
            now_utc=UTC_NOW,
            lease_seconds=10,
        )
        is None
    )


@pytest.mark.asyncio
async def test_cancel_atomically_releases_all_accepted_actions_from_paused(
    repository: AttemptRepository,
) -> None:
    engine = _engine(repository)
    action_ids = await _accept_actions(engine, repository, count=2)
    running = await repository.load(ATTEMPT_ID)
    assert running is not None
    paused = await engine.handle(_pause(running.state.revision))

    cancelled = await engine.handle(_cancel(paused.revision))

    assert cancelled.accepted is True
    assert cancelled.phase is AttemptPhase.CANCELLED
    loaded = await repository.load(ATTEMPT_ID)
    assert loaded is not None
    assert tuple(action.action_id for action in loaded.state.actions) == action_ids
    assert all(
        action.status is ActionStatus.CANCELLED
        for action in loaded.state.actions
    )
    assert all(resource.reserved == 0 for resource in loaded.state.budget.resources)
    assert tuple(event.event_type for event in loaded.events[-6:]) == (
        "CANCEL_REQUESTED",
        "ACTION_CANCELLED",
        "BUDGET_RELEASED",
        "ACTION_CANCELLED",
        "BUDGET_RELEASED",
        "ATTEMPT_CANCELLED",
    )
    assert loaded.checkpoint_revisions[-1] == loaded.state.revision
    assert (
        await repository.claim_action(
            worker_id="cancelled-worker",
            now_utc=UTC_NOW,
            lease_seconds=10,
        )
        is None
    )


@pytest.mark.asyncio
async def test_running_cancel_is_idempotent_and_terminal_controls_add_no_events(
    repository: AttemptRepository,
) -> None:
    engine = _engine(repository)
    await _start_attempt(engine)
    cancel = _cancel(2)

    cancelled = await engine.handle(cancel)
    duplicate = await engine.handle(cancel)
    terminal_pause = await engine.handle(_pause(cancelled.revision, suffix=9))
    terminal_resume = await engine.handle(_resume(cancelled.revision, suffix=9))

    assert cancelled.accepted is True
    assert cancelled.phase is AttemptPhase.CANCELLED
    assert duplicate == cancelled
    assert terminal_pause.accepted is False
    assert terminal_pause.error is not None
    assert terminal_pause.error.code == "ILLEGAL_TRANSITION"
    assert terminal_resume.accepted is False
    assert terminal_resume.error is not None
    assert terminal_resume.error.code == "ILLEGAL_TRANSITION"
    loaded = await repository.load(ATTEMPT_ID)
    assert loaded is not None
    assert tuple(event.event_type for event in loaded.events[-2:]) == (
        "CANCEL_REQUESTED",
        "ATTEMPT_CANCELLED",
    )
    assert loaded.state.revision == cancelled.revision
    assert loaded.checkpoint_revisions == (cancelled.revision,)


@pytest.mark.asyncio
@pytest.mark.parametrize("outcome_kind", ("success", "failure"))
async def test_started_action_records_real_outcome_after_cancel_request(
    repository: AttemptRepository,
    outcome_kind: str,
) -> None:
    engine = _engine(repository)
    await _accept_actions(engine, repository, count=1)
    action_id = await _start_first_action(engine, repository)
    started = await repository.load(ATTEMPT_ID)
    assert started is not None
    pause_requested = await engine.handle(_pause(started.state.revision))

    cancel_requested = await engine.handle(_cancel(pause_requested.revision))

    assert cancel_requested.accepted is True
    assert cancel_requested.phase is AttemptPhase.CANCEL_REQUESTED
    requested = await repository.load(ATTEMPT_ID)
    assert requested is not None
    assert tuple(event.event_type for event in requested.events[-2:]) == (
        "CANCEL_REQUESTED",
        "ACTION_CANCELLATION_REQUESTED",
    )
    assert requested.state.actions[0].status is ActionStatus.STARTED
    assert requested.checkpoint_revisions == ()
    repeated_cancel = await engine.handle(
        _cancel(cancel_requested.revision, suffix=2)
    )
    assert repeated_cancel.accepted is False
    assert repeated_cancel.error is not None
    assert repeated_cancel.error.code == "ILLEGAL_TRANSITION"
    after_repeated_cancel = await repository.load(ATTEMPT_ID)
    assert after_repeated_cancel is not None
    assert after_repeated_cancel.events == requested.events
    assert after_repeated_cancel.checkpoint_revisions == ()

    if outcome_kind == "success":
        outcome = ActionSucceededOutcome(
            status=ActionStatus.SUCCEEDED,
            action_id=action_id,
            result_ref=_artifact("cancel-race-success"),
        )
        expected_event_type = "ACTION_SUCCEEDED"
        expected_status = ActionStatus.SUCCEEDED
    else:
        error = ErrorSummary(
            code="BACKEND_FAILED",
            retryable=False,
            safe_message="The Backend reported a failure.",
        )
        outcome = ActionFailedOutcome(
            status=ActionStatus.FAILED,
            action_id=action_id,
            error=error,
        )
        expected_event_type = "ACTION_FAILED"
        expected_status = ActionStatus.FAILED
    observed = await engine.handle(
        ReportActionOutcome(
            schema_version=1,
            command_type="REPORT_ACTION_OUTCOME",
            command_id=UUID(
                "80000000-0000-0000-0000-000000000001"
                if outcome_kind == "success"
                else "80000000-0000-0000-0000-000000000002"
            ),
            attempt_id=ATTEMPT_ID,
            expected_revision=cancel_requested.revision,
            action_id=action_id,
            outcome=outcome,
        )
    )

    assert observed.accepted is True
    assert observed.phase is AttemptPhase.CANCELLED
    completed = await repository.load(ATTEMPT_ID)
    assert completed is not None
    assert completed.state.actions[0].status is expected_status
    assert completed.events[-3].event_type == expected_event_type
    assert completed.events[-1].event_type == "ATTEMPT_CANCELLED"
    assert completed.checkpoint_revisions == (completed.state.revision,)


@pytest.mark.asyncio
async def test_expiry_rejects_wrong_or_early_deadline_without_events(
    repository: AttemptRepository,
) -> None:
    clock = MutableClock(DEADLINE - timedelta(seconds=1))
    engine = _engine(repository, clock=clock)
    await _start_attempt(engine)

    early = await engine.handle(_expire(2, suffix=1))
    mismatch = await engine.handle(
        _expire(
            2,
            deadline_at=DEADLINE + timedelta(seconds=1),
            suffix=2,
        )
    )

    assert early.accepted is False
    assert early.error is not None
    assert early.error.code == "DEADLINE_NOT_REACHED"
    assert mismatch.accepted is False
    assert mismatch.error is not None
    assert mismatch.error.code == "DEADLINE_MISMATCH"
    loaded = await repository.load(ATTEMPT_ID)
    assert loaded is not None
    assert loaded.state.revision == 2
    assert loaded.checkpoint_revisions == ()
    assert tuple(event.event_type for event in loaded.events) == (
        "ATTEMPT_PLANNED",
        "ATTEMPT_STARTED",
    )


@pytest.mark.asyncio
async def test_expiry_cancels_accepted_actions_and_checkpoints_timeout(
    repository: AttemptRepository,
) -> None:
    clock = MutableClock()
    engine = _engine(repository, clock=clock)
    await _accept_actions(engine, repository, count=2)
    loaded = await repository.load(ATTEMPT_ID)
    assert loaded is not None
    clock.value = DEADLINE

    expired = await engine.handle(_expire(loaded.state.revision))

    assert expired.accepted is True
    assert expired.phase is AttemptPhase.TIMED_OUT
    completed = await repository.load(ATTEMPT_ID)
    assert completed is not None
    assert all(
        action.status is ActionStatus.CANCELLED
        for action in completed.state.actions
    )
    assert all(resource.reserved == 0 for resource in completed.state.budget.resources)
    assert completed.events[-1].event_type == "ATTEMPT_TIMED_OUT"
    assert completed.checkpoint_revisions == (completed.state.revision,)
    assert (
        await repository.claim_action(
            worker_id="expired-worker",
            now_utc=DEADLINE,
            lease_seconds=10,
        )
        is None
    )


@pytest.mark.asyncio
async def test_expiry_rejects_started_action_as_unsafe_without_events(
    repository: AttemptRepository,
) -> None:
    clock = MutableClock()
    engine = _engine(repository, clock=clock)
    await _accept_actions(engine, repository, count=1)
    await _start_first_action(engine, repository)
    started = await repository.load(ATTEMPT_ID)
    assert started is not None
    clock.value = DEADLINE

    expired = await engine.handle(_expire(started.state.revision))

    assert expired.accepted is False
    assert expired.error is not None
    assert expired.error.code == "UNSAFE_IN_FLIGHT_ACTIONS"
    reloaded = await repository.load(ATTEMPT_ID)
    assert reloaded is not None
    assert reloaded.events == started.events
    assert reloaded.checkpoint_revisions == ()


@pytest.mark.asyncio
async def test_sqlite_reopen_preserves_paused_outbox_and_terminal_checkpoint(
    tmp_path: Path,
) -> None:
    database_path = tmp_path / "control-reopen.sqlite3"
    repository = SQLiteAttemptRepository(database_path)
    engine = _engine(repository)
    action_ids = await _accept_actions(engine, repository, count=1)
    loaded = await repository.load(ATTEMPT_ID)
    assert loaded is not None
    paused = await engine.handle(_pause(loaded.state.revision))

    reopened = SQLiteAttemptRepository(database_path)
    restored = await reopened.load(ATTEMPT_ID)

    assert restored is not None
    assert restored.state.phase is AttemptPhase.PAUSED
    assert restored.state.actions[0].action_id == action_ids[0]
    assert restored.state.actions[0].status is ActionStatus.ACCEPTED
    assert restored.checkpoint_revisions == (paused.revision,)
    assert (
        await reopened.claim_action(
            worker_id="reopened-paused",
            now_utc=UTC_NOW,
            lease_seconds=10,
        )
        is None
    )

    reopened_engine = _engine(reopened, skip_ids=50)
    cancelled = await reopened_engine.handle(_cancel(paused.revision))
    terminal_reopen = SQLiteAttemptRepository(database_path)
    terminal = await terminal_reopen.load(ATTEMPT_ID)

    assert cancelled.phase is AttemptPhase.CANCELLED
    assert terminal is not None
    assert terminal.state.phase is AttemptPhase.CANCELLED
    assert terminal.checkpoint_revisions == (paused.revision, cancelled.revision)
    assert (
        await terminal_reopen.claim_action(
            worker_id="reopened-terminal",
            now_utc=UTC_NOW,
            lease_seconds=10,
        )
        is None
    )
