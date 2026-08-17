from __future__ import annotations

import json
from copy import deepcopy
from datetime import datetime, timedelta, timezone
from hashlib import sha256
from uuid import UUID

import pytest
from pydantic import ValidationError

import experiment_system.events as event_models
from experiment_system.actions import (
    ActionProposal,
    ActionSucceededOutcome,
    ActionType,
    ActionUnknownOutcome,
    ExternalInputRequirement,
)
from experiment_system.events import (
    ActionProposed,
    ActionRejected,
    AttemptCancelled,
    AttemptFailed,
    AttemptInterrupted,
    AttemptPlanned,
    AttemptStarted,
    AttemptSucceeded,
    AttemptTimedOut,
    EventEnvelope,
    StrategyDecisionRecorded,
    UnsupportedEventSchema,
    parse_domain_event,
)
from experiment_system.contract import StrategyDirective
from experiment_system.commands import outcome_reconciliation_request_id
from experiment_system.reducer import StateTransitionError, apply_event, replay_events
from experiment_system.state import (
    ActionState,
    ActionStatus,
    ArtifactRef,
    AttemptPhase,
    AttemptState,
    BudgetState,
    ErrorSummary,
    ExternalRequest,
    ExternalRequestKind,
    ExternalResponseKind,
    RecoveryPolicy,
    ResourceBudget,
    StrategyStateEnvelope,
    canonical_state_bytes,
)


UTC_NOW = datetime(2026, 7, 23, 8, 30, tzinfo=timezone.utc)
EVENT_IDS = tuple(UUID(f"00000000-0000-0000-0000-{index:012d}") for index in range(1, 20))
COMMAND_IDS = tuple(UUID(f"10000000-0000-0000-0000-{index:012d}") for index in range(1, 20))
HASH = "a" * 64


def _canonical_json(value: object) -> bytes:
    return json.dumps(
        value,
        ensure_ascii=False,
        allow_nan=False,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")


def _artifact(path: str = "attempts/attempt-1/manifest.json") -> ArtifactRef:
    return ArtifactRef(
        capture_class="full",
        content_hash=HASH,
        media_type="application/json",
        byte_size=12,
        relative_path=path,
    )


def _strategy() -> StrategyStateEnvelope:
    value = {"queue": ["agent-a"], "round": 1}
    payload = _canonical_json(value)
    return StrategyStateEnvelope(
        strategy_id="router",
        strategy_schema_version=1,
        value=value,
        content_hash=sha256(payload).hexdigest(),
        byte_size=len(payload),
    )


def _budget() -> BudgetState:
    return BudgetState(
        resources=(ResourceBudget(resource="actions", limit=10),),
        deadline_at=UTC_NOW + timedelta(hours=1),
        max_call_depth=4,
        max_concurrent_actions=2,
    )


def _envelope(
    sequence_no: int,
    *,
    event_index: int | None = None,
    attempt_id: str = "attempt-1",
    trial_id: str = "trial-1",
    causal_parent_id: UUID | None = None,
) -> dict[str, object]:
    index = sequence_no if event_index is None else event_index
    return {
        "schema_version": 1,
        "event_id": EVENT_IDS[index - 1],
        "trial_id": trial_id,
        "attempt_id": attempt_id,
        "sequence_no": sequence_no,
        "command_id": COMMAND_IDS[index - 1],
        "causal_parent_id": causal_parent_id,
        "logical_time": sequence_no - 1,
        "wall_time_utc": UTC_NOW + timedelta(seconds=sequence_no - 1),
    }


def _planned(**changes: object) -> AttemptPlanned:
    values: dict[str, object] = {
        **_envelope(1),
        "event_type": "ATTEMPT_PLANNED",
        "state_schema_version": 1,
        "experiment_id": "experiment-1",
        "strategy_id": "router",
        "manifest_ref": _artifact(),
        "strategy": _strategy(),
        "budget": _budget(),
    }
    values.update(changes)
    return AttemptPlanned(**values)


def _started(sequence_no: int = 2, **changes: object) -> AttemptStarted:
    values: dict[str, object] = {
        **_envelope(sequence_no, causal_parent_id=EVENT_IDS[0]),
        "event_type": "ATTEMPT_STARTED",
    }
    values.update(changes)
    return AttemptStarted(**values)


def _succeeded(sequence_no: int = 3, **changes: object) -> AttemptSucceeded:
    values: dict[str, object] = {
        **_envelope(sequence_no, causal_parent_id=EVENT_IDS[1]),
        "event_type": "ATTEMPT_SUCCEEDED",
        "result_ref": _artifact("attempts/attempt-1/result.json"),
    }
    values.update(changes)
    return AttemptSucceeded(**values)


def _error() -> ErrorSummary:
    return ErrorSummary(
        code="BACKEND_FAILURE",
        retryable=True,
        safe_message="The backend did not complete the request.",
    )


def _failed(sequence_no: int = 3, **changes: object) -> AttemptFailed:
    values: dict[str, object] = {
        **_envelope(sequence_no, causal_parent_id=EVENT_IDS[1]),
        "event_type": "ATTEMPT_FAILED",
        "error": _error(),
    }
    values.update(changes)
    return AttemptFailed(**values)


def _timed_out(sequence_no: int = 3, **changes: object) -> AttemptTimedOut:
    values: dict[str, object] = {
        **_envelope(sequence_no, causal_parent_id=EVENT_IDS[1]),
        "event_type": "ATTEMPT_TIMED_OUT",
        "error": _error(),
    }
    values.update(changes)
    return AttemptTimedOut(**values)


def _interrupted(sequence_no: int = 3, **changes: object) -> AttemptInterrupted:
    values: dict[str, object] = {
        **_envelope(sequence_no, causal_parent_id=EVENT_IDS[1]),
        "event_type": "ATTEMPT_INTERRUPTED",
        "error": _error(),
    }
    values.update(changes)
    return AttemptInterrupted(**values)


def _proposal(action_id: str, *, causal_parent_id: UUID) -> ActionProposal:
    return ActionProposal(
        action_id=action_id,
        action_type=ActionType.SEND_MESSAGE,
        actor="router",
        target_ids=("agent-a",),
        invocation_id=None,
        causal_parent_id=str(causal_parent_id),
        payload_ref=None,
        recovery_policy=RecoveryPolicy.REPLAY_SAFE,
        requested_timeout=30,
        call_depth=0,
        resource_requests=(),
    )


def _strategy_decision(
    sequence_no: int,
    *,
    trigger_sequence_no: int,
    causal_parent_id: UUID,
    proposals: tuple[ActionProposal, ...] = (),
    directive: StrategyDirective = StrategyDirective.CONTINUE,
) -> StrategyDecisionRecorded:
    return StrategyDecisionRecorded(
        **_envelope(sequence_no, causal_parent_id=causal_parent_id),
        event_type="STRATEGY_DECISION_RECORDED",
        trigger_sequence_no=trigger_sequence_no,
        strategy=_strategy(),
        proposals=proposals,
        directive=directive,
        error=_error() if directive is StrategyDirective.FAIL else None,
    )


def _two_pending_trigger_stream() -> tuple[object, ...]:
    first = _proposal("action-1", causal_parent_id=EVENT_IDS[1])
    second = _proposal("action-2", causal_parent_id=EVENT_IDS[1])
    return (
        _planned(),
        _started(),
        _strategy_decision(
            3,
            trigger_sequence_no=2,
            causal_parent_id=EVENT_IDS[1],
            proposals=(first, second),
        ),
        ActionProposed(
            **_envelope(4, causal_parent_id=EVENT_IDS[2]),
            event_type="ACTION_PROPOSED",
            proposal=first.model_copy(
                update={"causal_parent_id": str(EVENT_IDS[2])}
            ),
        ),
        ActionRejected(
            **_envelope(5, causal_parent_id=EVENT_IDS[3]),
            event_type="ACTION_REJECTED",
            action_id=first.action_id,
            error=_error(),
        ),
        ActionProposed(
            **_envelope(6, causal_parent_id=EVENT_IDS[4]),
            event_type="ACTION_PROPOSED",
            proposal=second.model_copy(
                update={"causal_parent_id": str(EVENT_IDS[4])}
            ),
        ),
        ActionRejected(
            **_envelope(7, causal_parent_id=EVENT_IDS[5]),
            event_type="ACTION_REJECTED",
            action_id=second.action_id,
            error=_error(),
        ),
    )


def _cancelled(sequence_no: int = 3, **changes: object) -> AttemptCancelled:
    values: dict[str, object] = {
        **_envelope(sequence_no, causal_parent_id=EVENT_IDS[1]),
        "event_type": "ATTEMPT_CANCELLED",
    }
    values.update(changes)
    return AttemptCancelled(**values)


def _running_state() -> AttemptState:
    return apply_event(apply_event(None, _planned()), _started())


def _action(status: ActionStatus) -> ActionState:
    is_proposed = status is ActionStatus.PROPOSED
    is_succeeded = status is ActionStatus.SUCCEEDED
    return ActionState(
        action_id="action-1",
        action_type="coordinate",
        actor_id="engine",
        target_ids=("agent-a",),
        status=status,
        causal_parent_id=None,
        invocation_id=None,
        payload_ref=_artifact("attempts/attempt-1/actions/action-1.json"),
        result_ref=(
            _artifact("attempts/attempt-1/actions/action-1-result.json")
            if is_succeeded
            else None
        ),
        idempotency_key=None if is_proposed else "idem-action-1",
        recovery_policy=RecoveryPolicy.REPLAY_SAFE,
        reservation_id=None if is_proposed else "reservation-1",
        retry_of_action_id=None,
        error_code=None,
    )


def _state_with_action(
    status: ActionStatus, *, phase: AttemptPhase = AttemptPhase.RUNNING
) -> AttemptState:
    state = _running_state()
    return AttemptState.model_validate(
        {
            **state.model_dump(mode="python"),
            "phase": phase,
            "actions": (_action(status),),
        }
    )


def _assert_transition_code(code: str, state: AttemptState | None, event: object) -> None:
    with pytest.raises(StateTransitionError) as caught:
        apply_event(state, event)  # type: ignore[arg-type]
    assert caught.value.code == code
    assert str(caught.value) == caught.value.safe_message


def test_planned_creates_complete_deterministic_state() -> None:
    state = apply_event(None, _planned())

    assert state == AttemptState.model_validate(state.model_dump(mode="python"))
    assert state.schema_version == 1
    assert state.experiment_id == "experiment-1"
    assert state.trial_id == "trial-1"
    assert state.attempt_id == "attempt-1"
    assert state.strategy_id == "router"
    assert state.revision == 1
    assert state.phase is AttemptPhase.PLANNED
    assert state.manifest_ref == _artifact()
    assert state.strategy == _strategy()
    assert state.budget == _budget()
    assert state.actions == ()
    assert state.invocations == ()
    assert state.pending_external == ()
    assert state.result_ref is None
    assert state.terminal_error is None
    assert state.started_at is None
    assert state.finished_at is None


def test_external_request_events_wait_receive_and_resolve_explicitly() -> None:
    requested_type = getattr(event_models, "ExternalInputRequested", None)
    received_type = getattr(event_models, "ExternalInputReceived", None)
    approved_type = getattr(event_models, "ExternalInputApproved", None)
    rejected_type = getattr(event_models, "ExternalInputRejected", None)
    expired_type = getattr(event_models, "ExternalInputExpired", None)
    assert requested_type is not None
    assert received_type is not None
    assert approved_type is not None
    assert rejected_type is not None
    assert expired_type is not None

    proposal = _proposal("action-1", causal_parent_id=EVENT_IDS[1])
    requirement = ExternalInputRequirement(
        request_id="approval-1",
        request_kind=ExternalRequestKind.ACTION_APPROVAL,
        action_id="action-1",
    )
    decision = StrategyDecisionRecorded(
        **_envelope(3, causal_parent_id=EVENT_IDS[1]),
        event_type="STRATEGY_DECISION_RECORDED",
        trigger_sequence_no=2,
        strategy=_strategy(),
        proposals=(proposal,),
        external_requirements=(requirement,),
        directive=StrategyDirective.CONTINUE,
    )
    proposed = ActionProposed(
        **{
            **_envelope(4, causal_parent_id=EVENT_IDS[2]),
            "command_id": decision.command_id,
        },
        event_type="ACTION_PROPOSED",
        proposal=proposal.model_copy(
            update={"causal_parent_id": str(EVENT_IDS[2])}
        ),
    )
    request = ExternalRequest(
        request_id="approval-1",
        request_kind=ExternalRequestKind.ACTION_APPROVAL,
        action_id="action-1",
        payload_ref=None,
    )
    requested = requested_type(
        **{
            **_envelope(5, causal_parent_id=EVENT_IDS[3]),
            "command_id": decision.command_id,
        },
        event_type="EXTERNAL_INPUT_REQUESTED",
        request=request,
    )

    request_stream = (_planned(), _started(), decision, proposed, requested)
    waiting = replay_events(request_stream)
    assert waiting.phase is AttemptPhase.WAITING_EXTERNAL
    assert waiting.pending_external == (request,)
    assert waiting.actions[0].status is ActionStatus.PROPOSED

    received = received_type(
        **_envelope(6, causal_parent_id=EVENT_IDS[4]),
        event_type="EXTERNAL_INPUT_RECEIVED",
        request_id="approval-1",
        request_kind=ExternalRequestKind.ACTION_APPROVAL,
        action_id="action-1",
        response_kind=ExternalResponseKind.APPROVE,
    )
    still_waiting = apply_event(waiting, received)
    assert still_waiting.phase is AttemptPhase.WAITING_EXTERNAL
    assert still_waiting.pending_external == (request,)

    approved = approved_type(
        **{
            **_envelope(7, causal_parent_id=EVENT_IDS[5]),
            "command_id": received.command_id,
        },
        event_type="EXTERNAL_INPUT_APPROVED",
        request_id="approval-1",
        request_kind=ExternalRequestKind.ACTION_APPROVAL,
        action_id="action-1",
    )
    running = apply_event(still_waiting, approved)
    assert running.phase is AttemptPhase.RUNNING
    assert running.pending_external == ()

    with pytest.raises(StateTransitionError) as caught:
        replay_events((*request_stream, received, approved))
    assert caught.value.code == "invalid_external_transition"

    expired = expired_type(
        **_envelope(6, causal_parent_id=requested.event_id),
        event_type="EXTERNAL_INPUT_EXPIRED",
        request_id="approval-1",
        request_kind=ExternalRequestKind.ACTION_APPROVAL,
        action_id="action-1",
    )
    expired_state = apply_event(waiting, expired)
    assert expired_state.phase is AttemptPhase.RUNNING
    with pytest.raises(StateTransitionError) as caught:
        replay_events((*request_stream, expired))
    assert caught.value.code == "invalid_external_transition"

    mismatched_request = requested.model_copy(
        update={
            "request": request.model_copy(update={"request_id": "approval-other"})
        }
    )
    broken_command = proposed.model_copy(update={"command_id": COMMAND_IDS[3]})
    direct_approved = approved.model_copy(
        update={
            "sequence_no": 6,
            "event_id": EVENT_IDS[5],
            "command_id": COMMAND_IDS[5],
            "causal_parent_id": requested.event_id,
            "logical_time": 5,
        }
    )
    direct_rejected = rejected_type(
        **_envelope(6, causal_parent_id=requested.event_id),
        event_type="EXTERNAL_INPUT_REJECTED",
        request_id="approval-1",
        request_kind=ExternalRequestKind.ACTION_APPROVAL,
        action_id="action-1",
    )
    duplicate_received = received.model_copy(
        update={
            "sequence_no": 7,
            "event_id": EVENT_IDS[6],
            "causal_parent_id": received.event_id,
            "logical_time": 6,
        }
    )
    wrong_received_parent = received.model_copy(
        update={"causal_parent_id": proposed.event_id}
    )
    wrong_resolution_kind = rejected_type(
        **{
            **_envelope(7, causal_parent_id=received.event_id),
            "command_id": received.command_id,
        },
        event_type="EXTERNAL_INPUT_REJECTED",
        request_id="approval-1",
        request_kind=ExternalRequestKind.ACTION_APPROVAL,
        action_id="action-1",
    )
    wrong_expired_parent = expired.model_copy(
        update={"causal_parent_id": proposed.event_id}
    )
    for invalid_stream, expected_code in (
        ((*request_stream[:-1], mismatched_request), "invalid_external_transition"),
        (
            (_planned(), _started(), decision, broken_command, requested),
            "duplicate_command_id",
        ),
        ((*request_stream, direct_approved), "invalid_external_transition"),
        ((*request_stream, direct_rejected), "invalid_external_transition"),
        (
            (*request_stream, received, duplicate_received),
            "invalid_external_transition",
        ),
        ((*request_stream, wrong_received_parent), "invalid_external_transition"),
        (
            (*request_stream, received, wrong_resolution_kind),
            "invalid_external_transition",
        ),
        ((*request_stream, wrong_expired_parent), "invalid_external_transition"),
    ):
        with pytest.raises(StateTransitionError) as caught:
            replay_events(invalid_stream)
        assert caught.value.code == expected_code

    wrong_id = approved.model_copy(
        update={
            "event_id": EVENT_IDS[7],
            "sequence_no": 7,
            "logical_time": 6,
            "request_id": "approval-other",
        }
    )
    _assert_transition_code("illegal_transition", still_waiting, wrong_id)


def test_effective_strategy_triggers_distinguish_external_boundary_kinds() -> None:
    received_type = getattr(event_models, "ExternalInputReceived", None)
    requested_type = getattr(event_models, "ExternalInputRequested", None)
    trigger_fold = getattr(event_models, "effective_strategy_triggers", None)
    reconciled_type = getattr(event_models, "ActionOutcomeReconciled", None)
    assert received_type is not None
    assert requested_type is not None
    assert trigger_fold is not None
    assert reconciled_type is not None

    additional_received = received_type(
        **_envelope(3, causal_parent_id=EVENT_IDS[1]),
        event_type="EXTERNAL_INPUT_RECEIVED",
        request_id="input-1",
        request_kind=ExternalRequestKind.ADDITIONAL_INPUT,
        action_id=None,
        response_kind=ExternalResponseKind.PROVIDE_INPUT,
        response_ref=_artifact("attempts/attempt-1/input-response.json"),
    )
    approval_received = received_type(
        **_envelope(4, causal_parent_id=EVENT_IDS[2]),
        event_type="EXTERNAL_INPUT_RECEIVED",
        request_id="approval-1",
        request_kind=ExternalRequestKind.ACTION_APPROVAL,
        action_id="action-1",
        response_kind=ExternalResponseKind.APPROVE,
    )
    reconciled = reconciled_type(
        **_envelope(5, causal_parent_id=EVENT_IDS[3]),
        event_type="ACTION_OUTCOME_RECONCILED",
        action_id="action-1",
        outcome=ActionSucceededOutcome(
            status=ActionStatus.SUCCEEDED,
            action_id="action-1",
            result_ref=_artifact("attempts/attempt-1/reconciled.json"),
        ),
    )

    assert trigger_fold((additional_received, approval_received, reconciled)) == (
        additional_received,
        reconciled,
    )

    unknown = event_models.ActionOutcomeUnknown(
        **_envelope(6, causal_parent_id=EVENT_IDS[4]),
        event_type="ACTION_OUTCOME_UNKNOWN",
        action_id="action-1",
        outcome=ActionUnknownOutcome(
            status=ActionStatus.OUTCOME_UNKNOWN,
            action_id="action-1",
            error=_error(),
        ),
    )
    reconciliation_requested = requested_type(
        **_envelope(7, causal_parent_id=EVENT_IDS[5]),
        event_type="EXTERNAL_INPUT_REQUESTED",
        request=ExternalRequest(
            request_id="reconcile-1",
            request_kind=ExternalRequestKind.OUTCOME_RECONCILIATION,
            action_id="action-1",
            payload_ref=None,
        ),
    )
    paired_request = reconciliation_requested.model_copy(
        update={"command_id": unknown.command_id}
    )
    wrong_parent = paired_request.model_copy(
        update={"causal_parent_id": EVENT_IDS[4]}
    )
    assert trigger_fold((unknown, paired_request)) == ()
    assert trigger_fold((unknown, reconciliation_requested)) == (unknown,)
    assert trigger_fold((unknown, wrong_parent)) == (unknown,)
    assert trigger_fold((unknown,)) == (unknown,)


def test_additional_input_requirement_requires_exact_immediate_request() -> None:
    request_ref = _artifact("attempts/attempt-1/additional-request.json")
    requirement = ExternalInputRequirement(
        request_id="additional-1",
        request_kind=ExternalRequestKind.ADDITIONAL_INPUT,
        payload_ref=request_ref,
    )
    decision = StrategyDecisionRecorded(
        **_envelope(3, causal_parent_id=EVENT_IDS[1]),
        event_type="STRATEGY_DECISION_RECORDED",
        trigger_sequence_no=2,
        strategy=_strategy(),
        proposals=(),
        external_requirements=(requirement,),
        directive=StrategyDirective.CONTINUE,
    )
    request = event_models.ExternalInputRequested(
        **{
            **_envelope(4, causal_parent_id=decision.event_id),
            "command_id": decision.command_id,
        },
        event_type="EXTERNAL_INPUT_REQUESTED",
        request=ExternalRequest(
            request_id=requirement.request_id,
            request_kind=requirement.request_kind,
            action_id=None,
            payload_ref=request_ref,
        ),
    )
    assert replay_events((_planned(), _started(), decision, request)).phase is (
        AttemptPhase.WAITING_EXTERNAL
    )

    for invalid_request in (
        request.model_copy(
            update={
                "request": request.request.model_copy(
                    update={"request_id": "additional-other"}
                )
            }
        ),
        request.model_copy(update={"command_id": COMMAND_IDS[3]}),
    ):
        with pytest.raises(StateTransitionError) as caught:
            replay_events((_planned(), _started(), decision, invalid_request))
        assert caught.value.code == "invalid_external_transition"


def test_started_and_replay_produce_the_same_canonical_state() -> None:
    planned = apply_event(None, _planned())
    running = apply_event(planned, _started())
    replayed = replay_events((_planned(), _started()))

    assert running.phase is AttemptPhase.RUNNING
    assert running.revision == 2
    assert running.started_at == UTC_NOW + timedelta(seconds=1)
    assert canonical_state_bytes(replayed) == canonical_state_bytes(running)


@pytest.mark.parametrize(
    ("event", "phase", "has_result", "has_error"),
    [
        (_succeeded(), AttemptPhase.SUCCEEDED, True, False),
        (_failed(), AttemptPhase.FAILED, False, True),
        (_timed_out(), AttemptPhase.TIMED_OUT, False, True),
        (_interrupted(), AttemptPhase.INTERRUPTED, False, True),
    ],
)
def test_running_attempt_reaches_each_execution_terminal(
    event: object,
    phase: AttemptPhase,
    has_result: bool,
    has_error: bool,
) -> None:
    state = apply_event(_running_state(), event)  # type: ignore[arg-type]

    assert state.phase is phase
    assert state.revision == 3
    assert (state.result_ref is not None) is has_result
    assert (state.terminal_error is not None) is has_error
    assert state.finished_at == UTC_NOW + timedelta(seconds=2)


@pytest.mark.parametrize(
    ("event", "source_phase"),
    [
        (_succeeded(), AttemptPhase.RUNNING),
        (_failed(), AttemptPhase.RUNNING),
        (_cancelled(), AttemptPhase.CANCEL_REQUESTED),
        (_timed_out(), AttemptPhase.RUNNING),
        (_interrupted(), AttemptPhase.RUNNING),
    ],
)
@pytest.mark.parametrize(
    "action_status",
    [ActionStatus.PROPOSED, ActionStatus.ACCEPTED, ActionStatus.STARTED],
)
def test_attempt_terminal_event_rejects_nonterminal_action(
    event: object,
    source_phase: AttemptPhase,
    action_status: ActionStatus,
) -> None:
    state = _state_with_action(action_status, phase=source_phase)

    _assert_transition_code("illegal_transition", state, event)


@pytest.mark.parametrize(
    ("event", "source_phase", "terminal_phase"),
    [
        (_succeeded(), AttemptPhase.RUNNING, AttemptPhase.SUCCEEDED),
        (_failed(), AttemptPhase.RUNNING, AttemptPhase.FAILED),
        (_cancelled(), AttemptPhase.CANCEL_REQUESTED, AttemptPhase.CANCELLED),
        (_timed_out(), AttemptPhase.RUNNING, AttemptPhase.TIMED_OUT),
        (_interrupted(), AttemptPhase.RUNNING, AttemptPhase.INTERRUPTED),
    ],
)
def test_attempt_terminal_event_allows_terminal_action_observation(
    event: object,
    source_phase: AttemptPhase,
    terminal_phase: AttemptPhase,
) -> None:
    state = _state_with_action(ActionStatus.SUCCEEDED, phase=source_phase)

    terminal = apply_event(state, event)  # type: ignore[arg-type]

    assert terminal.phase is terminal_phase


def test_cancelled_is_legal_only_from_cancel_requested() -> None:
    running = _running_state()
    cancel_requested = AttemptState.model_validate(
        {**running.model_dump(mode="python"), "phase": AttemptPhase.CANCEL_REQUESTED}
    )

    cancelled = apply_event(cancel_requested, _cancelled())

    assert cancelled.phase is AttemptPhase.CANCELLED
    assert cancelled.result_ref is None
    assert cancelled.terminal_error is None
    assert cancelled.finished_at == UTC_NOW + timedelta(seconds=2)
    _assert_transition_code("illegal_transition", running, _cancelled())


def test_event_models_are_strict_and_omit_non_applicable_domain_fields() -> None:
    planned_fields = set(_planned().model_dump())
    started_fields = set(_started().model_dump())
    succeeded_fields = set(_succeeded().model_dump())

    assert "strategy_id" in planned_fields
    assert "causal_parent_id" not in planned_fields
    assert "strategy_id" not in started_fields
    assert "result_ref" not in started_fields
    assert "error" not in started_fields
    assert "result_ref" in succeeded_fields
    assert "error" not in succeeded_fields

    with pytest.raises(ValidationError, match="extra_forbidden"):
        _started(payload={"phase": "SUCCEEDED"})
    with pytest.raises(ValidationError, match="extra_forbidden"):
        _cancelled(error=_error())


def test_event_envelope_validates_uuid_sequence_logical_time_and_utc() -> None:
    common = {
        **_envelope(2, causal_parent_id=EVENT_IDS[0]),
        "event_type": "ATTEMPT_STARTED",
    }
    envelope = EventEnvelope(**common)
    assert isinstance(envelope.event_id, UUID)
    assert isinstance(envelope.command_id, UUID)

    for changes in (
        {"sequence_no": 0},
        {"logical_time": -1},
        {"wall_time_utc": datetime(2026, 7, 23, 8, 30)},
        {
            "wall_time_utc": datetime(
                2026, 7, 23, 16, 30, tzinfo=timezone(timedelta(hours=8))
            )
        },
    ):
        with pytest.raises(ValidationError):
            EventEnvelope(**{**common, **changes})


def test_root_and_non_root_events_enforce_causal_shape() -> None:
    with pytest.raises(ValidationError, match="causal"):
        _planned(causal_parent_id=EVENT_IDS[1])
    with pytest.raises(ValidationError, match="causal"):
        _started(causal_parent_id=None)


def test_terminal_event_domain_requirements_are_strict() -> None:
    with pytest.raises(ValidationError):
        _succeeded(result_ref=None)
    with pytest.raises(ValidationError):
        _failed(error=None)
    with pytest.raises(ValidationError):
        _timed_out(error={"message": "raw exception"})
    with pytest.raises(ValidationError):
        _interrupted(error={"traceback": "secret"})


def test_parse_domain_event_uses_version_then_discriminator() -> None:
    parsed = parse_domain_event(_started().model_dump(mode="json"))
    assert isinstance(parsed, AttemptStarted)

    wrong_type = _started().model_dump(mode="json")
    wrong_type["event_type"] = "ATTEMPT_DOES_NOT_EXIST"
    with pytest.raises(ValidationError):
        parse_domain_event(wrong_type)


def test_legacy_v1_strategy_decision_upcasts_without_rewriting_and_replays() -> None:
    legacy_payload = {
        "schema_version": 1,
        "event_id": str(EVENT_IDS[2]),
        "trial_id": "trial-1",
        "attempt_id": "attempt-1",
        "sequence_no": 3,
        "event_type": "STRATEGY_DECISION_RECORDED",
        "command_id": str(COMMAND_IDS[2]),
        "causal_parent_id": str(EVENT_IDS[1]),
        "logical_time": 2,
        "wall_time_utc": (UTC_NOW + timedelta(seconds=2)).isoformat(),
        "strategy": _strategy().model_dump(mode="json"),
    }
    original_payload = deepcopy(legacy_payload)

    parsed = parse_domain_event(legacy_payload)
    replayed = replay_events((_planned(), _started(), parsed))

    assert isinstance(parsed, StrategyDecisionRecorded)
    assert parsed.trigger_sequence_no == 2
    assert parsed.proposals == ()
    assert parsed.directive is StrategyDirective.CONTINUE
    assert parsed.result_ref is None
    assert parsed.error is None
    assert replayed.strategy == _strategy()
    assert legacy_payload == original_payload

    partial_payload = {**legacy_payload, "directive": "CONTINUE"}
    with pytest.raises(ValidationError):
        parse_domain_event(partial_payload)


def test_legacy_upcast_action_proposal_reserves_reconciliation_request_id() -> None:
    stream = _two_pending_trigger_stream()
    legacy_decision = stream[2].model_dump(mode="json")
    for field in {
        "trigger_sequence_no",
        "proposals",
        "external_requirements",
        "directive",
        "result_ref",
        "error",
    }:
        legacy_decision.pop(field, None)
    legacy_stream = (*stream[:2], legacy_decision, *stream[3:])
    assert replay_events(legacy_stream).phase is AttemptPhase.RUNNING
    request_id = outcome_reconciliation_request_id("attempt-1", "action-1")
    requirement = ExternalInputRequirement(
        request_id=request_id,
        request_kind=ExternalRequestKind.ADDITIONAL_INPUT,
    )
    decision = StrategyDecisionRecorded(
        **_envelope(8, causal_parent_id=EVENT_IDS[6]),
        event_type="STRATEGY_DECISION_RECORDED",
        trigger_sequence_no=5,
        strategy=_strategy(),
        proposals=(),
        external_requirements=(requirement,),
        directive=StrategyDirective.CONTINUE,
    )
    requested = event_models.ExternalInputRequested(
        **{
            **_envelope(9, causal_parent_id=decision.event_id),
            "command_id": decision.command_id,
        },
        event_type="EXTERNAL_INPUT_REQUESTED",
        request=ExternalRequest(
            request_id=request_id,
            request_kind=ExternalRequestKind.ADDITIONAL_INPUT,
        ),
    )

    with pytest.raises(StateTransitionError) as caught:
        replay_events((*legacy_stream, decision, requested))

    assert caught.value.code == "duplicate_external_request_id"


def test_legacy_origin_does_not_survive_a_tampered_cursor() -> None:
    legacy_payload = _strategy_decision(
        3,
        trigger_sequence_no=2,
        causal_parent_id=EVENT_IDS[1],
    ).model_dump(mode="json")
    for field in {
        "trigger_sequence_no",
        "proposals",
        "directive",
        "result_ref",
        "error",
    }:
        legacy_payload.pop(field)

    parsed = parse_domain_event(legacy_payload)
    tampered = parsed.model_copy(update={"trigger_sequence_no": 9})
    reparsed = parse_domain_event(tampered)

    assert parsed._legacy_cursor_origin is True
    assert reparsed._legacy_cursor_origin is False
    with pytest.raises(StateTransitionError) as caught:
        replay_events((_planned(), _started(), reparsed))
    assert caught.value.code == "invalid_strategy_cursor"


def test_legacy_origin_does_not_survive_a_tampered_strategy() -> None:
    legacy_payload = _strategy_decision(
        8,
        trigger_sequence_no=7,
        causal_parent_id=EVENT_IDS[6],
    ).model_dump(mode="json")
    for field in {
        "trigger_sequence_no",
        "proposals",
        "directive",
        "result_ref",
        "error",
    }:
        legacy_payload.pop(field)

    parsed = parse_domain_event(legacy_payload)
    value = {"queue": ["agent-b"], "round": 1}
    payload = _canonical_json(value)
    tampered_strategy = StrategyStateEnvelope(
        strategy_id="router",
        strategy_schema_version=1,
        value=value,
        content_hash=sha256(payload).hexdigest(),
        byte_size=len(payload),
    )
    tampered = parsed.model_copy(update={"strategy": tampered_strategy})
    reparsed = parse_domain_event(tampered)

    assert parsed._legacy_cursor_origin is True
    assert reparsed._legacy_cursor_origin is False
    with pytest.raises(StateTransitionError) as caught:
        replay_events((*_two_pending_trigger_stream(), reparsed))
    assert caught.value.code == "invalid_strategy_cursor"


@pytest.mark.parametrize("invalid_cursor", [9, 2, 7])
def test_replay_rejects_future_stale_or_skipped_strategy_cursor(
    invalid_cursor: int,
) -> None:
    stream = _two_pending_trigger_stream()
    invalid = _strategy_decision(
        8,
        trigger_sequence_no=invalid_cursor,
        causal_parent_id=EVENT_IDS[6],
    )

    with pytest.raises(StateTransitionError) as caught:
        replay_events((*stream, invalid))

    assert caught.value.code == "invalid_strategy_cursor"


def test_replay_rejects_duplicate_strategy_cursor() -> None:
    stream = _two_pending_trigger_stream()
    consumed = _strategy_decision(
        8,
        trigger_sequence_no=5,
        causal_parent_id=EVENT_IDS[6],
    )
    duplicate = _strategy_decision(
        9,
        trigger_sequence_no=5,
        causal_parent_id=EVENT_IDS[7],
    )

    with pytest.raises(StateTransitionError) as caught:
        replay_events((*stream, consumed, duplicate))

    assert caught.value.code == "invalid_strategy_cursor"


def test_replay_rejects_decision_after_terminal_strategy_directive() -> None:
    stream = _two_pending_trigger_stream()
    terminal = _strategy_decision(
        8,
        trigger_sequence_no=5,
        causal_parent_id=EVENT_IDS[6],
        directive=StrategyDirective.FAIL,
    )
    later = _strategy_decision(
        9,
        trigger_sequence_no=7,
        causal_parent_id=EVENT_IDS[7],
    )

    with pytest.raises(StateTransitionError) as caught:
        replay_events((*stream, terminal, later))

    assert caught.value.code == "terminal_strategy_decision"


def test_parse_domain_event_rejects_unknown_schema_with_safe_stable_error() -> None:
    payload = _started().model_dump(mode="json")
    payload["schema_version"] = 999
    payload["secret"] = "do-not-leak"

    with pytest.raises(UnsupportedEventSchema) as caught:
        parse_domain_event(payload)

    assert caught.value.code == "unsupported_event_schema"
    assert str(caught.value) == caught.value.safe_message
    assert "999" not in str(caught.value)
    assert "do-not-leak" not in str(caught.value)


def test_parse_domain_event_rejects_unknown_schema_on_copied_event_instance() -> None:
    event = _started().model_copy(update={"schema_version": 999})

    with pytest.raises(UnsupportedEventSchema) as caught:
        parse_domain_event(event)

    assert caught.value.code == "unsupported_event_schema"
    assert str(caught.value) == caught.value.safe_message
    assert "999" not in str(caught.value)


@pytest.mark.parametrize(
    "changes",
    [
        {"sequence_no": 0},
        {"logical_time": -1},
    ],
)
def test_parse_domain_event_revalidates_copied_event_constraints(
    changes: dict[str, object],
) -> None:
    event = _started().model_copy(update=changes)

    with pytest.raises(ValidationError):
        parse_domain_event(event)


def test_parse_domain_event_rejects_extra_on_copied_event_instance() -> None:
    event = _started().model_copy(update={"payload": {"phase": "SUCCEEDED"}})

    with pytest.raises(ValidationError, match="extra_forbidden"):
        parse_domain_event(event)


def test_parse_domain_event_rejects_extra_on_nested_copied_model() -> None:
    manifest = _artifact().model_copy(update={"storage_key": "secret-location"})
    event = _planned().model_copy(update={"manifest_ref": manifest})

    with pytest.raises(ValidationError, match="extra_forbidden"):
        parse_domain_event(event)


def test_apply_event_revalidates_copied_event_schema() -> None:
    event = _planned().model_copy(update={"schema_version": 999})

    with pytest.raises(UnsupportedEventSchema):
        apply_event(None, event)


@pytest.mark.parametrize(
    "manifest",
    [
        _artifact().model_copy(update={"relative_path": "../secret.json"}),
        _artifact().model_copy(update={"storage_key": "hidden-location"}),
    ],
    ids=("invalid-nested-artifact", "extra-on-nested-artifact"),
)
def test_apply_event_revalidates_nested_models_on_copied_event(
    manifest: ArtifactRef,
) -> None:
    event = _planned().model_copy(update={"manifest_ref": manifest})

    with pytest.raises(ValidationError):
        apply_event(None, event)


@pytest.mark.parametrize(
    ("event", "error_type"),
    [
        (
            _planned().model_copy(update={"schema_version": 999}),
            UnsupportedEventSchema,
        ),
        (
            _planned().model_copy(
                update={
                    "manifest_ref": _artifact().model_copy(
                        update={"relative_path": "../secret.json"}
                    )
                }
            ),
            ValidationError,
        ),
        (
            _planned().model_copy(
                update={
                    "manifest_ref": _artifact().model_copy(
                        update={"storage_key": "hidden-location"}
                    )
                }
            ),
            ValidationError,
        ),
    ],
    ids=("unsupported-schema", "invalid-nested-artifact", "nested-extra"),
)
def test_replay_revalidates_copied_events(
    event: AttemptPlanned,
    error_type: type[Exception],
) -> None:
    with pytest.raises(error_type):
        replay_events((event,))


def test_apply_event_revalidates_copied_attempt_state() -> None:
    invalid_state = _running_state().model_copy(
        update={"unknown_snapshot_field": "hidden"}
    )

    with pytest.raises(ValidationError, match="extra_forbidden"):
        apply_event(invalid_state, _succeeded())


def test_only_planned_accepts_missing_state_and_only_once() -> None:
    _assert_transition_code("state_required", None, _started())
    _assert_transition_code("state_already_exists", apply_event(None, _planned()), _planned())


def test_planned_requires_root_sequence_one() -> None:
    event = _planned(sequence_no=2, logical_time=1)
    _assert_transition_code("invalid_initial_sequence", None, event)


@pytest.mark.parametrize(
    ("event", "code"),
    [
        (_started(sequence_no=2), "stale_sequence"),
        (_started(sequence_no=4), "sequence_gap"),
    ],
)
def test_reducer_rejects_duplicate_or_gapped_sequence(event: object, code: str) -> None:
    _assert_transition_code(code, _running_state(), event)


@pytest.mark.parametrize(
    "changes",
    [
        {"attempt_id": "attempt-other"},
        {"trial_id": "trial-other"},
    ],
)
def test_reducer_rejects_attempt_identity_mismatch(changes: dict[str, object]) -> None:
    event = _succeeded(**changes)
    _assert_transition_code("identity_mismatch", _running_state(), event)


def test_started_from_running_is_illegal() -> None:
    event = _started(sequence_no=3, event_id=EVENT_IDS[2], command_id=COMMAND_IDS[2])
    _assert_transition_code("illegal_transition", _running_state(), event)


@pytest.mark.parametrize("second_terminal", [_failed(sequence_no=4), _timed_out(sequence_no=4)])
def test_terminal_events_are_exclusive(second_terminal: object) -> None:
    terminal = apply_event(_running_state(), _succeeded())
    _assert_transition_code("terminal_state", terminal, second_terminal)


def test_events_after_execution_terminal_are_rejected() -> None:
    terminal = apply_event(_running_state(), _failed())
    later = _started(
        sequence_no=4,
        event_id=EVENT_IDS[3],
        command_id=COMMAND_IDS[3],
        causal_parent_id=EVENT_IDS[2],
    )
    _assert_transition_code("terminal_state", terminal, later)


def test_replay_rejects_empty_stream() -> None:
    with pytest.raises(StateTransitionError) as caught:
        replay_events(())
    assert caught.value.code == "empty_replay"


def test_replay_rejects_duplicate_event_ids_without_global_history() -> None:
    duplicate = _succeeded(event_id=EVENT_IDS[1])
    with pytest.raises(StateTransitionError) as caught:
        replay_events((_planned(), _started(), duplicate))
    assert caught.value.code == "duplicate_event_id"

    assert replay_events((_planned(), _started())).phase is AttemptPhase.RUNNING


def test_replay_requires_causal_parent_to_have_appeared() -> None:
    orphan = _started(causal_parent_id=EVENT_IDS[8])
    with pytest.raises(StateTransitionError) as caught:
        replay_events((_planned(), orphan))
    assert caught.value.code == "causal_parent_not_seen"


def test_replay_requires_causal_parent_from_same_attempt() -> None:
    child = _started(attempt_id="attempt-other")
    with pytest.raises(StateTransitionError) as caught:
        replay_events((_planned(), child))
    assert caught.value.code == "causal_parent_wrong_attempt"


def test_replay_rejects_same_attempt_id_parent_from_different_trial() -> None:
    child = _started(trial_id="trial-other")
    with pytest.raises(StateTransitionError) as caught:
        replay_events((_planned(), child))
    assert caught.value.code == "causal_parent_wrong_attempt"


def test_replay_requires_causal_parent_with_earlier_sequence() -> None:
    child = _started(
        sequence_no=1,
        event_id=EVENT_IDS[1],
        causal_parent_id=EVENT_IDS[0],
    )
    with pytest.raises(StateTransitionError) as caught:
        replay_events((_planned(), child))
    assert caught.value.code == "causal_parent_not_earlier"


def test_transition_errors_do_not_leak_identity_phase_or_sequence_values() -> None:
    event = _succeeded(attempt_id="secret-attempt")
    with pytest.raises(StateTransitionError) as caught:
        apply_event(_running_state(), event)

    message = str(caught.value)
    assert message == caught.value.safe_message
    assert "secret-attempt" not in message
    assert "RUNNING" not in message
    assert "3" not in message
