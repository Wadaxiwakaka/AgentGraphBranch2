from __future__ import annotations

from datetime import datetime, timedelta, timezone
from hashlib import sha256
from uuid import UUID

import pytest
from pydantic import TypeAdapter, ValidationError

from experiment_system.actions import (
    ActionCancelledOutcome,
    ActionExecutionStatus,
    ActionFailedOutcome,
    ActionOutcome,
    ActionProposal,
    ActionSucceededOutcome,
    ActionTimedOutOutcome,
    ActionType,
    ActionUnknownOutcome,
    NormalizedAction,
    ResourceKind,
    ResourceRequest,
)
from experiment_system.events import (
    ActionAccepted,
    ActionCancelled,
    ActionFailed,
    ActionOutcomeReconciled,
    ActionOutcomeUnknown,
    ActionProposed,
    ActionRejected,
    ActionStarted,
    ActionSucceeded,
    ActionTimedOut,
    AttemptPlanned,
    AttemptStarted,
    InvocationCompleted,
    InvocationFailed,
    InvocationRequested,
    InvocationStarted,
    parse_domain_event,
)
from experiment_system.reducer import StateTransitionError, apply_event
from experiment_system.state import (
    ActionState,
    ActionStatus,
    ArtifactRef,
    AttemptState,
    BudgetState,
    ErrorSummary,
    InvocationState,
    InvocationStatus,
    RecoveryPolicy,
    ResourceBudget,
    StrategyStateEnvelope,
)


UTC_NOW = datetime(2026, 7, 23, 8, 30, tzinfo=timezone.utc)
EVENT_IDS = tuple(UUID(f"00000000-0000-0000-0000-{index:012d}") for index in range(1, 40))
COMMAND_IDS = tuple(UUID(f"10000000-0000-0000-0000-{index:012d}") for index in range(1, 40))
HASH = "a" * 64


def _artifact(path: str) -> ArtifactRef:
    return ArtifactRef(
        capture_class="full",
        content_hash=HASH,
        media_type="application/json",
        byte_size=12,
        relative_path=path,
    )


def _error(code: str = "BACKEND_FAILURE") -> ErrorSummary:
    return ErrorSummary(
        code=code,
        retryable=True,
        safe_message="The backend did not complete the request.",
    )


def _resource_requests() -> tuple[ResourceRequest, ...]:
    return (
        ResourceRequest(resource=ResourceKind.COORDINATION_ACTIONS, amount=1),
        ResourceRequest(resource=ResourceKind.MODEL_CALLS, amount=1),
    )


def _proposal(**changes: object) -> ActionProposal:
    values: dict[str, object] = {
        "action_id": "action-1",
        "action_type": ActionType.INVOKE_AGENT,
        "actor": "engine",
        "target_ids": ("worker-a",),
        "invocation_id": "invocation-1",
        "causal_parent_id": str(EVENT_IDS[1]),
        "payload_ref": _artifact("attempts/attempt-1/actions/action-1.json"),
        "recovery_policy": RecoveryPolicy.REPLAY_SAFE,
        "requested_timeout": 30,
        "batch_id": None,
        "retry_of_action_id": None,
        "call_depth": 0,
        "resource_requests": _resource_requests(),
    }
    values.update(changes)
    return ActionProposal(**values)


def _normalized(**changes: object) -> NormalizedAction:
    values = _proposal().model_dump(mode="python")
    values.update(
        {
            "reservation_id": "reservation-1",
            "idempotency_key": "idem-action-1",
            **changes,
        }
    )
    return NormalizedAction(**values)


def _envelope(sequence_no: int, **changes: object) -> dict[str, object]:
    values: dict[str, object] = {
        "schema_version": 1,
        "event_id": EVENT_IDS[sequence_no - 1],
        "trial_id": "trial-1",
        "attempt_id": "attempt-1",
        "sequence_no": sequence_no,
        "command_id": COMMAND_IDS[sequence_no - 1],
        "causal_parent_id": EVENT_IDS[sequence_no - 2] if sequence_no > 1 else None,
        "logical_time": sequence_no - 1,
        "wall_time_utc": UTC_NOW + timedelta(seconds=sequence_no - 1),
    }
    values.update(changes)
    return values


def _planned() -> AttemptPlanned:
    strategy_value = {"queue": ["worker-a"]}
    payload = b'{"queue":["worker-a"]}'
    return AttemptPlanned(
        **_envelope(1),
        event_type="ATTEMPT_PLANNED",
        state_schema_version=1,
        experiment_id="experiment-1",
        strategy_id="router",
        manifest_ref=_artifact("attempts/attempt-1/manifest.json"),
        strategy=StrategyStateEnvelope(
            strategy_id="router",
            strategy_schema_version=1,
            value=strategy_value,
            content_hash=sha256(payload).hexdigest(),
            byte_size=len(payload),
        ),
        budget=BudgetState(
            resources=(ResourceBudget(resource="actions", limit=10),),
            deadline_at=UTC_NOW + timedelta(hours=1),
            max_call_depth=4,
            max_concurrent_actions=2,
        ),
    )


def _attempt_started() -> AttemptStarted:
    return AttemptStarted(**_envelope(2), event_type="ATTEMPT_STARTED")


def _action_proposed(sequence_no: int = 3, **changes: object) -> ActionProposed:
    values: dict[str, object] = {
        **_envelope(sequence_no),
        "event_type": "ACTION_PROPOSED",
        "proposal": _proposal(),
    }
    values.update(changes)
    return ActionProposed(**values)


def _action_accepted(sequence_no: int = 4, **changes: object) -> ActionAccepted:
    values: dict[str, object] = {
        **_envelope(sequence_no),
        "event_type": "ACTION_ACCEPTED",
        "action": _normalized(),
    }
    values.update(changes)
    return ActionAccepted(**values)


def _action_started(sequence_no: int = 5, action_id: str = "action-1") -> ActionStarted:
    return ActionStarted(
        **_envelope(sequence_no), event_type="ACTION_STARTED", action_id=action_id
    )


def _running_state() -> AttemptState:
    return apply_event(apply_event(None, _planned()), _attempt_started())


def _accepted_state() -> AttemptState:
    proposed = apply_event(_running_state(), _action_proposed())
    requested = apply_event(proposed, _invocation_requested(sequence_no=4))
    return apply_event(requested, _action_accepted(sequence_no=5))


def _started_action_state() -> AttemptState:
    return apply_event(_accepted_state(), _action_started(sequence_no=6))


def _assert_illegal(state: AttemptState, event: object) -> None:
    with pytest.raises(StateTransitionError) as caught:
        apply_event(state, event)  # type: ignore[arg-type]
    assert caught.value.code == "illegal_transition"


def _at_sequence(event: object, sequence_no: int) -> object:
    return event.model_copy(update=_envelope(sequence_no))  # type: ignore[attr-defined]


def test_action_status_alias_is_the_state_enum_and_proposal_is_normalized() -> None:
    proposal = _proposal()

    assert ActionExecutionStatus is ActionStatus
    assert proposal.action_type is ActionType.INVOKE_AGENT
    assert proposal.actor == "engine"
    assert proposal.target_ids == ("worker-a",)
    assert proposal.recovery_policy is RecoveryPolicy.REPLAY_SAFE
    assert proposal.resource_requests[0].resource is ResourceKind.COORDINATION_ACTIONS
    assert {status.value for status in InvocationStatus} == {
        "REQUESTED",
        "RUNNING",
        "COMPLETED",
        "FAILED",
    }

    with pytest.raises(ValidationError, match="extra_forbidden"):
        ActionProposal(**{**proposal.model_dump(mode="python"), "type": "INVOKE_AGENT"})


@pytest.mark.parametrize(
    ("changes", "match"),
    [
        ({"action_id": ""}, "action_id"),
        ({"action_id": "action/1"}, "action_id"),
        ({"actor": "bad actor"}, "actor"),
        ({"target_ids": ()}, "target"),
        ({"target_ids": ("worker-a", "worker-a")}, "unique"),
        ({"target_ids": ("worker/a",)}, "target"),
        ({"target_ids": (object(),)}, "string"),
        ({"invocation_id": "invocation/1"}, "invocation"),
        ({"batch_id": "batch 1"}, "batch"),
        ({"retry_of_action_id": "action-1"}, "retry"),
        ({"requested_timeout": 0}, "greater than 0"),
        ({"requested_timeout": -1}, "greater than 0"),
        ({"requested_timeout": True}, "integer"),
        ({"call_depth": -1}, "greater than or equal to 0"),
        ({"call_depth": True}, "integer"),
    ],
)
def test_action_proposal_rejects_invalid_ids_targets_timeout_and_depth(
    changes: dict[str, object], match: str
) -> None:
    with pytest.raises(ValidationError, match=match):
        _proposal(**changes)


def test_both_action_types_require_exactly_one_target() -> None:
    send = _proposal(action_type=ActionType.SEND_MESSAGE, invocation_id=None)
    assert send.target_ids == ("worker-a",)

    with pytest.raises(ValidationError, match="exactly one target"):
        _proposal(
            action_type=ActionType.SEND_MESSAGE,
            invocation_id=None,
            target_ids=("a", "b"),
        )
    with pytest.raises(ValidationError, match="exactly one target"):
        _proposal(target_ids=("a", "b"))


@pytest.mark.parametrize("amount", [-1, True, 1.5, "1"])
def test_resource_request_amount_is_a_nonnegative_strict_integer(amount: object) -> None:
    with pytest.raises(ValidationError):
        ResourceRequest(resource=ResourceKind.MODEL_CALLS, amount=amount)


def test_resource_requests_are_required_and_unique_by_resource() -> None:
    with pytest.raises(ValidationError):
        _proposal(resource_requests=None)
    duplicate = ResourceRequest(resource=ResourceKind.MODEL_CALLS, amount=1)
    with pytest.raises(ValidationError, match="unique"):
        _proposal(resource_requests=(duplicate, duplicate))


@pytest.mark.parametrize("field", ["reservation_id", "idempotency_key"])
def test_normalized_action_requires_stable_nonempty_acceptance_ids(field: str) -> None:
    with pytest.raises(ValidationError, match=field):
        _normalized(**{field: ""})
    with pytest.raises(ValidationError, match=field):
        _normalized(**{field: "bad/id"})


def _action_state(
    status: ActionStatus = ActionStatus.PROPOSED,
    **changes: object,
) -> ActionState:
    values: dict[str, object] = {
        "action_id": "action-1",
        "action_type": ActionType.SEND_MESSAGE.value,
        "actor_id": "engine",
        "target_ids": ("worker-a",),
        "status": status,
        "causal_parent_id": str(EVENT_IDS[1]),
        "invocation_id": None,
        "payload_ref": None,
        "result_ref": None,
        "idempotency_key": None,
        "recovery_policy": RecoveryPolicy.REPLAY_SAFE,
        "reservation_id": None,
        "retry_of_action_id": None,
        "error_code": None,
    }
    if status is ActionStatus.REJECTED:
        values.update(error_code="ACTION_REJECTED", error=_error("ACTION_REJECTED"))
    elif status is not ActionStatus.PROPOSED:
        values.update(
            reservation_id="reservation-1",
            idempotency_key="idem-action-1",
        )
    if status is ActionStatus.SUCCEEDED:
        values["result_ref"] = _artifact("attempts/attempt-1/result.json")
    elif status in {
        ActionStatus.FAILED,
        ActionStatus.TIMED_OUT,
        ActionStatus.CANCELLED,
        ActionStatus.OUTCOME_UNKNOWN,
    }:
        values.update(error_code=status.value, error=_error(status.value))
    values.update(changes)
    return ActionState(**values)


@pytest.mark.parametrize("status", list(ActionStatus))
def test_action_state_accepts_complete_status_shapes(status: ActionStatus) -> None:
    action = _action_state(status)

    assert action.status is status


@pytest.mark.parametrize(
    ("status", "invalid_fields"),
    [
        (ActionStatus.PROPOSED, {"reservation_id": "reservation-1"}),
        (ActionStatus.PROPOSED, {"idempotency_key": "idem-action-1"}),
        (
            ActionStatus.PROPOSED,
            {"result_ref": _artifact("attempts/attempt-1/result.json")},
        ),
        (ActionStatus.PROPOSED, {"error": _error()}),
        (ActionStatus.REJECTED, {"error": None}),
        (ActionStatus.REJECTED, {"error_code": "OTHER"}),
        (ActionStatus.REJECTED, {"reservation_id": "reservation-1"}),
        (
            ActionStatus.REJECTED,
            {"result_ref": _artifact("attempts/attempt-1/result.json")},
        ),
        (ActionStatus.ACCEPTED, {"reservation_id": None}),
        (ActionStatus.ACCEPTED, {"idempotency_key": None}),
        (
            ActionStatus.ACCEPTED,
            {"result_ref": _artifact("attempts/attempt-1/result.json")},
        ),
        (ActionStatus.ACCEPTED, {"error": _error()}),
        (ActionStatus.STARTED, {"reservation_id": None}),
        (ActionStatus.STARTED, {"idempotency_key": None}),
        (ActionStatus.SUCCEEDED, {"result_ref": None}),
        (ActionStatus.SUCCEEDED, {"error": _error()}),
        (ActionStatus.SUCCEEDED, {"reservation_id": None}),
        (ActionStatus.FAILED, {"error": None}),
        (ActionStatus.FAILED, {"error_code": "OTHER"}),
        (
            ActionStatus.FAILED,
            {"result_ref": _artifact("attempts/attempt-1/result.json")},
        ),
        (ActionStatus.TIMED_OUT, {"error": None}),
        (ActionStatus.CANCELLED, {"error_code": "OTHER"}),
        (ActionStatus.OUTCOME_UNKNOWN, {"idempotency_key": None}),
    ],
)
def test_action_state_rejects_incomplete_or_contradictory_status_shapes(
    status: ActionStatus,
    invalid_fields: dict[str, object],
) -> None:
    with pytest.raises(ValidationError, match="status lifecycle"):
        _action_state(status, **invalid_fields)


def test_action_outcome_is_a_strict_status_discriminated_union() -> None:
    adapter = TypeAdapter(ActionOutcome)
    result_ref = _artifact("attempts/attempt-1/actions/action-1-result.json")

    success = adapter.validate_python(
        {"status": "SUCCEEDED", "action_id": "action-1", "result_ref": result_ref}
    )
    assert isinstance(success, ActionSucceededOutcome)
    assert success.result_ref == result_ref

    for status, expected_type in (
        ("FAILED", ActionFailedOutcome),
        ("TIMED_OUT", ActionTimedOutOutcome),
        ("CANCELLED", ActionCancelledOutcome),
        ("OUTCOME_UNKNOWN", ActionUnknownOutcome),
    ):
        outcome = adapter.validate_python(
            {"status": status, "action_id": "action-1", "error": _error()}
        )
        assert isinstance(outcome, expected_type)

    with pytest.raises(ValidationError):
        adapter.validate_python(
            {
                "status": "SUCCEEDED",
                "action_id": "action-1",
                "result": {"inline": "forbidden"},
            }
        )
    with pytest.raises(ValidationError):
        adapter.validate_python(
            {
                "status": "FAILED",
                "action_id": "action-1",
                "error": {"traceback": "secret"},
            }
        )


def test_action_event_models_match_envelope_and_nested_action_identity() -> None:
    with pytest.raises(ValidationError, match="causal"):
        _action_proposed(causal_parent_id=EVENT_IDS[0])
    with pytest.raises(ValidationError, match="action_id"):
        ActionSucceeded(
            **_envelope(6),
            event_type="ACTION_SUCCEEDED",
            action_id="action-other",
            outcome=ActionSucceededOutcome(
                status=ActionStatus.SUCCEEDED,
                action_id="action-1",
                result_ref=_artifact("attempts/attempt-1/result.json"),
            ),
        )


def test_action_lifecycle_reduces_through_success() -> None:
    state = _running_state()
    state = apply_event(state, _action_proposed(sequence_no=3))
    state = apply_event(state, _invocation_requested(sequence_no=4))
    state = apply_event(state, _action_accepted(sequence_no=5))
    state = apply_event(state, _action_started(sequence_no=6))
    state = apply_event(
        state,
        InvocationStarted(
            **_envelope(7),
            event_type="INVOCATION_STARTED",
            action_id="action-1",
            invocation_id="invocation-1",
        ),
    )
    state = apply_event(
        state,
        InvocationCompleted(
            **_envelope(8),
            event_type="INVOCATION_COMPLETED",
            action_id="action-1",
            invocation_id="invocation-1",
            latest_context_ref=_artifact("attempts/attempt-1/context/completed.json"),
        ),
    )
    state = apply_event(
        state,
        ActionSucceeded(
            **_envelope(9),
            event_type="ACTION_SUCCEEDED",
            action_id="action-1",
            outcome=ActionSucceededOutcome(
                status=ActionStatus.SUCCEEDED,
                action_id="action-1",
                result_ref=_artifact("attempts/attempt-1/result.json"),
            ),
        ),
    )

    assert state.actions[0].status is ActionStatus.SUCCEEDED
    assert state.actions[0].result_ref is not None
    assert state.invocations[0].status is InvocationStatus.COMPLETED


def test_action_can_be_rejected_only_while_proposed() -> None:
    proposed = apply_event(_running_state(), _action_proposed())
    rejected = apply_event(
        proposed,
        ActionRejected(
            **_envelope(4),
            event_type="ACTION_REJECTED",
            action_id="action-1",
            error=_error("ACTION_REJECTED"),
        ),
    )
    assert rejected.actions[0].status is ActionStatus.REJECTED
    _assert_illegal(rejected, _action_accepted(sequence_no=5))


def test_invoke_action_cannot_be_rejected_after_its_invocation_is_requested() -> None:
    proposed = apply_event(_running_state(), _action_proposed())
    requested = apply_event(proposed, _invocation_requested(sequence_no=4))

    _assert_illegal(
        requested,
        ActionRejected(
            **_envelope(5),
            event_type="ACTION_REJECTED",
            action_id="action-1",
            error=_error("ACTION_REJECTED"),
        ),
    )


@pytest.mark.parametrize(
    "event",
    [
        ActionFailed(
            **_envelope(7),
            event_type="ACTION_FAILED",
            action_id="action-1",
            outcome=ActionFailedOutcome(
                status=ActionStatus.FAILED, action_id="action-1", error=_error()
            ),
        ),
        ActionTimedOut(
            **_envelope(7),
            event_type="ACTION_TIMED_OUT",
            action_id="action-1",
            outcome=ActionTimedOutOutcome(
                status=ActionStatus.TIMED_OUT, action_id="action-1", error=_error()
            ),
        ),
        ActionCancelled(
            **_envelope(7),
            event_type="ACTION_CANCELLED",
            action_id="action-1",
            outcome=ActionCancelledOutcome(
                status=ActionStatus.CANCELLED, action_id="action-1", error=_error()
            ),
        ),
        ActionOutcomeUnknown(
            **_envelope(7),
            event_type="ACTION_OUTCOME_UNKNOWN",
            action_id="action-1",
            outcome=ActionUnknownOutcome(
                status=ActionStatus.OUTCOME_UNKNOWN, action_id="action-1", error=_error()
            ),
        ),
    ],
)
def test_started_action_accepts_each_non_success_terminal(event: object) -> None:
    state = apply_event(_started_action_state(), event)  # type: ignore[arg-type]
    assert state.actions[0].status.value == event.outcome.status.value  # type: ignore[attr-defined]
    assert state.actions[0].error_code == "BACKEND_FAILURE"


def test_invoke_action_success_requires_completed_invocation() -> None:
    succeeded = ActionSucceeded(
        **_envelope(7),
        event_type="ACTION_SUCCEEDED",
        action_id="action-1",
        outcome=ActionSucceededOutcome(
            status=ActionStatus.SUCCEEDED,
            action_id="action-1",
            result_ref=_artifact("attempts/attempt-1/result.json"),
        ),
    )
    _assert_illegal(_started_action_state(), succeeded)

    running = apply_event(
        _started_action_state(),
        InvocationStarted(
            **_envelope(7),
            event_type="INVOCATION_STARTED",
            action_id="action-1",
            invocation_id="invocation-1",
        ),
    )
    _assert_illegal(running, _at_sequence(succeeded, 8))


@pytest.mark.parametrize(
    "event",
    [
        ActionFailed(
            **_envelope(8),
            event_type="ACTION_FAILED",
            action_id="action-1",
            outcome=ActionFailedOutcome(
                status=ActionStatus.FAILED, action_id="action-1", error=_error()
            ),
        ),
        ActionTimedOut(
            **_envelope(8),
            event_type="ACTION_TIMED_OUT",
            action_id="action-1",
            outcome=ActionTimedOutOutcome(
                status=ActionStatus.TIMED_OUT, action_id="action-1", error=_error()
            ),
        ),
        ActionCancelled(
            **_envelope(8),
            event_type="ACTION_CANCELLED",
            action_id="action-1",
            outcome=ActionCancelledOutcome(
                status=ActionStatus.CANCELLED, action_id="action-1", error=_error()
            ),
        ),
    ],
)
@pytest.mark.parametrize("invocation_outcome", ["RUNNING", "COMPLETED"])
def test_invoke_action_non_success_terminal_rejects_incompatible_invocation_status(
    event: object, invocation_outcome: str
) -> None:
    state = apply_event(
        _started_action_state(),
        InvocationStarted(
            **_envelope(7),
            event_type="INVOCATION_STARTED",
            action_id="action-1",
            invocation_id="invocation-1",
        ),
    )
    if invocation_outcome == "COMPLETED":
        state = apply_event(
            state,
            InvocationCompleted(
                **_envelope(8),
                event_type="INVOCATION_COMPLETED",
                action_id="action-1",
                invocation_id="invocation-1",
                latest_context_ref=None,
            ),
        )
        event = _at_sequence(event, 9)

    _assert_illegal(state, event)


@pytest.mark.parametrize(
    "event",
    [
        ActionFailed(
            **_envelope(9),
            event_type="ACTION_FAILED",
            action_id="action-1",
            outcome=ActionFailedOutcome(
                status=ActionStatus.FAILED, action_id="action-1", error=_error()
            ),
        ),
        ActionTimedOut(
            **_envelope(9),
            event_type="ACTION_TIMED_OUT",
            action_id="action-1",
            outcome=ActionTimedOutOutcome(
                status=ActionStatus.TIMED_OUT, action_id="action-1", error=_error()
            ),
        ),
        ActionCancelled(
            **_envelope(9),
            event_type="ACTION_CANCELLED",
            action_id="action-1",
            outcome=ActionCancelledOutcome(
                status=ActionStatus.CANCELLED, action_id="action-1", error=_error()
            ),
        ),
    ],
)
def test_invoke_action_non_success_terminal_is_allowed_after_invocation_failure(
    event: object,
) -> None:
    state = apply_event(
        _started_action_state(),
        InvocationStarted(
            **_envelope(7),
            event_type="INVOCATION_STARTED",
            action_id="action-1",
            invocation_id="invocation-1",
        ),
    )
    state = apply_event(
        state,
        InvocationFailed(
            **_envelope(8),
            event_type="INVOCATION_FAILED",
            action_id="action-1",
            invocation_id="invocation-1",
            error=_error("INVOCATION_FAILED"),
        ),
    )
    state = apply_event(state, event)  # type: ignore[arg-type]

    assert state.actions[0].status.value == event.outcome.status.value  # type: ignore[attr-defined]
    assert state.invocations[0].status is InvocationStatus.FAILED


def test_outcome_unknown_allows_requested_or_running_invocation_without_mutating_it() -> None:
    event = ActionOutcomeUnknown(
        **_envelope(7),
        event_type="ACTION_OUTCOME_UNKNOWN",
        action_id="action-1",
        outcome=ActionUnknownOutcome(
            status=ActionStatus.OUTCOME_UNKNOWN,
            action_id="action-1",
            error=_error("OBSERVATION_LOST"),
        ),
    )
    requested = apply_event(_started_action_state(), event)
    assert requested.invocations[0].status is InvocationStatus.REQUESTED

    running = apply_event(
        _started_action_state(),
        InvocationStarted(
            **_envelope(7),
            event_type="INVOCATION_STARTED",
            action_id="action-1",
            invocation_id="invocation-1",
        ),
    )
    unknown = apply_event(running, _at_sequence(event, 8))  # type: ignore[arg-type]
    assert unknown.invocations[0].status is InvocationStatus.RUNNING


@pytest.mark.parametrize("invocation_outcome", ["COMPLETED", "FAILED"])
def test_outcome_unknown_rejects_conclusive_invocation_status(
    invocation_outcome: str,
) -> None:
    state = apply_event(
        _started_action_state(),
        InvocationStarted(
            **_envelope(7),
            event_type="INVOCATION_STARTED",
            action_id="action-1",
            invocation_id="invocation-1",
        ),
    )
    if invocation_outcome == "COMPLETED":
        invocation_event: object = InvocationCompleted(
            **_envelope(8),
            event_type="INVOCATION_COMPLETED",
            action_id="action-1",
            invocation_id="invocation-1",
            latest_context_ref=None,
        )
    else:
        invocation_event = InvocationFailed(
            **_envelope(8),
            event_type="INVOCATION_FAILED",
            action_id="action-1",
            invocation_id="invocation-1",
            error=_error("INVOCATION_FAILED"),
        )
    state = apply_event(state, invocation_event)  # type: ignore[arg-type]

    _assert_illegal(
        state,
        ActionOutcomeUnknown(
            **_envelope(9),
            event_type="ACTION_OUTCOME_UNKNOWN",
            action_id="action-1",
            outcome=ActionUnknownOutcome(
                status=ActionStatus.OUTCOME_UNKNOWN,
                action_id="action-1",
                error=_error("OBSERVATION_LOST"),
            ),
        ),
    )


def test_reducer_rejects_jumps_unknown_before_started_and_second_terminal() -> None:
    _assert_illegal(_running_state(), _action_started(sequence_no=3))
    _assert_illegal(
        apply_event(_running_state(), _action_proposed()),
        _action_started(sequence_no=4),
    )
    unknown = ActionOutcomeUnknown(
        **_envelope(6),
        event_type="ACTION_OUTCOME_UNKNOWN",
        action_id="action-1",
        outcome=ActionUnknownOutcome(
            status=ActionStatus.OUTCOME_UNKNOWN, action_id="action-1", error=_error()
        ),
    )
    _assert_illegal(_accepted_state(), unknown)

    invocation_started = apply_event(
        _started_action_state(),
        InvocationStarted(
            **_envelope(7),
            event_type="INVOCATION_STARTED",
            action_id="action-1",
            invocation_id="invocation-1",
        ),
    )
    invocation_completed = apply_event(
        invocation_started,
        InvocationCompleted(
            **_envelope(8),
            event_type="INVOCATION_COMPLETED",
            action_id="action-1",
            invocation_id="invocation-1",
            latest_context_ref=None,
        ),
    )
    succeeded = apply_event(
        invocation_completed,
        ActionSucceeded(
            **_envelope(9),
            event_type="ACTION_SUCCEEDED",
            action_id="action-1",
            outcome=ActionSucceededOutcome(
                status=ActionStatus.SUCCEEDED,
                action_id="action-1",
                result_ref=_artifact("attempts/attempt-1/result.json"),
            ),
        ),
    )
    second = ActionFailed(
        **_envelope(10),
        event_type="ACTION_FAILED",
        action_id="action-1",
        outcome=ActionFailedOutcome(
            status=ActionStatus.FAILED, action_id="action-1", error=_error()
        ),
    )
    _assert_illegal(succeeded, second)


@pytest.mark.parametrize(
    "changes",
    [
        {"action_type": ActionType.SEND_MESSAGE},
        {"actor": "other"},
        {"target_ids": ("worker-b",)},
        {"invocation_id": "invocation-other"},
        {"payload_ref": None},
        {"recovery_policy": RecoveryPolicy.NON_REPLAYABLE},
        {"retry_of_action_id": "action-previous"},
        {"causal_parent_id": str(EVENT_IDS[8])},
        {"requested_timeout": 999},
        {"batch_id": "batch-other"},
        {"call_depth": 2},
        {
            "resource_requests": (
                ResourceRequest(
                    resource=ResourceKind.COORDINATION_ACTIONS,
                    amount=2,
                ),
            )
        },
    ],
)
def test_acceptance_cannot_change_proposal_execution_identity(changes: dict[str, object]) -> None:
    proposed = apply_event(_running_state(), _action_proposed())
    requested = apply_event(proposed, _invocation_requested(sequence_no=4))
    _assert_illegal(
        requested,
        _action_accepted(sequence_no=5, action=_normalized(**changes)),
    )


def test_reconciliation_is_separate_single_fact_only_for_unknown_outcome() -> None:
    unknown = apply_event(
        _started_action_state(),
        ActionOutcomeUnknown(
            **_envelope(7),
            event_type="ACTION_OUTCOME_UNKNOWN",
            action_id="action-1",
            outcome=ActionUnknownOutcome(
                status=ActionStatus.OUTCOME_UNKNOWN,
                action_id="action-1",
                error=_error("OBSERVATION_LOST"),
            ),
        ),
    )
    reconciled = apply_event(
        unknown,
        ActionOutcomeReconciled(
            **_envelope(8),
            event_type="ACTION_OUTCOME_RECONCILED",
            action_id="action-1",
            outcome=ActionSucceededOutcome(
                status=ActionStatus.SUCCEEDED,
                action_id="action-1",
                result_ref=_artifact("attempts/attempt-1/reconciled-result.json"),
            ),
        ),
    )
    action = reconciled.actions[0]
    assert action.status is ActionStatus.OUTCOME_UNKNOWN
    assert action.error_code == "OBSERVATION_LOST"
    assert action.reconciled_status is ActionStatus.SUCCEEDED
    assert action.reconciled_result_ref is not None
    assert action.reconciled_error is None

    with pytest.raises(ValidationError, match="reconciled status"):
        type(action).model_validate(
            {
                **action.model_dump(mode="python"),
                "reconciled_status": ActionStatus.ACCEPTED,
                "reconciled_result_ref": None,
                "reconciled_error": _error(),
            }
        )

    second = ActionOutcomeReconciled(
        **_envelope(9),
        event_type="ACTION_OUTCOME_RECONCILED",
        action_id="action-1",
        outcome=ActionFailedOutcome(
            status=ActionStatus.FAILED, action_id="action-1", error=_error()
        ),
    )
    _assert_illegal(reconciled, second)
    _assert_illegal(_started_action_state(), second.model_copy(update={"sequence_no": 7}))

    with pytest.raises(ValidationError):
        ActionOutcomeReconciled(
            **_envelope(8),
            event_type="ACTION_OUTCOME_RECONCILED",
            action_id="action-1",
            outcome=ActionUnknownOutcome(
                status=ActionStatus.OUTCOME_UNKNOWN,
                action_id="action-1",
                error=_error(),
            ),
        )


def _invocation_requested(sequence_no: int = 4, **changes: object) -> InvocationRequested:
    values: dict[str, object] = {
        **_envelope(sequence_no),
        "event_type": "INVOCATION_REQUESTED",
        "action_id": "action-1",
        "invocation_id": "invocation-1",
        "agent_id": "worker-a",
        "conversation_id": "conversation-1",
        "parent_invocation_id": None,
        "latest_context_ref": _artifact("attempts/attempt-1/context/request.json"),
    }
    values.update(changes)
    return InvocationRequested(**values)


def test_invocation_lifecycle_is_explicit_and_does_not_advance_action() -> None:
    state = apply_event(
        apply_event(_running_state(), _action_proposed()),
        _invocation_requested(),
    )
    assert state.actions[0].status is ActionStatus.PROPOSED
    assert state.invocations[0].status is InvocationStatus.REQUESTED

    state = apply_event(state, _action_accepted(sequence_no=5))
    assert state.actions[0].status is ActionStatus.ACCEPTED
    assert state.invocations[0].status is InvocationStatus.REQUESTED

    state = apply_event(state, _action_started(sequence_no=6))
    assert state.actions[0].status is ActionStatus.STARTED
    assert state.invocations[0].status is InvocationStatus.REQUESTED

    state = apply_event(
        state,
        InvocationStarted(
            **_envelope(7),
            event_type="INVOCATION_STARTED",
            action_id="action-1",
            invocation_id="invocation-1",
        ),
    )
    assert state.actions[0].status is ActionStatus.STARTED
    assert state.invocations[0].status is InvocationStatus.RUNNING

    state = apply_event(
        state,
        InvocationCompleted(
            **_envelope(8),
            event_type="INVOCATION_COMPLETED",
            action_id="action-1",
            invocation_id="invocation-1",
            latest_context_ref=_artifact("attempts/attempt-1/context/completed.json"),
        ),
    )
    assert state.actions[0].status is ActionStatus.STARTED
    assert state.invocations[0].status is InvocationStatus.COMPLETED


def test_invocation_completion_before_start_and_owner_mismatch_are_illegal() -> None:
    requested = _started_action_state()
    completed = InvocationCompleted(
        **_envelope(7),
        event_type="INVOCATION_COMPLETED",
        action_id="action-1",
        invocation_id="invocation-1",
        latest_context_ref=None,
    )
    _assert_illegal(requested, completed)

    wrong_owner = InvocationStarted(
        **_envelope(7),
        event_type="INVOCATION_STARTED",
        action_id="action-other",
        invocation_id="invocation-1",
    )
    _assert_illegal(requested, wrong_owner)


def test_invocation_failed_requires_start_and_records_safe_error() -> None:
    requested = _started_action_state()
    started = apply_event(
        requested,
        InvocationStarted(
            **_envelope(7),
            event_type="INVOCATION_STARTED",
            action_id="action-1",
            invocation_id="invocation-1",
        ),
    )
    failed = apply_event(
        started,
        InvocationFailed(
            **_envelope(8),
            event_type="INVOCATION_FAILED",
            action_id="action-1",
            invocation_id="invocation-1",
            error=_error("INVOCATION_FAILED"),
        ),
    )
    assert failed.invocations[0].status is InvocationStatus.FAILED


@pytest.mark.parametrize("outcome", ["COMPLETED", "FAILED"])
def test_late_invocation_outcome_is_recorded_for_outcome_unknown_action(
    outcome: str,
) -> None:
    running = apply_event(
        _started_action_state(),
        InvocationStarted(
            **_envelope(7),
            event_type="INVOCATION_STARTED",
            action_id="action-1",
            invocation_id="invocation-1",
        ),
    )
    unknown = apply_event(
        running,
        ActionOutcomeUnknown(
            **_envelope(8),
            event_type="ACTION_OUTCOME_UNKNOWN",
            action_id="action-1",
            outcome=ActionUnknownOutcome(
                status=ActionStatus.OUTCOME_UNKNOWN,
                action_id="action-1",
                error=_error("OBSERVATION_LOST"),
            ),
        ),
    )
    if outcome == "COMPLETED":
        event: object = InvocationCompleted(
            **_envelope(9),
            event_type="INVOCATION_COMPLETED",
            action_id="action-1",
            invocation_id="invocation-1",
            latest_context_ref=_artifact("attempts/attempt-1/context/late.json"),
        )
    else:
        event = InvocationFailed(
            **_envelope(9),
            event_type="INVOCATION_FAILED",
            action_id="action-1",
            invocation_id="invocation-1",
            error=_error("INVOCATION_FAILED"),
        )

    updated = apply_event(unknown, event)  # type: ignore[arg-type]
    assert updated.actions[0].status is ActionStatus.OUTCOME_UNKNOWN
    assert updated.invocations[0].status.value == outcome


def test_invocation_outcome_requires_started_or_outcome_unknown_owner() -> None:
    accepted = _accepted_state()
    invocation = accepted.invocations[0].model_copy(
        update={"status": InvocationStatus.RUNNING}
    )
    invalid_owner = AttemptState.model_construct(
        **{
            **accepted.__dict__,
            "invocations": (invocation,),
        }
    )

    with pytest.raises((ValidationError, StateTransitionError)):
        apply_event(
            invalid_owner,
            InvocationCompleted(
                **_envelope(6),
                event_type="INVOCATION_COMPLETED",
                action_id="action-1",
                invocation_id="invocation-1",
                latest_context_ref=None,
            ),
        )


def test_invocation_request_requires_proposed_invoke_action_and_exact_pairing() -> None:
    proposed = apply_event(_running_state(), _action_proposed())
    requested = apply_event(proposed, _invocation_requested(sequence_no=4))
    assert requested.actions[0].status is ActionStatus.PROPOSED

    for changes in (
        {"action_id": "action-other"},
        {"invocation_id": "invocation-other"},
        {"agent_id": "worker-b"},
    ):
        _assert_illegal(proposed, _invocation_requested(**changes))

    _assert_illegal(proposed, _action_accepted(sequence_no=4))

    accepted = apply_event(requested, _action_accepted(sequence_no=5))
    _assert_illegal(
        accepted,
        InvocationStarted(
            **_envelope(6),
            event_type="INVOCATION_STARTED",
            action_id="action-1",
            invocation_id="invocation-1",
        ),
    )
    _assert_illegal(accepted, _invocation_requested(sequence_no=6))

    send_proposal = _proposal(
        action_id="message-1",
        action_type=ActionType.SEND_MESSAGE,
        invocation_id=None,
        causal_parent_id=str(EVENT_IDS[1]),
    )
    send_state = apply_event(
        _running_state(), _action_proposed(proposal=send_proposal)
    )
    _assert_illegal(
        send_state,
        _invocation_requested(
            action_id="message-1",
            invocation_id="invocation-1",
        ),
    )


def test_invocation_id_cannot_be_reused_by_another_action() -> None:
    first = apply_event(
        apply_event(_running_state(), _action_proposed()),
        _invocation_requested(),
    )
    second_proposal = _proposal(
        action_id="action-2",
        invocation_id="invocation-1",
        causal_parent_id=str(EVENT_IDS[3]),
    )
    _assert_illegal(
        first,
        _action_proposed(sequence_no=5, proposal=second_proposal),
    )


def test_invocation_id_is_reserved_uniquely_when_action_is_proposed() -> None:
    first = apply_event(_running_state(), _action_proposed())
    duplicate = _proposal(
        action_id="action-2",
        invocation_id="invocation-1",
        causal_parent_id=str(EVENT_IDS[2]),
    )
    _assert_illegal(first, _action_proposed(sequence_no=4, proposal=duplicate))


def _invocation_state(**changes: object) -> InvocationState:
    values: dict[str, object] = {
        "invocation_id": "invocation-1",
        "agent_id": "worker-a",
        "conversation_id": "conversation-1",
        "parent_invocation_id": None,
        "latest_context_ref": None,
        "last_action_id": "action-1",
        "status": InvocationStatus.REQUESTED,
    }
    values.update(changes)
    return InvocationState(**values)


@pytest.mark.parametrize(
    ("actions", "invocation"),
    [
        ((), _invocation_state()),
        (None, _invocation_state(last_action_id="action-other")),
        (None, _invocation_state(agent_id="worker-b")),
        (None, _invocation_state(invocation_id="invocation-other")),
    ],
    ids=(
        "missing-owner-action",
        "last-action-mismatch",
        "agent-target-mismatch",
        "invocation-id-ownership-mismatch",
    ),
)
def test_attempt_state_rejects_inconsistent_invocation_ownership(
    actions: tuple[object, ...] | None,
    invocation: InvocationState,
) -> None:
    state = _accepted_state()
    values = state.model_dump(mode="python")
    values["actions"] = state.actions if actions is None else actions
    values["invocations"] = (invocation,)

    with pytest.raises(ValidationError, match="Invocation owner"):
        AttemptState.model_validate(values)


@pytest.mark.parametrize(
    ("action_status", "invocation_status"),
    [
        (ActionStatus.PROPOSED, InvocationStatus.RUNNING),
        (ActionStatus.REJECTED, InvocationStatus.REQUESTED),
        (ActionStatus.ACCEPTED, InvocationStatus.COMPLETED),
        (ActionStatus.ACCEPTED, None),
        (ActionStatus.STARTED, None),
        (ActionStatus.SUCCEEDED, InvocationStatus.REQUESTED),
        (ActionStatus.SUCCEEDED, None),
        (ActionStatus.FAILED, InvocationStatus.RUNNING),
        (ActionStatus.FAILED, None),
        (ActionStatus.TIMED_OUT, InvocationStatus.COMPLETED),
        (ActionStatus.TIMED_OUT, None),
        (ActionStatus.CANCELLED, InvocationStatus.RUNNING),
        (ActionStatus.CANCELLED, None),
        (ActionStatus.OUTCOME_UNKNOWN, None),
    ],
)
def test_attempt_state_rejects_persisted_invoke_action_invocation_mismatch(
    action_status: ActionStatus,
    invocation_status: InvocationStatus | None,
) -> None:
    source = _accepted_state()
    action = source.actions[0]
    action_values = action.model_dump(mode="python")
    action_values["status"] = action_status
    if action_status is ActionStatus.PROPOSED:
        action_values["reservation_id"] = None
        action_values["idempotency_key"] = None
    elif action_status is ActionStatus.REJECTED:
        action_values["reservation_id"] = None
        action_values["idempotency_key"] = None
        action_values["error_code"] = "ACTION_REJECTED"
        action_values["error"] = _error("ACTION_REJECTED")
    elif action_status is ActionStatus.SUCCEEDED:
        action_values["result_ref"] = _artifact("attempts/attempt-1/result.json")
    elif action_status in {
        ActionStatus.FAILED,
        ActionStatus.TIMED_OUT,
        ActionStatus.CANCELLED,
        ActionStatus.OUTCOME_UNKNOWN,
    }:
        action_values["error_code"] = "BACKEND_FAILURE"
        action_values["error"] = _error()

    state_values = source.model_dump(mode="python")
    state_values["actions"] = (action_values,)
    if invocation_status is None:
        state_values["invocations"] = ()
    else:
        invocation_values = source.invocations[0].model_dump(mode="python")
        invocation_values["status"] = invocation_status
        state_values["invocations"] = (invocation_values,)

    with pytest.raises(ValidationError, match="lifecycle"):
        AttemptState.model_validate(state_values)


@pytest.mark.parametrize(
    "invocation_status",
    [
        InvocationStatus.REQUESTED,
        InvocationStatus.RUNNING,
        InvocationStatus.COMPLETED,
        InvocationStatus.FAILED,
    ],
)
def test_attempt_state_allows_late_invocation_status_for_persisted_unknown_action(
    invocation_status: InvocationStatus,
) -> None:
    source = _accepted_state()
    action_values = source.actions[0].model_dump(mode="python")
    action_values.update(
        status=ActionStatus.OUTCOME_UNKNOWN,
        error_code="OBSERVATION_LOST",
        error=_error("OBSERVATION_LOST"),
    )
    invocation_values = source.invocations[0].model_dump(mode="python")
    invocation_values["status"] = invocation_status
    state_values = source.model_dump(mode="python")
    state_values["actions"] = (action_values,)
    state_values["invocations"] = (invocation_values,)

    validated = AttemptState.model_validate(state_values)
    assert validated.actions[0].status is ActionStatus.OUTCOME_UNKNOWN
    assert validated.invocations[0].status is invocation_status


def test_legacy_invoke_action_spelling_cannot_bypass_joint_lifecycle() -> None:
    source = _accepted_state()
    action_values = source.actions[0].model_dump(mode="python")
    action_values["action_type"] = "invoke_agent"
    invocation_values = source.invocations[0].model_dump(mode="python")
    invocation_values["status"] = InvocationStatus.RUNNING
    state_values = source.model_dump(mode="python")
    state_values["actions"] = (action_values,)
    state_values["invocations"] = (invocation_values,)

    with pytest.raises(ValidationError, match="lifecycle"):
        AttemptState.model_validate(state_values)

    started = apply_event(
        _started_action_state(),
        InvocationStarted(
            **_envelope(7),
            event_type="INVOCATION_STARTED",
            action_id="action-1",
            invocation_id="invocation-1",
        ),
    )
    started_values = started.model_dump(mode="python")
    started_values["actions"][0]["action_type"] = "invoke_agent"
    legacy_started = AttemptState.model_validate(started_values)
    _assert_illegal(
        legacy_started,
        ActionFailed(
            **_envelope(8),
            event_type="ACTION_FAILED",
            action_id="action-1",
            outcome=ActionFailedOutcome(
                status=ActionStatus.FAILED,
                action_id="action-1",
                error=_error(),
            ),
        ),
    )


def test_action_events_require_running_attempt() -> None:
    planned = apply_event(None, _planned())
    proposal = _proposal(causal_parent_id=str(EVENT_IDS[0]))
    _assert_illegal(planned, _action_proposed(sequence_no=2, proposal=proposal))


def test_parser_revalidates_copied_action_event_and_nested_models() -> None:
    parsed = parse_domain_event(_action_proposed().model_dump(mode="json"))
    assert isinstance(parsed, ActionProposed)

    copied = _action_proposed().model_copy(update={"sequence_no": 0})
    with pytest.raises(ValidationError):
        parse_domain_event(copied)

    copied_extra = _action_proposed().model_copy(update={"secret": "leak"})
    with pytest.raises(ValidationError, match="extra_forbidden"):
        parse_domain_event(copied_extra)

    nested = _proposal().model_copy(update={"action_id": "bad/action"})
    copied_nested = _action_proposed().model_copy(update={"proposal": nested})
    with pytest.raises(ValidationError, match="action_id"):
        parse_domain_event(copied_nested)
