from __future__ import annotations

from collections.abc import Callable, Iterable
from datetime import datetime
from typing import cast
from uuid import UUID

from .events import (
    ActionAccepted,
    ActionCancellationRequested,
    ActionCancelled,
    ActionFailed,
    ActionOutcomeReconciled,
    ActionOutcomeUnknown,
    ActionProposed,
    ActionRejected,
    ActionStarted,
    ActionSucceeded,
    ActionTimedOut,
    AttemptCancelled,
    AttemptFailed,
    AttemptInterrupted,
    AttemptPaused,
    AttemptPlanned,
    AttemptRecoveryRequested,
    AttemptResumed,
    AttemptStarted,
    AttemptSucceeded,
    AttemptTimedOut,
    BudgetReservationEntry,
    BudgetReleased,
    BudgetReserved,
    BudgetSettled,
    BudgetUncertainSettled,
    CancelRequested,
    DomainEvent,
    ExternalInputApproved,
    ExternalInputExpired,
    ExternalInputReceived,
    ExternalInputRejected,
    ExternalInputRequested,
    InvocationCompleted,
    InvocationFailed,
    InvocationRequested,
    InvocationStarted,
    PauseRequested,
    StrategyDecisionRecorded,
    _has_legacy_cursor_shape,
    _to_validation_payload,
    effective_strategy_triggers,
    is_strategy_trigger,
    parse_domain_event,
)
from .actions import ActionFailedOutcome, ActionSucceededOutcome, ActionType
from .commands import outcome_reconciliation_request_id
from .contract import StrategyDirective
from .state import (
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
    InvocationState,
    InvocationStatus,
    ResourceBudget,
    StrategyStateEnvelope,
    TERMINAL_ACTION_STATUSES,
)


_ERROR_MESSAGES = {
    "state_required": "The event requires an existing Attempt state.",
    "state_already_exists": "The planning event requires a missing Attempt state.",
    "invalid_initial_sequence": "The planning event has an invalid initial sequence.",
    "causal_parent_required": "The event requires a causal parent.",
    "causal_parent_forbidden": "The root event cannot have a causal parent.",
    "identity_mismatch": "The event identity does not match the Attempt.",
    "stale_sequence": "The event sequence is stale.",
    "sequence_gap": "The event sequence contains a gap.",
    "terminal_state": "The Attempt is already terminal.",
    "illegal_transition": "The event is not legal in the current Attempt state.",
    "empty_replay": "An event stream is required for replay.",
    "duplicate_event_id": "The event stream contains a duplicate event identifier.",
    "duplicate_command_id": (
        "The event stream reuses a closed command identifier."
    ),
    "duplicate_external_request_id": (
        "The event stream contains a duplicate external request identifier."
    ),
    "causal_parent_not_seen": "The causal parent has not appeared earlier in the stream.",
    "causal_parent_wrong_attempt": "The causal parent belongs to a different Attempt.",
    "causal_parent_not_earlier": "The causal parent sequence is not earlier than the child.",
    "invalid_budget_transition": "The budget Event does not preserve the ledger transition.",
    "invalid_external_transition": (
        "The external-input Event lifecycle is invalid."
    ),
    "invalid_strategy_cursor": "The Strategy decision cursor is invalid.",
    "terminal_strategy_decision": "The terminal Strategy decision is final.",
}


class StateTransitionError(ValueError):
    def __init__(self, code: str) -> None:
        self.code = code
        self.safe_message = _ERROR_MESSAGES[code]
        super().__init__(self.safe_message)


_TERMINAL_PHASES = frozenset(
    {
        AttemptPhase.SUCCEEDED,
        AttemptPhase.FAILED,
        AttemptPhase.CANCELLED,
        AttemptPhase.TIMED_OUT,
        AttemptPhase.INTERRUPTED,
    }
)


def _create_attempt(event: AttemptPlanned) -> AttemptState:
    if event.sequence_no != 1:
        raise StateTransitionError("invalid_initial_sequence")
    if any(resource.reserved != 0 for resource in event.budget.resources):
        raise StateTransitionError("invalid_budget_transition")
    return AttemptState.model_validate(
        {
            "schema_version": event.state_schema_version,
            "experiment_id": event.experiment_id,
            "trial_id": event.trial_id,
            "attempt_id": event.attempt_id,
            "strategy_id": event.strategy_id,
            "revision": 1,
            "phase": AttemptPhase.PLANNED,
            "manifest_ref": event.manifest_ref,
            "strategy": event.strategy,
            "budget": event.budget,
            "actions": (),
            "invocations": (),
            "pending_external": (),
            "result_ref": None,
            "terminal_error": None,
            "started_at": None,
            "finished_at": None,
        }
    )


def _advance_attempt(
    state: AttemptState,
    event: DomainEvent,
    *,
    phase: AttemptPhase,
    result_ref: ArtifactRef | None,
    terminal_error: ErrorSummary | None,
    started_at: datetime | None,
    finished_at: datetime | None,
) -> AttemptState:
    return AttemptState.model_validate(
        {
            "schema_version": state.schema_version,
            "experiment_id": state.experiment_id,
            "trial_id": state.trial_id,
            "attempt_id": state.attempt_id,
            "strategy_id": state.strategy_id,
            "revision": event.sequence_no,
            "phase": phase,
            "manifest_ref": state.manifest_ref,
            "strategy": state.strategy,
            "budget": state.budget,
            "actions": state.actions,
            "invocations": state.invocations,
            "pending_external": state.pending_external,
            "result_ref": result_ref,
            "terminal_error": terminal_error,
            "started_at": started_at,
            "finished_at": finished_at,
        }
    )


def _require_phase(state: AttemptState, expected: AttemptPhase) -> None:
    if state.phase is not expected:
        raise StateTransitionError("illegal_transition")


def _require_phase_in(
    state: AttemptState,
    allowed: frozenset[AttemptPhase],
) -> None:
    if state.phase not in allowed:
        raise StateTransitionError("illegal_transition")


def _require_terminal_actions(state: AttemptState) -> None:
    if any(
        action.status not in TERMINAL_ACTION_STATUSES for action in state.actions
    ):
        raise StateTransitionError("illegal_transition")


def _replace_summaries(
    state: AttemptState,
    event: DomainEvent,
    *,
    actions: tuple[ActionState, ...],
    invocations: tuple[InvocationState, ...],
) -> AttemptState:
    return AttemptState.model_validate(
        {
            "schema_version": state.schema_version,
            "experiment_id": state.experiment_id,
            "trial_id": state.trial_id,
            "attempt_id": state.attempt_id,
            "strategy_id": state.strategy_id,
            "revision": event.sequence_no,
            "phase": state.phase,
            "manifest_ref": state.manifest_ref,
            "strategy": state.strategy,
            "budget": state.budget,
            "actions": actions,
            "invocations": invocations,
            "pending_external": state.pending_external,
            "result_ref": state.result_ref,
            "terminal_error": state.terminal_error,
            "started_at": state.started_at,
            "finished_at": state.finished_at,
        }
    )


def _replace_strategy(
    state: AttemptState,
    event: DomainEvent,
    strategy: StrategyStateEnvelope,
) -> AttemptState:
    return AttemptState.model_validate(
        {
            **state.model_dump(mode="python"),
            "revision": event.sequence_no,
            "strategy": strategy,
        }
    )


def _replace_budget(
    state: AttemptState,
    event: DomainEvent,
    budget: BudgetState,
) -> AttemptState:
    return AttemptState.model_validate(
        {
            **state.model_dump(mode="python"),
            "revision": event.sequence_no,
            "budget": budget,
        }
    )


def _replace_external(
    state: AttemptState,
    event: DomainEvent,
    *,
    phase: AttemptPhase,
    pending_external: tuple[ExternalRequest, ...],
) -> AttemptState:
    return AttemptState.model_validate(
        {
            **state.model_dump(mode="python"),
            "revision": event.sequence_no,
            "phase": phase,
            "pending_external": pending_external,
        }
    )


def _budget_pairs(
    state: AttemptState,
    event_budget: BudgetState,
) -> tuple[tuple[ResourceBudget, ResourceBudget], ...]:
    current = state.budget
    if (
        current.deadline_at != event_budget.deadline_at
        or current.max_call_depth != event_budget.max_call_depth
        or current.max_concurrent_actions != event_budget.max_concurrent_actions
        or len(current.resources) != len(event_budget.resources)
    ):
        raise StateTransitionError("invalid_budget_transition")
    pairs = tuple(zip(current.resources, event_budget.resources, strict=True))
    if any(
        before.resource != after.resource or before.limit != after.limit
        for before, after in pairs
    ):
        raise StateTransitionError("invalid_budget_transition")
    return pairs


def _reservation_amounts(
    reservations: tuple[BudgetReservationEntry, ...],
) -> dict[str, int]:
    amounts: dict[str, int] = {}
    for reservation in reservations:
        for request in reservation.resource_requests:
            resource = request.resource.value
            amounts[resource] = amounts.get(resource, 0) + request.amount
    return amounts


def _validate_budget_delta(
    state: AttemptState,
    event_budget: BudgetState,
    reservations: tuple[BudgetReservationEntry, ...],
    *,
    reserved_multiplier: int,
    consumed_multiplier: int,
) -> None:
    pairs = _budget_pairs(state, event_budget)
    amounts = _reservation_amounts(reservations)
    if not set(amounts).issubset({before.resource for before, _ in pairs}):
        raise StateTransitionError("invalid_budget_transition")
    if any(
        after.reserved
        != before.reserved + reserved_multiplier * amounts.get(before.resource, 0)
        or after.consumed
        != before.consumed + consumed_multiplier * amounts.get(before.resource, 0)
        for before, after in pairs
    ):
        raise StateTransitionError("invalid_budget_transition")


def _reservation_action(
    state: AttemptState,
    reservation: BudgetReservationEntry,
) -> ActionState:
    action = next(
        (
            candidate
            for candidate in state.actions
            if candidate.action_id == reservation.action_id
        ),
        None,
    )
    if (
        action is None
        or action.reservation_id != reservation.reservation_id
        or action.resource_requests != reservation.resource_requests
    ):
        raise StateTransitionError("invalid_budget_transition")
    return action


def _handle_strategy_decision(
    state: AttemptState,
    event: StrategyDecisionRecorded,
) -> AttemptState:
    _require_running(state)
    if event.strategy.strategy_id != state.strategy_id:
        raise StateTransitionError("illegal_transition")
    return _replace_strategy(state, event, event.strategy)


def _handle_external_requested(
    state: AttemptState,
    event: ExternalInputRequested,
) -> AttemptState:
    if event.request.request_kind is ExternalRequestKind.OUTCOME_RECONCILIATION:
        _require_phase_in(
            state,
            frozenset(
                {
                    AttemptPhase.RUNNING,
                    AttemptPhase.PAUSE_REQUESTED,
                    AttemptPhase.CANCEL_REQUESTED,
                }
            ),
        )
    else:
        _require_running(state)
    if state.pending_external:
        raise StateTransitionError("illegal_transition")
    return _replace_external(
        state,
        event,
        phase=AttemptPhase.WAITING_EXTERNAL,
        pending_external=(event.request,),
    )


def _pending_request(
    state: AttemptState,
    *,
    request_id: str,
    request_kind: ExternalRequestKind,
    action_id: str | None,
) -> ExternalRequest:
    _require_phase(state, AttemptPhase.WAITING_EXTERNAL)
    if len(state.pending_external) != 1:
        raise StateTransitionError("illegal_transition")
    request = state.pending_external[0]
    if (
        request.request_id != request_id
        or request.request_kind is not request_kind
        or request.action_id != action_id
    ):
        raise StateTransitionError("illegal_transition")
    return request


def _is_waiting_external_sibling(
    state: AttemptState,
    action_id: str,
) -> bool:
    return (
        state.phase is AttemptPhase.WAITING_EXTERNAL
        and len(state.pending_external) == 1
        and state.pending_external[0].action_id != action_id
    )


def _handle_external_received(
    state: AttemptState,
    event: ExternalInputReceived,
) -> AttemptState:
    _pending_request(
        state,
        request_id=event.request_id,
        request_kind=event.request_kind,
        action_id=event.action_id,
    )
    return _replace_external(
        state,
        event,
        phase=AttemptPhase.WAITING_EXTERNAL,
        pending_external=state.pending_external,
    )


def _handle_external_resolution(
    state: AttemptState,
    event: ExternalInputApproved | ExternalInputRejected | ExternalInputExpired,
) -> AttemptState:
    _pending_request(
        state,
        request_id=event.request_id,
        request_kind=event.request_kind,
        action_id=event.action_id,
    )
    return _replace_external(
        state,
        event,
        phase=AttemptPhase.RUNNING,
        pending_external=(),
    )


def _handle_budget_reserved(
    state: AttemptState,
    event: BudgetReserved,
) -> AttemptState:
    _require_running(state)
    for reservation in event.reservations:
        action = next(
            (
                candidate
                for candidate in state.actions
                if candidate.action_id == reservation.action_id
            ),
            None,
        )
        if (
            action is None
            or action.status is not ActionStatus.PROPOSED
            or action.resource_requests != reservation.resource_requests
            or any(
                candidate.reservation_id == reservation.reservation_id
                for candidate in state.actions
                if candidate.reservation_id is not None
            )
        ):
            raise StateTransitionError("invalid_budget_transition")
    _validate_budget_delta(
        state,
        event.budget,
        event.reservations,
        reserved_multiplier=1,
        consumed_multiplier=0,
    )
    return _replace_budget(state, event, event.budget)


def _handle_budget_settled(
    state: AttemptState,
    event: BudgetSettled | BudgetUncertainSettled,
) -> AttemptState:
    waiting_sibling = (
        isinstance(event, BudgetSettled)
        and _is_waiting_external_sibling(
            state,
            event.reservation.action_id,
        )
    )
    if not waiting_sibling:
        _require_phase_in(
            state,
            frozenset(
                {
                    AttemptPhase.RUNNING,
                    AttemptPhase.PAUSE_REQUESTED,
                    AttemptPhase.CANCEL_REQUESTED,
                }
            ),
        )
    action = _reservation_action(state, event.reservation)
    known_terminal = action.status in {
        ActionStatus.SUCCEEDED,
        ActionStatus.FAILED,
        ActionStatus.TIMED_OUT,
        ActionStatus.CANCELLED,
    }
    reconciled_unknown = (
        action.status is ActionStatus.OUTCOME_UNKNOWN
        and action.reconciled_status
        in {ActionStatus.SUCCEEDED, ActionStatus.FAILED}
    )
    unresolved_unknown = (
        action.status is ActionStatus.OUTCOME_UNKNOWN
        and action.reconciled_status is None
    )
    if isinstance(event, BudgetUncertainSettled):
        valid_status = unresolved_unknown
    else:
        valid_status = known_terminal or reconciled_unknown
    if not valid_status:
        raise StateTransitionError("invalid_budget_transition")
    _validate_budget_delta(
        state,
        event.budget,
        (event.reservation,),
        reserved_multiplier=-1,
        consumed_multiplier=1,
    )
    return _replace_budget(state, event, event.budget)


def _handle_budget_released(
    state: AttemptState,
    event: BudgetReleased,
) -> AttemptState:
    _require_phase_in(
        state,
        frozenset({AttemptPhase.RUNNING, AttemptPhase.CANCEL_REQUESTED}),
    )
    action = _reservation_action(state, event.reservation)
    if action.status is not ActionStatus.CANCELLED:
        raise StateTransitionError("invalid_budget_transition")
    _validate_budget_delta(
        state,
        event.budget,
        (event.reservation,),
        reserved_multiplier=-1,
        consumed_multiplier=0,
    )
    return _replace_budget(state, event, event.budget)


def _require_running(state: AttemptState) -> None:
    _require_phase(state, AttemptPhase.RUNNING)


def _action_index(state: AttemptState, action_id: str) -> int:
    for index, action in enumerate(state.actions):
        if action.action_id == action_id:
            return index
    raise StateTransitionError("illegal_transition")


def _invocation_index(state: AttemptState, invocation_id: str) -> int:
    for index, invocation in enumerate(state.invocations):
        if invocation.invocation_id == invocation_id:
            return index
    raise StateTransitionError("illegal_transition")


def _replace_action(
    state: AttemptState, event: DomainEvent, index: int, action: ActionState
) -> AttemptState:
    actions = state.actions[:index] + (action,) + state.actions[index + 1 :]
    return _replace_summaries(
        state, event, actions=actions, invocations=state.invocations
    )


def _replace_invocation(
    state: AttemptState,
    event: DomainEvent,
    index: int,
    invocation: InvocationState,
) -> AttemptState:
    invocations = (
        state.invocations[:index] + (invocation,) + state.invocations[index + 1 :]
    )
    return _replace_summaries(
        state, event, actions=state.actions, invocations=invocations
    )


def _action_values(action: ActionState) -> dict[str, object]:
    return {
        "action_id": action.action_id,
        "action_type": action.action_type,
        "actor_id": action.actor_id,
        "target_ids": action.target_ids,
        "status": action.status,
        "causal_parent_id": action.causal_parent_id,
        "invocation_id": action.invocation_id,
        "payload_ref": action.payload_ref,
        "result_ref": action.result_ref,
        "idempotency_key": action.idempotency_key,
        "recovery_policy": action.recovery_policy,
        "reservation_id": action.reservation_id,
        "retry_of_action_id": action.retry_of_action_id,
        "error_code": action.error_code,
        "requested_timeout": action.requested_timeout,
        "batch_id": action.batch_id,
        "call_depth": action.call_depth,
        "resource_requests": action.resource_requests,
        "error": action.error,
        "reconciled_status": action.reconciled_status,
        "reconciled_result_ref": action.reconciled_result_ref,
        "reconciled_error": action.reconciled_error,
    }


def _invocation_values(invocation: InvocationState) -> dict[str, object]:
    return {
        "invocation_id": invocation.invocation_id,
        "agent_id": invocation.agent_id,
        "conversation_id": invocation.conversation_id,
        "parent_invocation_id": invocation.parent_invocation_id,
        "latest_context_ref": invocation.latest_context_ref,
        "last_action_id": invocation.last_action_id,
        "status": invocation.status,
    }


def _handle_action_proposed(
    state: AttemptState, event: ActionProposed
) -> AttemptState:
    _require_running(state)
    duplicate_action = any(
        action.action_id == event.proposal.action_id for action in state.actions
    )
    duplicate_invocation = event.proposal.invocation_id is not None and any(
        action.invocation_id == event.proposal.invocation_id for action in state.actions
    )
    if duplicate_action or duplicate_invocation:
        raise StateTransitionError("illegal_transition")
    proposal = event.proposal
    action = ActionState.model_validate(
        {
            "action_id": proposal.action_id,
            "action_type": proposal.action_type.value,
            "actor_id": proposal.actor,
            "target_ids": proposal.target_ids,
            "status": ActionStatus.PROPOSED,
            "causal_parent_id": proposal.causal_parent_id,
            "invocation_id": proposal.invocation_id,
            "payload_ref": proposal.payload_ref,
            "result_ref": None,
            "idempotency_key": None,
            "recovery_policy": proposal.recovery_policy,
            "reservation_id": None,
            "retry_of_action_id": proposal.retry_of_action_id,
            "error_code": None,
            "requested_timeout": proposal.requested_timeout,
            "batch_id": proposal.batch_id,
            "call_depth": proposal.call_depth,
            "resource_requests": proposal.resource_requests,
        }
    )
    return _replace_summaries(
        state,
        event,
        actions=state.actions + (action,),
        invocations=state.invocations,
    )


def _handle_action_rejected(
    state: AttemptState, event: ActionRejected
) -> AttemptState:
    _require_running(state)
    index = _action_index(state, event.action_id)
    current = state.actions[index]
    has_invocation = current.invocation_id is not None and any(
        invocation.invocation_id == current.invocation_id
        for invocation in state.invocations
    )
    if current.status is not ActionStatus.PROPOSED or has_invocation:
        raise StateTransitionError("illegal_transition")
    action = ActionState.model_validate(
        {
            **_action_values(current),
            "status": ActionStatus.REJECTED,
            "error_code": event.error.code,
            "error": event.error,
        }
    )
    return _replace_action(state, event, index, action)


def _handle_action_accepted(
    state: AttemptState, event: ActionAccepted
) -> AttemptState:
    _require_running(state)
    accepted = event.action
    index = _action_index(state, accepted.action_id)
    current = state.actions[index]
    if current.status is not ActionStatus.PROPOSED:
        raise StateTransitionError("illegal_transition")
    if (
        current.action_type in {ActionType.INVOKE_AGENT.value, "invoke_agent"}
        and not any(
            invocation.invocation_id == current.invocation_id
            and invocation.last_action_id == current.action_id
            for invocation in state.invocations
        )
    ):
        raise StateTransitionError("illegal_transition")
    unchanged = (
        current.action_type == accepted.action_type.value
        and current.actor_id == accepted.actor
        and current.target_ids == accepted.target_ids
        and current.invocation_id == accepted.invocation_id
        and current.payload_ref == accepted.payload_ref
        and current.recovery_policy is accepted.recovery_policy
        and current.retry_of_action_id == accepted.retry_of_action_id
        and current.causal_parent_id == accepted.causal_parent_id
        and current.requested_timeout == accepted.requested_timeout
        and current.batch_id == accepted.batch_id
        and current.call_depth == accepted.call_depth
        and current.resource_requests == accepted.resource_requests
    )
    if not unchanged:
        raise StateTransitionError("illegal_transition")
    action = ActionState.model_validate(
        {
            **_action_values(current),
            "status": ActionStatus.ACCEPTED,
            "idempotency_key": accepted.idempotency_key,
            "reservation_id": accepted.reservation_id,
        }
    )
    return _replace_action(state, event, index, action)


def _handle_action_started(state: AttemptState, event: ActionStarted) -> AttemptState:
    _require_running(state)
    index = _action_index(state, event.action_id)
    current = state.actions[index]
    if current.status is not ActionStatus.ACCEPTED:
        raise StateTransitionError("illegal_transition")
    action = ActionState.model_validate(
        {**_action_values(current), "status": ActionStatus.STARTED}
    )
    return _replace_action(state, event, index, action)


def _handle_action_terminal(
    state: AttemptState,
    event: ActionSucceeded
    | ActionFailed
    | ActionTimedOut
    | ActionCancelled
    | ActionOutcomeUnknown,
) -> AttemptState:
    waiting_sibling = (
        event.outcome.status is not ActionStatus.OUTCOME_UNKNOWN
        and _is_waiting_external_sibling(state, event.action_id)
    )
    if not waiting_sibling:
        _require_phase_in(
            state,
            frozenset(
                {
                    AttemptPhase.RUNNING,
                    AttemptPhase.PAUSE_REQUESTED,
                    AttemptPhase.CANCEL_REQUESTED,
                }
            ),
        )
    index = _action_index(state, event.action_id)
    current = state.actions[index]
    predispatch_cancellation = (
        isinstance(event, ActionCancelled)
        and current.status is ActionStatus.ACCEPTED
        and state.phase
        in {AttemptPhase.RUNNING, AttemptPhase.CANCEL_REQUESTED}
    )
    if current.status is not ActionStatus.STARTED and not predispatch_cancellation:
        raise StateTransitionError("illegal_transition")
    outcome = event.outcome
    if current.action_type in {ActionType.INVOKE_AGENT.value, "invoke_agent"}:
        matching_invocations = [
            invocation
            for invocation in state.invocations
            if invocation.invocation_id == current.invocation_id
            and invocation.last_action_id == current.action_id
        ]
        allowed_invocation_statuses = {
            ActionStatus.SUCCEEDED: {InvocationStatus.COMPLETED},
            ActionStatus.FAILED: {
                InvocationStatus.REQUESTED,
                InvocationStatus.FAILED,
            },
            ActionStatus.TIMED_OUT: {
                InvocationStatus.REQUESTED,
                InvocationStatus.FAILED,
            },
            ActionStatus.CANCELLED: {
                InvocationStatus.REQUESTED,
                InvocationStatus.FAILED,
            },
            ActionStatus.OUTCOME_UNKNOWN: {
                InvocationStatus.REQUESTED,
                InvocationStatus.RUNNING,
            },
        }
        if (
            len(matching_invocations) != 1
            or matching_invocations[0].status
            not in allowed_invocation_statuses[outcome.status]
        ):
            raise StateTransitionError("illegal_transition")
    result_ref = outcome.result_ref if isinstance(outcome, ActionSucceededOutcome) else None
    error = None if isinstance(outcome, ActionSucceededOutcome) else outcome.error
    action = ActionState.model_validate(
        {
            **_action_values(current),
            "status": outcome.status,
            "result_ref": result_ref,
            "error_code": None if error is None else error.code,
            "error": error,
        }
    )
    return _replace_action(state, event, index, action)


def _handle_action_reconciled(
    state: AttemptState, event: ActionOutcomeReconciled
) -> AttemptState:
    _require_running(state)
    index = _action_index(state, event.action_id)
    current = state.actions[index]
    if (
        current.status is not ActionStatus.OUTCOME_UNKNOWN
        or current.reconciled_status is not None
    ):
        raise StateTransitionError("illegal_transition")
    outcome = event.outcome
    result_ref = outcome.result_ref if isinstance(outcome, ActionSucceededOutcome) else None
    error = None if isinstance(outcome, ActionSucceededOutcome) else outcome.error
    action = ActionState.model_validate(
        {
            **_action_values(current),
            "reconciled_status": outcome.status,
            "reconciled_result_ref": result_ref,
            "reconciled_error": error,
        }
    )
    return _replace_action(state, event, index, action)


def _handle_invocation_requested(
    state: AttemptState, event: InvocationRequested
) -> AttemptState:
    _require_running(state)
    action_index = _action_index(state, event.action_id)
    action = state.actions[action_index]
    if (
        action.status is not ActionStatus.PROPOSED
        or action.action_type
        not in {ActionType.INVOKE_AGENT.value, "invoke_agent"}
        or action.invocation_id != event.invocation_id
        or action.target_ids != (event.agent_id,)
        or any(
            invocation.invocation_id == event.invocation_id
            for invocation in state.invocations
        )
    ):
        raise StateTransitionError("illegal_transition")
    invocation = InvocationState.model_validate(
        {
            "invocation_id": event.invocation_id,
            "agent_id": event.agent_id,
            "conversation_id": event.conversation_id,
            "parent_invocation_id": event.parent_invocation_id,
            "latest_context_ref": event.latest_context_ref,
            "last_action_id": event.action_id,
            "status": InvocationStatus.REQUESTED,
        }
    )
    return _replace_summaries(
        state,
        event,
        actions=state.actions,
        invocations=state.invocations + (invocation,),
    )


def _handle_invocation_started(
    state: AttemptState, event: InvocationStarted
) -> AttemptState:
    _require_running(state)
    index = _invocation_index(state, event.invocation_id)
    current = state.invocations[index]
    action = state.actions[_action_index(state, event.action_id)]
    if (
        current.last_action_id != event.action_id
        or current.status is not InvocationStatus.REQUESTED
        or action.status is not ActionStatus.STARTED
        or action.invocation_id != event.invocation_id
    ):
        raise StateTransitionError("illegal_transition")
    invocation = InvocationState.model_validate(
        {**_invocation_values(current), "status": InvocationStatus.RUNNING}
    )
    return _replace_invocation(state, event, index, invocation)


def _handle_invocation_completed(
    state: AttemptState, event: InvocationCompleted
) -> AttemptState:
    if not _is_waiting_external_sibling(state, event.action_id):
        _require_phase_in(
            state,
            frozenset(
                {
                    AttemptPhase.RUNNING,
                    AttemptPhase.PAUSE_REQUESTED,
                    AttemptPhase.CANCEL_REQUESTED,
                }
            ),
        )
    index = _invocation_index(state, event.invocation_id)
    current = state.invocations[index]
    action = state.actions[_action_index(state, event.action_id)]
    if (
        current.last_action_id != event.action_id
        or current.status is not InvocationStatus.RUNNING
        or action.invocation_id != event.invocation_id
        or action.status
        not in {ActionStatus.STARTED, ActionStatus.OUTCOME_UNKNOWN}
    ):
        raise StateTransitionError("illegal_transition")
    invocation = InvocationState.model_validate(
        {
            **_invocation_values(current),
            "status": InvocationStatus.COMPLETED,
            "latest_context_ref": event.latest_context_ref,
        }
    )
    return _replace_invocation(state, event, index, invocation)


def _handle_invocation_failed(
    state: AttemptState, event: InvocationFailed
) -> AttemptState:
    if not _is_waiting_external_sibling(state, event.action_id):
        _require_phase_in(
            state,
            frozenset(
                {
                    AttemptPhase.RUNNING,
                    AttemptPhase.PAUSE_REQUESTED,
                    AttemptPhase.CANCEL_REQUESTED,
                }
            ),
        )
    index = _invocation_index(state, event.invocation_id)
    current = state.invocations[index]
    action = state.actions[_action_index(state, event.action_id)]
    if (
        current.last_action_id != event.action_id
        or current.status is not InvocationStatus.RUNNING
        or action.invocation_id != event.invocation_id
        or action.status
        not in {ActionStatus.STARTED, ActionStatus.OUTCOME_UNKNOWN}
    ):
        raise StateTransitionError("illegal_transition")
    invocation = InvocationState.model_validate(
        {**_invocation_values(current), "status": InvocationStatus.FAILED}
    )
    return _replace_invocation(state, event, index, invocation)


def _handle_started(state: AttemptState, event: AttemptStarted) -> AttemptState:
    _require_phase(state, AttemptPhase.PLANNED)
    return _advance_attempt(
        state,
        event,
        phase=AttemptPhase.RUNNING,
        result_ref=None,
        terminal_error=None,
        started_at=event.wall_time_utc,
        finished_at=None,
    )


def _handle_recovery_requested(
    state: AttemptState,
    event: AttemptRecoveryRequested,
) -> AttemptState:
    return _replace_summaries(
        state,
        event,
        actions=state.actions,
        invocations=state.invocations,
    )


def _handle_pause_requested(
    state: AttemptState,
    event: PauseRequested,
) -> AttemptState:
    _require_phase(state, AttemptPhase.RUNNING)
    return _advance_attempt(
        state,
        event,
        phase=AttemptPhase.PAUSE_REQUESTED,
        result_ref=None,
        terminal_error=None,
        started_at=state.started_at,
        finished_at=None,
    )


def _handle_paused(state: AttemptState, event: AttemptPaused) -> AttemptState:
    _require_phase(state, AttemptPhase.PAUSE_REQUESTED)
    if any(action.status is ActionStatus.STARTED for action in state.actions):
        raise StateTransitionError("illegal_transition")
    return _advance_attempt(
        state,
        event,
        phase=AttemptPhase.PAUSED,
        result_ref=None,
        terminal_error=None,
        started_at=state.started_at,
        finished_at=None,
    )


def _handle_resumed(state: AttemptState, event: AttemptResumed) -> AttemptState:
    _require_phase(state, AttemptPhase.PAUSED)
    return _advance_attempt(
        state,
        event,
        phase=AttemptPhase.RUNNING,
        result_ref=None,
        terminal_error=None,
        started_at=state.started_at,
        finished_at=None,
    )


def _handle_cancel_requested(
    state: AttemptState,
    event: CancelRequested,
) -> AttemptState:
    _require_phase_in(
        state,
        frozenset(
            {
                AttemptPhase.RUNNING,
                AttemptPhase.PAUSE_REQUESTED,
                AttemptPhase.PAUSED,
            }
        ),
    )
    return _advance_attempt(
        state,
        event,
        phase=AttemptPhase.CANCEL_REQUESTED,
        result_ref=None,
        terminal_error=None,
        started_at=state.started_at,
        finished_at=None,
    )


def _handle_action_cancellation_requested(
    state: AttemptState,
    event: ActionCancellationRequested,
) -> AttemptState:
    _require_phase(state, AttemptPhase.CANCEL_REQUESTED)
    action = state.actions[_action_index(state, event.action_id)]
    if action.status is not ActionStatus.STARTED:
        raise StateTransitionError("illegal_transition")
    return _replace_summaries(
        state,
        event,
        actions=state.actions,
        invocations=state.invocations,
    )


def _handle_succeeded(state: AttemptState, event: AttemptSucceeded) -> AttemptState:
    _require_phase(state, AttemptPhase.RUNNING)
    _require_terminal_actions(state)
    return _advance_attempt(
        state,
        event,
        phase=AttemptPhase.SUCCEEDED,
        result_ref=event.result_ref,
        terminal_error=None,
        started_at=state.started_at,
        finished_at=event.wall_time_utc,
    )


def _handle_failed(state: AttemptState, event: AttemptFailed) -> AttemptState:
    _require_phase(state, AttemptPhase.RUNNING)
    _require_terminal_actions(state)
    return _advance_attempt(
        state,
        event,
        phase=AttemptPhase.FAILED,
        result_ref=None,
        terminal_error=event.error,
        started_at=state.started_at,
        finished_at=event.wall_time_utc,
    )


def _handle_cancelled(state: AttemptState, event: AttemptCancelled) -> AttemptState:
    _require_phase(state, AttemptPhase.CANCEL_REQUESTED)
    _require_terminal_actions(state)
    return _advance_attempt(
        state,
        event,
        phase=AttemptPhase.CANCELLED,
        result_ref=None,
        terminal_error=None,
        started_at=state.started_at,
        finished_at=event.wall_time_utc,
    )


def _handle_timed_out(state: AttemptState, event: AttemptTimedOut) -> AttemptState:
    _require_phase(state, AttemptPhase.RUNNING)
    _require_terminal_actions(state)
    return _advance_attempt(
        state,
        event,
        phase=AttemptPhase.TIMED_OUT,
        result_ref=None,
        terminal_error=event.error,
        started_at=state.started_at,
        finished_at=event.wall_time_utc,
    )


def _handle_interrupted(state: AttemptState, event: AttemptInterrupted) -> AttemptState:
    _require_phase(state, AttemptPhase.RUNNING)
    _require_terminal_actions(state)
    return _advance_attempt(
        state,
        event,
        phase=AttemptPhase.INTERRUPTED,
        result_ref=None,
        terminal_error=event.error,
        started_at=state.started_at,
        finished_at=event.wall_time_utc,
    )


_Handler = Callable[[AttemptState, DomainEvent], AttemptState]
_HANDLERS: dict[type[DomainEvent], _Handler] = {
    AttemptStarted: cast(_Handler, _handle_started),
    AttemptRecoveryRequested: cast(_Handler, _handle_recovery_requested),
    PauseRequested: cast(_Handler, _handle_pause_requested),
    AttemptPaused: cast(_Handler, _handle_paused),
    AttemptResumed: cast(_Handler, _handle_resumed),
    CancelRequested: cast(_Handler, _handle_cancel_requested),
    AttemptSucceeded: cast(_Handler, _handle_succeeded),
    AttemptFailed: cast(_Handler, _handle_failed),
    AttemptCancelled: cast(_Handler, _handle_cancelled),
    AttemptTimedOut: cast(_Handler, _handle_timed_out),
    AttemptInterrupted: cast(_Handler, _handle_interrupted),
    StrategyDecisionRecorded: cast(_Handler, _handle_strategy_decision),
    BudgetReserved: cast(_Handler, _handle_budget_reserved),
    BudgetSettled: cast(_Handler, _handle_budget_settled),
    BudgetUncertainSettled: cast(_Handler, _handle_budget_settled),
    BudgetReleased: cast(_Handler, _handle_budget_released),
    ExternalInputRequested: cast(_Handler, _handle_external_requested),
    ExternalInputReceived: cast(_Handler, _handle_external_received),
    ExternalInputApproved: cast(_Handler, _handle_external_resolution),
    ExternalInputRejected: cast(_Handler, _handle_external_resolution),
    ExternalInputExpired: cast(_Handler, _handle_external_resolution),
    ActionProposed: cast(_Handler, _handle_action_proposed),
    ActionRejected: cast(_Handler, _handle_action_rejected),
    ActionAccepted: cast(_Handler, _handle_action_accepted),
    ActionStarted: cast(_Handler, _handle_action_started),
    ActionCancellationRequested: cast(
        _Handler,
        _handle_action_cancellation_requested,
    ),
    ActionSucceeded: cast(_Handler, _handle_action_terminal),
    ActionFailed: cast(_Handler, _handle_action_terminal),
    ActionTimedOut: cast(_Handler, _handle_action_terminal),
    ActionCancelled: cast(_Handler, _handle_action_terminal),
    ActionOutcomeUnknown: cast(_Handler, _handle_action_terminal),
    ActionOutcomeReconciled: cast(_Handler, _handle_action_reconciled),
    InvocationRequested: cast(_Handler, _handle_invocation_requested),
    InvocationStarted: cast(_Handler, _handle_invocation_started),
    InvocationCompleted: cast(_Handler, _handle_invocation_completed),
    InvocationFailed: cast(_Handler, _handle_invocation_failed),
}


def _validate_causal_shape(event: DomainEvent) -> None:
    if isinstance(event, AttemptPlanned):
        if event.causal_parent_id is not None:
            raise StateTransitionError("causal_parent_forbidden")
    elif event.causal_parent_id is None:
        raise StateTransitionError("causal_parent_required")


def _apply_validated_event(
    state: AttemptState | None, event: DomainEvent
) -> AttemptState:
    _validate_causal_shape(event)

    if state is None:
        if not isinstance(event, AttemptPlanned):
            raise StateTransitionError("state_required")
        return _create_attempt(event)

    if isinstance(event, AttemptPlanned):
        raise StateTransitionError("state_already_exists")
    if event.trial_id != state.trial_id or event.attempt_id != state.attempt_id:
        raise StateTransitionError("identity_mismatch")

    expected_sequence = state.revision + 1
    if event.sequence_no < expected_sequence:
        raise StateTransitionError("stale_sequence")
    if event.sequence_no > expected_sequence:
        raise StateTransitionError("sequence_gap")
    if state.phase in _TERMINAL_PHASES:
        raise StateTransitionError("terminal_state")

    handler = _HANDLERS.get(type(event))
    if handler is None:
        raise StateTransitionError("illegal_transition")
    return handler(state, event)


def apply_event(state: AttemptState | None, event: DomainEvent) -> AttemptState:
    validated_event = parse_domain_event(event)
    validated_state = (
        None
        if state is None
        else AttemptState.model_validate(_to_validation_payload(state))
    )
    return _apply_validated_event(validated_state, validated_event)


def _same_transaction(parent: DomainEvent, child: DomainEvent) -> bool:
    return (
        child.command_id == parent.command_id
        and child.causal_parent_id == parent.event_id
    )


def _request_matches_requirement(
    request: ExternalRequest,
    requirement: object,
) -> bool:
    return all(
        getattr(request, field) == getattr(requirement, field)
        for field in ("request_id", "request_kind", "action_id", "payload_ref")
    )


def _same_request_identity(
    request_event: ExternalInputRequested,
    response_event: ExternalInputReceived
    | ExternalInputApproved
    | ExternalInputRejected
    | ExternalInputExpired,
) -> bool:
    return (
        request_event.request.request_id == response_event.request_id
        and request_event.request.request_kind is response_event.request_kind
        and request_event.request.action_id == response_event.action_id
    )


def _command_batches(
    events: tuple[DomainEvent, ...],
) -> tuple[tuple[DomainEvent, ...], ...]:
    batches: list[tuple[DomainEvent, ...]] = []
    current: list[DomainEvent] = []
    closed_command_ids: set[UUID] = set()
    for event in events:
        if current and event.command_id != current[-1].command_id:
            closed_command_ids.add(current[-1].command_id)
            batches.append(tuple(current))
            current = []
            if event.command_id in closed_command_ids:
                raise StateTransitionError("duplicate_command_id")
        current.append(event)
    if current:
        batches.append(tuple(current))
    return tuple(batches)


def _require_external_shape(condition: bool) -> None:
    if not condition:
        raise StateTransitionError("invalid_external_transition")


def _require_linear_batch(batch: tuple[DomainEvent, ...]) -> None:
    _require_external_shape(
        all(
            _same_transaction(parent, child)
            for parent, child in zip(batch, batch[1:])
        )
    )


def _resolution_matches_received(
    received: ExternalInputReceived,
    resolution: ExternalInputApproved | ExternalInputRejected,
) -> bool:
    return (
        received.request_id == resolution.request_id
        and received.request_kind is resolution.request_kind
        and received.action_id == resolution.action_id
    )


def _action_requires_invocation(
    action_shapes: dict[str, tuple[object, str | None]],
    action_id: str,
) -> bool:
    action_shape = action_shapes.get(action_id)
    _require_external_shape(action_shape is not None)
    assert action_shape is not None
    return action_shape[0] in {
        ActionType.INVOKE_AGENT,
        ActionType.INVOKE_AGENT.value,
        "invoke_agent",
    }


def _record_action_context(
    batch: tuple[DomainEvent, ...],
    action_shapes: dict[str, tuple[object, str | None]],
    action_statuses: dict[str, ActionStatus],
) -> None:
    for event in batch:
        if isinstance(event, ActionProposed):
            action_id = event.proposal.action_id
            shape = (
                event.proposal.action_type,
                event.proposal.invocation_id,
            )
            _require_external_shape(action_id not in action_shapes)
            action_shapes[action_id] = shape
            action_statuses[action_id] = ActionStatus.PROPOSED
        elif isinstance(event, ActionAccepted):
            action_id = event.action.action_id
            shape = (event.action.action_type, event.action.invocation_id)
            _require_external_shape(action_shapes.get(action_id) == shape)
            action_statuses[action_id] = ActionStatus.ACCEPTED
        elif isinstance(event, ActionRejected):
            action_statuses[event.action_id] = ActionStatus.REJECTED
        elif isinstance(event, ActionStarted):
            action_statuses[event.action_id] = ActionStatus.STARTED
        elif isinstance(
            event,
            (
                ActionSucceeded,
                ActionFailed,
                ActionTimedOut,
                ActionCancelled,
                ActionOutcomeUnknown,
            ),
        ):
            action_statuses[event.action_id] = event.outcome.status


def _validate_approval_response_batch(
    batch: tuple[DomainEvent, ...],
    received: ExternalInputReceived,
    action_shapes: dict[str, tuple[object, str | None]],
) -> None:
    assert received.action_id is not None
    if received.response_kind is ExternalResponseKind.REJECT:
        _require_external_shape(
            len(batch) == 3
            and isinstance(batch[1], ExternalInputRejected)
            and isinstance(batch[2], ActionRejected)
            and batch[2].action_id == received.action_id
        )
        return

    _require_external_shape(
        received.response_kind is ExternalResponseKind.APPROVE
        and isinstance(batch[1], ExternalInputApproved)
    )
    if len(batch) == 3 and isinstance(batch[2], ActionRejected):
        _require_external_shape(batch[2].action_id == received.action_id)
        return

    requires_invocation = _action_requires_invocation(
        action_shapes,
        received.action_id,
    )
    expected_length = 5 if requires_invocation else 4
    _require_external_shape(len(batch) == expected_length)
    offset = 2
    if requires_invocation:
        invocation = batch[offset]
        action_shape = action_shapes[received.action_id]
        _require_external_shape(
            isinstance(invocation, InvocationRequested)
            and invocation.action_id == received.action_id
            and invocation.invocation_id == action_shape[1]
        )
        offset += 1
    budget = batch[offset]
    accepted = batch[offset + 1]
    _require_external_shape(
        isinstance(budget, BudgetReserved)
        and len(budget.reservations) == 1
        and budget.reservations[0].action_id == received.action_id
        and isinstance(accepted, ActionAccepted)
        and accepted.action.action_id == received.action_id
        and accepted.action.reservation_id
        == budget.reservations[0].reservation_id
    )


def _validate_reconciliation_response_batch(
    batch: tuple[DomainEvent, ...],
    received: ExternalInputReceived,
    action_shapes: dict[str, tuple[object, str | None]],
) -> None:
    assert received.action_id is not None
    if received.response_kind is ExternalResponseKind.ABANDON:
        _require_external_shape(
            len(batch) >= 4
            and isinstance(batch[1], ExternalInputRejected)
            and isinstance(batch[2], BudgetUncertainSettled)
            and batch[2].reservation.action_id == received.action_id
            and isinstance(batch[-1], AttemptInterrupted)
            and (len(batch) - 4) % 2 == 0
        )
        for index in range(3, len(batch) - 1, 2):
            cancelled = batch[index]
            released = batch[index + 1]
            _require_external_shape(
                isinstance(cancelled, ActionCancelled)
                and isinstance(released, BudgetReleased)
                and released.reservation.action_id == cancelled.action_id
            )
        return

    _require_external_shape(
        received.response_kind
        in {
            ExternalResponseKind.CONFIRM_SUCCEEDED,
            ExternalResponseKind.CONFIRM_FAILED,
        }
        and isinstance(batch[1], ExternalInputApproved)
    )
    requires_invocation = _action_requires_invocation(
        action_shapes,
        received.action_id,
    )
    expected_length = 5 if requires_invocation else 4
    _require_external_shape(len(batch) == expected_length)
    offset = 2
    invocation: DomainEvent | None = None
    if requires_invocation:
        invocation = batch[offset]
        action_shape = action_shapes[received.action_id]
        expected_invocation_type = (
            InvocationCompleted
            if received.response_kind is ExternalResponseKind.CONFIRM_SUCCEEDED
            else InvocationFailed
        )
        _require_external_shape(
            isinstance(invocation, expected_invocation_type)
            and invocation.action_id == received.action_id
            and invocation.invocation_id == action_shape[1]
        )
        offset += 1
    reconciled = batch[offset]
    settled = batch[offset + 1]
    _require_external_shape(
        isinstance(reconciled, ActionOutcomeReconciled)
        and reconciled.action_id == received.action_id
        and isinstance(settled, BudgetSettled)
        and settled.reservation.action_id == received.action_id
    )
    if received.response_kind is ExternalResponseKind.CONFIRM_SUCCEEDED:
        _require_external_shape(
            isinstance(reconciled.outcome, ActionSucceededOutcome)
            and reconciled.outcome.result_ref == received.response_ref
            and (
                invocation is None
                or (
                    isinstance(invocation, InvocationCompleted)
                    and invocation.latest_context_ref == received.response_ref
                )
            )
        )
    else:
        _require_external_shape(
            isinstance(reconciled.outcome, ActionFailedOutcome)
            and reconciled.outcome.error == received.error
            and (
                invocation is None
                or (
                    isinstance(invocation, InvocationFailed)
                    and invocation.error == received.error
                )
            )
        )


def _validate_expiry_batch(
    batch: tuple[DomainEvent, ...],
    request_event: ExternalInputRequested,
    action_statuses: dict[str, ActionStatus],
) -> None:
    expired = batch[0]
    assert isinstance(expired, ExternalInputExpired)
    _require_external_shape(_same_request_identity(request_event, expired))
    index = 1
    if expired.request_kind is ExternalRequestKind.ACTION_APPROVAL:
        _require_external_shape(
            index < len(batch)
            and isinstance(batch[index], ActionRejected)
            and batch[index].action_id == expired.action_id
        )
        index += 1
    elif expired.request_kind is ExternalRequestKind.OUTCOME_RECONCILIATION:
        _require_external_shape(
            index < len(batch)
            and isinstance(batch[index], BudgetUncertainSettled)
            and batch[index].reservation.action_id == expired.action_id
        )
        index += 1

    _require_external_shape(index < len(batch))
    expected_accepted = {
        action_id
        for action_id, status in action_statuses.items()
        if status is ActionStatus.ACCEPTED
    }
    expected_started = {
        action_id
        for action_id, status in action_statuses.items()
        if status is ActionStatus.STARTED
    }
    if isinstance(batch[index], CancelRequested):
        index += 1
        cancelled_ids: list[str] = []
        while (
            index + 1 < len(batch)
            and isinstance(batch[index], ActionCancelled)
            and isinstance(batch[index + 1], BudgetReleased)
        ):
            _require_external_shape(
                batch[index + 1].reservation.action_id
                == batch[index].action_id
            )
            cancelled_ids.append(batch[index].action_id)
            index += 2
        _require_external_shape(
            len(cancelled_ids) == len(expected_accepted)
            and set(cancelled_ids) == expected_accepted
        )
        cancellation_request_ids: list[str] = []
        while index < len(batch) and isinstance(
            batch[index],
            ActionCancellationRequested,
        ):
            cancellation_request_ids.append(batch[index].action_id)
            index += 1
        _require_external_shape(
            len(cancellation_request_ids) == len(expected_started)
            and set(cancellation_request_ids) == expected_started
        )
        if index < len(batch) and isinstance(batch[index], AttemptCancelled):
            _require_external_shape(not expected_started)
            index += 1
        else:
            _require_external_shape(bool(expected_started))
        _require_external_shape(index == len(batch))
        return

    _require_external_shape(not expected_started)
    cancelled_ids = []
    while (
        index + 1 < len(batch)
        and isinstance(batch[index], ActionCancelled)
        and isinstance(batch[index + 1], BudgetReleased)
    ):
        _require_external_shape(
            batch[index + 1].reservation.action_id == batch[index].action_id
        )
        cancelled_ids.append(batch[index].action_id)
        index += 2
    _require_external_shape(
        len(cancelled_ids) == len(expected_accepted)
        and set(cancelled_ids) == expected_accepted
    )
    _require_external_shape(
        index == len(batch) - 1
        and isinstance(batch[index], AttemptTimedOut)
    )


def _validate_external_transaction_shapes(
    events: tuple[DomainEvent, ...],
) -> None:
    preceding_event_ids = {
        event.event_id: events[index - 1].event_id
        for index, event in enumerate(events)
        if index > 0
    }
    action_shapes: dict[str, tuple[object, str | None]] = {}
    action_statuses: dict[str, ActionStatus] = {}
    used_request_ids: set[str] = set()
    reserved_reconciliation_ids: set[str] = set()
    pending_request: ExternalInputRequested | None = None
    lifecycle_types = (
        ExternalInputRequested,
        ExternalInputReceived,
        ExternalInputApproved,
        ExternalInputRejected,
        ExternalInputExpired,
        BudgetUncertainSettled,
        ActionOutcomeReconciled,
    )

    for batch in _command_batches(events):
        decisions = tuple(
            event for event in batch if isinstance(event, StrategyDecisionRecorded)
        )
        batch_requirement_ids = {
            requirement.request_id
            for decision in decisions
            for requirement in decision.external_requirements
        }
        for decision in decisions:
            requirement_ids = {
                requirement.request_id
                for requirement in decision.external_requirements
            }
            proposal_reconciliation_ids = {
                outcome_reconciliation_request_id(
                    decision.attempt_id,
                    proposal.action_id,
                )
                for proposal in decision.proposals
            }
            if any(
                request_id in used_request_ids
                or request_id in reserved_reconciliation_ids
                for request_id in requirement_ids
            ) or any(
                request_id in used_request_ids
                or request_id in requirement_ids
                for request_id in proposal_reconciliation_ids
            ):
                raise StateTransitionError("duplicate_external_request_id")
            reserved_reconciliation_ids.update(proposal_reconciliation_ids)
        admitted_reconciliation_ids = {
            outcome_reconciliation_request_id(
                event.attempt_id,
                event.proposal.action_id,
            )
            for event in batch
            if isinstance(event, ActionProposed)
        }
        if any(
            request_id in used_request_ids
            or request_id in batch_requirement_ids
            for request_id in admitted_reconciliation_ids
        ):
            raise StateTransitionError("duplicate_external_request_id")
        reserved_reconciliation_ids.update(admitted_reconciliation_ids)

        has_external_lifecycle = any(
            isinstance(event, lifecycle_types) for event in batch
        )
        has_external_decision = any(
            decision.external_requirements for decision in decisions
        )
        if not has_external_lifecycle and not has_external_decision:
            _record_action_context(batch, action_shapes, action_statuses)
            continue

        _require_linear_batch(batch)
        first = batch[0]
        if any(isinstance(event, BudgetUncertainSettled) for event in batch):
            authorized_uncertain = (
                isinstance(first, ExternalInputReceived)
                and first.response_kind is ExternalResponseKind.ABANDON
                and len(batch) >= 3
                and isinstance(batch[1], ExternalInputRejected)
                and isinstance(batch[2], BudgetUncertainSettled)
            ) or (
                isinstance(first, ExternalInputExpired)
                and first.request_kind
                is ExternalRequestKind.OUTCOME_RECONCILIATION
                and len(batch) >= 2
                and isinstance(batch[1], BudgetUncertainSettled)
            )
            if not authorized_uncertain:
                raise StateTransitionError("invalid_budget_transition")
        request_event: ExternalInputRequested | None = None
        if isinstance(first, StrategyDecisionRecorded):
            _require_external_shape(
                len(first.external_requirements) == 1
                and len(decisions) == 1
            )
            requirement = first.external_requirements[0]
            if requirement.request_kind is ExternalRequestKind.ACTION_APPROVAL:
                _require_external_shape(
                    len(batch) == 3
                    and isinstance(batch[1], ActionProposed)
                    and isinstance(batch[2], ExternalInputRequested)
                )
                proposed = batch[1]
                request_event = batch[2]
                expected_proposal = first.proposals[0].model_copy(
                    update={"causal_parent_id": str(first.event_id)}
                )
                _require_external_shape(
                    proposed.proposal == expected_proposal
                )
            else:
                _require_external_shape(
                    requirement.request_kind
                    is ExternalRequestKind.ADDITIONAL_INPUT
                    and len(batch) == 2
                    and isinstance(batch[1], ExternalInputRequested)
                )
                request_event = batch[1]
            _require_external_shape(
                _request_matches_requirement(
                    request_event.request,
                    requirement,
                )
            )
        elif isinstance(first, ActionOutcomeUnknown):
            _require_external_shape(
                len(batch) == 2
                and isinstance(batch[1], ExternalInputRequested)
            )
            request_event = batch[1]
            _require_external_shape(
                request_event.request.request_kind
                is ExternalRequestKind.OUTCOME_RECONCILIATION
                and request_event.request.action_id == first.action_id
                and request_event.request.payload_ref is None
                and request_event.request.request_id
                == outcome_reconciliation_request_id(
                    first.attempt_id,
                    first.action_id,
                )
            )
        elif isinstance(first, ExternalInputReceived):
            _require_external_shape(
                pending_request is not None
                and len(batch) >= 2
                and first.causal_parent_id
                == preceding_event_ids.get(first.event_id)
                and _same_request_identity(pending_request, first)
                and isinstance(
                    batch[1],
                    (ExternalInputApproved, ExternalInputRejected),
                )
                and _resolution_matches_received(first, batch[1])
            )
            should_approve = first.response_kind in {
                ExternalResponseKind.APPROVE,
                ExternalResponseKind.PROVIDE_INPUT,
                ExternalResponseKind.CONFIRM_SUCCEEDED,
                ExternalResponseKind.CONFIRM_FAILED,
            }
            _require_external_shape(
                isinstance(batch[1], ExternalInputApproved) is should_approve
            )
            if first.request_kind is ExternalRequestKind.ADDITIONAL_INPUT:
                _require_external_shape(
                    len(batch) == 2
                    and isinstance(batch[1], ExternalInputApproved)
                )
            elif first.request_kind is ExternalRequestKind.ACTION_APPROVAL:
                _validate_approval_response_batch(batch, first, action_shapes)
            else:
                _validate_reconciliation_response_batch(
                    batch,
                    first,
                    action_shapes,
                )
            pending_request = None
            _record_action_context(batch, action_shapes, action_statuses)
            continue
        elif isinstance(first, ExternalInputExpired):
            _require_external_shape(
                pending_request is not None
                and first.causal_parent_id
                == preceding_event_ids.get(first.event_id)
            )
            assert pending_request is not None
            _validate_expiry_batch(
                batch,
                pending_request,
                action_statuses,
            )
            pending_request = None
            _record_action_context(batch, action_shapes, action_statuses)
            continue
        else:
            _require_external_shape(False)

        assert request_event is not None
        request_id = request_event.request.request_id
        if request_id in used_request_ids:
            raise StateTransitionError("duplicate_external_request_id")
        _require_external_shape(pending_request is None)
        if (
            request_event.request.request_kind
            is not ExternalRequestKind.OUTCOME_RECONCILIATION
            and request_id in reserved_reconciliation_ids
        ):
            raise StateTransitionError("duplicate_external_request_id")
        used_request_ids.add(request_id)
        pending_request = request_event
        _record_action_context(batch, action_shapes, action_statuses)


def replay_events(events: Iterable[DomainEvent]) -> AttemptState:
    validated_events = tuple(parse_domain_event(event) for event in events)
    _validate_external_transaction_shapes(validated_events)
    effective_trigger_ids = {
        event.event_id for event in effective_strategy_triggers(validated_events)
    }
    state: AttemptState | None = None
    seen_event_ids: dict[UUID, tuple[str, str, int]] = {}
    proposed_action_ids: set[str] = set()
    reservations_by_id: dict[str, BudgetReservationEntry] = {}
    reservation_id_by_action: dict[str, str] = {}
    closed_reservation_ids: set[str] = set()
    uncertain_settled_reservation_ids: set[str] = set()
    started_action_ids: set[str] = set()
    pending_strategy_triggers: list[int] = []
    terminal_strategy_decision = False
    event_seen = False

    for event in validated_events:
        event_seen = True
        if event.event_id in seen_event_ids:
            raise StateTransitionError("duplicate_event_id")

        if not isinstance(event, AttemptPlanned):
            parent_id = event.causal_parent_id
            if parent_id is None:
                raise StateTransitionError("causal_parent_required")
            parent = seen_event_ids.get(parent_id)
            if parent is None:
                raise StateTransitionError("causal_parent_not_seen")
            parent_trial_id, parent_attempt_id, parent_sequence = parent
            if (
                parent_trial_id != event.trial_id
                or parent_attempt_id != event.attempt_id
            ):
                raise StateTransitionError("causal_parent_wrong_attempt")
            if parent_sequence >= event.sequence_no:
                raise StateTransitionError("causal_parent_not_earlier")

        if isinstance(event, StrategyDecisionRecorded):
            if terminal_strategy_decision:
                raise StateTransitionError("terminal_strategy_decision")
            if (
                not pending_strategy_triggers
                or (
                    not (
                        event._legacy_cursor_origin
                        and _has_legacy_cursor_shape(event)
                    )
                    and event.trigger_sequence_no != pending_strategy_triggers[0]
                )
            ):
                raise StateTransitionError("invalid_strategy_cursor")

        if isinstance(event, BudgetReserved):
            for reservation in event.reservations:
                if (
                    reservation.action_id not in proposed_action_ids
                    or reservation.reservation_id in reservations_by_id
                    or reservation.action_id in reservation_id_by_action
                ):
                    raise StateTransitionError("invalid_budget_transition")
        elif isinstance(event, ActionAccepted):
            reservation_id = reservation_id_by_action.get(event.action.action_id)
            reservation = (
                None
                if reservation_id is None
                else reservations_by_id.get(reservation_id)
            )
            if (
                reservation is None
                or reservation.reservation_id != event.action.reservation_id
                or reservation.resource_requests != event.action.resource_requests
                or reservation.reservation_id in closed_reservation_ids
            ):
                raise StateTransitionError("invalid_budget_transition")
        elif isinstance(event, ActionStarted):
            if event.action_id in reservation_id_by_action:
                reservation_id = reservation_id_by_action.get(event.action_id)
                if (
                    reservation_id is None
                    or reservation_id in closed_reservation_ids
                ):
                    raise StateTransitionError("invalid_budget_transition")
        elif isinstance(
            event,
            (BudgetSettled, BudgetUncertainSettled, BudgetReleased),
        ):
            reservation = event.reservation
            expected = reservations_by_id.get(reservation.reservation_id)
            if (
                expected != reservation
                or reservation_id_by_action.get(reservation.action_id)
                != reservation.reservation_id
                or reservation.reservation_id in closed_reservation_ids
                or (
                    isinstance(event, BudgetReleased)
                    and reservation.action_id in started_action_ids
                )
                or (
                    isinstance(event, (BudgetSettled, BudgetUncertainSettled))
                    and reservation.action_id not in started_action_ids
                )
            ):
                raise StateTransitionError("invalid_budget_transition")

        state = _apply_validated_event(state, event)
        seen_event_ids[event.event_id] = (
            event.trial_id,
            event.attempt_id,
            event.sequence_no,
        )
        if isinstance(event, ActionProposed):
            proposed_action_ids.add(event.proposal.action_id)
        elif isinstance(event, StrategyDecisionRecorded):
            pending_strategy_triggers.pop(0)
            terminal_strategy_decision = (
                event.directive is not StrategyDirective.CONTINUE
            )
        elif event.event_id in effective_trigger_ids:
            pending_strategy_triggers.append(event.sequence_no)
        elif isinstance(event, BudgetReserved):
            for reservation in event.reservations:
                reservations_by_id[reservation.reservation_id] = reservation
                reservation_id_by_action[reservation.action_id] = (
                    reservation.reservation_id
                )
        elif isinstance(event, ActionStarted):
            started_action_ids.add(event.action_id)
        elif isinstance(
            event,
            (BudgetSettled, BudgetUncertainSettled, BudgetReleased),
        ):
            closed_reservation_ids.add(event.reservation.reservation_id)
            if isinstance(event, BudgetUncertainSettled):
                uncertain_settled_reservation_ids.add(
                    event.reservation.reservation_id
                )

    if not event_seen or state is None:
        raise StateTransitionError("empty_replay")
    actions_by_id = {action.action_id: action for action in state.actions}
    for action_id, reservation_id in reservation_id_by_action.items():
        action = actions_by_id.get(action_id)
        if action is None or action.reservation_id != reservation_id:
            raise StateTransitionError("invalid_budget_transition")
        closed = reservation_id in closed_reservation_ids
        if action.status in {
            ActionStatus.SUCCEEDED,
            ActionStatus.FAILED,
            ActionStatus.TIMED_OUT,
            ActionStatus.CANCELLED,
        }:
            if not closed:
                raise StateTransitionError("invalid_budget_transition")
        elif action.status in {ActionStatus.ACCEPTED, ActionStatus.STARTED}:
            if closed:
                raise StateTransitionError("invalid_budget_transition")
        elif action.status is ActionStatus.OUTCOME_UNKNOWN:
            should_be_closed = (
                action.reconciled_status is not None
                or reservation_id in uncertain_settled_reservation_ids
            )
            if closed is not should_be_closed:
                raise StateTransitionError("invalid_budget_transition")
        else:
            raise StateTransitionError("invalid_budget_transition")
    if state.phase in _TERMINAL_PHASES:
        has_open_reservation = any(
            reservation_id not in closed_reservation_ids
            for reservation_id in reservation_id_by_action.values()
        )
        if has_open_reservation or any(
            resource.reserved != 0 for resource in state.budget.resources
        ):
            raise StateTransitionError("invalid_budget_transition")
    return state
