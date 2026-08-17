from __future__ import annotations

from datetime import datetime, timedelta, timezone
from hashlib import sha256

import pytest
from pydantic import ValidationError

from experiment_system.actions import ActionProposal, ActionType
from experiment_system.budget import BudgetGuard, GuardRejection, ReservationPlan
from experiment_system.state import (
    ActionState,
    ActionStatus,
    ArtifactRef,
    AttemptPhase,
    AttemptState,
    BudgetState,
    ErrorSummary,
    RecoveryPolicy,
    ResourceBudget,
    ResourceKind,
    ResourceRequest,
    StrategyStateEnvelope,
)
from experiment_system.topology import TopologyGuard


UTC_NOW = datetime(2026, 7, 23, 8, 30, tzinfo=timezone.utc)
HASH = "a" * 64


def _artifact(path: str) -> ArtifactRef:
    return ArtifactRef(
        capture_class="full",
        content_hash=HASH,
        media_type="application/json",
        byte_size=12,
        relative_path=path,
    )


def _budget(
    *,
    resources: tuple[ResourceBudget, ...] | None = None,
    deadline_at: datetime | None = None,
    max_call_depth: int = 4,
    max_concurrent_actions: int = 2,
) -> BudgetState:
    if resources is None:
        resources = tuple(
            ResourceBudget(resource=kind.value, limit=20)
            for kind in ResourceKind
        )
    return BudgetState(
        resources=resources,
        deadline_at=deadline_at,
        max_call_depth=max_call_depth,
        max_concurrent_actions=max_concurrent_actions,
    )


def _existing_action(action_id: str, status: ActionStatus) -> ActionState:
    accepted = status in {
        ActionStatus.ACCEPTED,
        ActionStatus.STARTED,
        ActionStatus.OUTCOME_UNKNOWN,
    }
    error = (
        ErrorSummary(
            code="OUTCOME_UNKNOWN",
            retryable=True,
            safe_message="The Action outcome is not known.",
        )
        if status is ActionStatus.OUTCOME_UNKNOWN
        else None
    )
    return ActionState(
        action_id=action_id,
        action_type=ActionType.SEND_MESSAGE.value,
        actor_id="root",
        target_ids=("worker-a",),
        status=status,
        causal_parent_id="event-1",
        invocation_id=None,
        payload_ref=None,
        result_ref=None,
        idempotency_key=f"idem-{action_id}" if accepted else None,
        recovery_policy=RecoveryPolicy.REPLAY_SAFE,
        reservation_id=f"reservation-{action_id}" if accepted else None,
        retry_of_action_id=None,
        error_code=None if error is None else error.code,
        error=error,
    )


def _state(
    *,
    budget: BudgetState | None = None,
    actions: tuple[ActionState, ...] = (),
) -> AttemptState:
    strategy_value = {"queue": []}
    strategy_bytes = b'{"queue":[]}'
    return AttemptState(
        schema_version=1,
        experiment_id="experiment-1",
        trial_id="trial-1",
        attempt_id="attempt-1",
        strategy_id="router",
        revision=1,
        phase=AttemptPhase.RUNNING,
        manifest_ref=_artifact("attempts/attempt-1/manifest.json"),
        strategy=StrategyStateEnvelope(
            strategy_id="router",
            strategy_schema_version=1,
            value=strategy_value,
            content_hash=sha256(strategy_bytes).hexdigest(),
            byte_size=len(strategy_bytes),
        ),
        budget=budget or _budget(),
        actions=actions,
        invocations=(),
        pending_external=(),
        result_ref=None,
        terminal_error=None,
        started_at=UTC_NOW,
        finished_at=None,
    )


def _proposal(
    action_id: str = "action-1",
    *,
    actor: str = "root",
    target: str = "worker-a",
    call_depth: int = 1,
    requests: tuple[ResourceRequest, ...] | None = None,
) -> ActionProposal:
    if requests is None:
        requests = (
            ResourceRequest(
                resource=ResourceKind.COORDINATION_ACTIONS,
                amount=1,
            ),
        )
    return ActionProposal(
        action_id=action_id,
        action_type=ActionType.SEND_MESSAGE,
        actor=actor,
        target_ids=(target,),
        invocation_id=None,
        causal_parent_id="event-1",
        payload_ref=None,
        recovery_policy=RecoveryPolicy.REPLAY_SAFE,
        requested_timeout=30,
        call_depth=call_depth,
        resource_requests=requests,
    )


def test_parallel_batch_reserves_every_member_atomically_in_input_order() -> None:
    first_action = _proposal(
        "action-1",
        requests=(
            ResourceRequest(resource=ResourceKind.MODEL_CALLS, amount=1),
            ResourceRequest(resource=ResourceKind.COORDINATION_ACTIONS, amount=1),
        ),
    )
    second_action = _proposal("action-2")

    plan = BudgetGuard.evaluate_batch(
        _state(), (first_action, second_action), now_utc=UTC_NOW
    )

    assert isinstance(plan, ReservationPlan)
    assert plan.accepted is True
    assert tuple(item.action_id for item in plan.reservations) == (
        first_action.action_id,
        second_action.action_id,
    )
    assert tuple(
        request.resource for request in plan.reservations[0].resource_requests
    ) == (ResourceKind.MODEL_CALLS, ResourceKind.COORDINATION_ACTIONS)
    assert (
        plan.next_budget.counters[ResourceKind.COORDINATION_ACTIONS].reserved
        == 2
    )


def test_over_budget_batch_returns_no_partial_reservations_and_changes_no_input() -> None:
    budget = _budget(
        resources=(
            ResourceBudget(
                resource=ResourceKind.COORDINATION_ACTIONS.value,
                limit=1,
            ),
        )
    )
    state = _state(budget=budget)
    actions = (_proposal("action-1"), _proposal("action-2"))
    before_state = state.model_dump(mode="python")
    before_actions = tuple(action.model_dump(mode="python") for action in actions)

    rejection = BudgetGuard.evaluate_batch(state, actions, now_utc=UTC_NOW)

    assert isinstance(rejection, GuardRejection)
    assert rejection.accepted is False
    assert rejection.code == "BUDGET_EXHAUSTED"
    assert not hasattr(rejection, "reservations")
    assert not hasattr(rejection, "next_budget")
    assert state.model_dump(mode="python") == before_state
    assert tuple(action.model_dump(mode="python") for action in actions) == before_actions


@pytest.mark.parametrize(
    ("limit", "reserved", "consumed", "amount", "accepted"),
    [
        (10, 3, 4, 3, True),
        (10, 3, 4, 4, False),
    ],
)
def test_additive_guard_includes_consumed_and_existing_reserved_budget(
    limit: int,
    reserved: int,
    consumed: int,
    amount: int,
    accepted: bool,
) -> None:
    budget = _budget(
        resources=(
            ResourceBudget(
                resource=ResourceKind.MODEL_CALLS.value,
                limit=limit,
                reserved=reserved,
                consumed=consumed,
            ),
        )
    )
    action = _proposal(
        requests=(ResourceRequest(resource=ResourceKind.MODEL_CALLS, amount=amount),)
    )

    result = BudgetGuard.evaluate_batch(_state(budget=budget), (action,), now_utc=UTC_NOW)

    assert result.accepted is accepted
    if isinstance(result, ReservationPlan):
        counter = result.next_budget.counters[ResourceKind.MODEL_CALLS]
        assert counter.reserved + counter.consumed <= counter.limit
        assert counter.reserved == reserved + amount


def test_deadline_equal_to_now_is_expired() -> None:
    state = _state(budget=_budget(deadline_at=UTC_NOW))

    rejection = BudgetGuard.evaluate_batch(
        state, (_proposal(),), now_utc=UTC_NOW
    )

    assert isinstance(rejection, GuardRejection)
    assert rejection.code == "ATTEMPT_DEADLINE_EXPIRED"


def test_depth_equal_to_limit_is_allowed_and_one_greater_is_rejected() -> None:
    state = _state(budget=_budget(max_call_depth=3))

    accepted = BudgetGuard.evaluate_batch(
        state, (_proposal(call_depth=3),), now_utc=UTC_NOW
    )
    rejected = BudgetGuard.evaluate_batch(
        state, (_proposal(call_depth=4),), now_utc=UTC_NOW
    )

    assert isinstance(accepted, ReservationPlan)
    assert isinstance(rejected, GuardRejection)
    assert rejected.code == "MAX_CALL_DEPTH_EXCEEDED"
    assert rejected.action_id == "action-1"


def test_concurrency_counts_only_accepted_and_started_plus_the_whole_batch() -> None:
    state = _state(
        budget=_budget(max_concurrent_actions=4),
        actions=(
            _existing_action("accepted", ActionStatus.ACCEPTED),
            _existing_action("started", ActionStatus.STARTED),
            _existing_action("proposed", ActionStatus.PROPOSED),
            _existing_action("unknown", ActionStatus.OUTCOME_UNKNOWN),
        ),
    )

    exact = BudgetGuard.evaluate_batch(
        state,
        (_proposal("action-1"), _proposal("action-2")),
        now_utc=UTC_NOW,
    )
    exceeded = BudgetGuard.evaluate_batch(
        state,
        (
            _proposal("action-1"),
            _proposal("action-2"),
            _proposal("action-3"),
        ),
        now_utc=UTC_NOW,
    )

    assert isinstance(exact, ReservationPlan)
    assert isinstance(exceeded, GuardRejection)
    assert exceeded.code == "MAX_CONCURRENCY_EXCEEDED"


def test_missing_resource_ledger_is_zero_limit_and_rejects_request() -> None:
    action = _proposal(
        requests=(ResourceRequest(resource=ResourceKind.MODEL_CALLS, amount=1),)
    )

    rejection = BudgetGuard.evaluate_batch(
        _state(budget=_budget(resources=())), (action,), now_utc=UTC_NOW
    )

    assert isinstance(rejection, GuardRejection)
    assert rejection.code == "BUDGET_EXHAUSTED"
    assert rejection.resource is ResourceKind.MODEL_CALLS


def test_missing_resource_ledger_rejects_explicit_zero_request_atomically() -> None:
    state = _state(budget=_budget(resources=()))
    action = _proposal(
        requests=(ResourceRequest(resource=ResourceKind.MODEL_CALLS, amount=0),)
    )
    before_state = state.model_dump(mode="python")
    before_action = action.model_dump(mode="python")

    rejection = BudgetGuard.evaluate_batch(state, (action,), now_utc=UTC_NOW)

    assert isinstance(rejection, GuardRejection)
    assert rejection.code == "BUDGET_EXHAUSTED"
    assert rejection.action_id == action.action_id
    assert rejection.resource is ResourceKind.MODEL_CALLS
    assert not hasattr(rejection, "reservations")
    assert not hasattr(rejection, "next_budget")
    assert state.model_dump(mode="python") == before_state
    assert action.model_dump(mode="python") == before_action


def test_empty_batch_returns_a_new_equal_budget_and_duplicate_ids_are_invalid() -> None:
    state = _state()

    plan = BudgetGuard.evaluate_batch(state, (), now_utc=UTC_NOW)

    assert isinstance(plan, ReservationPlan)
    assert plan.reservations == ()
    assert plan.next_budget == state.budget
    assert plan.next_budget is not state.budget
    with pytest.raises(ValueError, match="action_id"):
        BudgetGuard.evaluate_batch(
            state, (_proposal(), _proposal()), now_utc=UTC_NOW
        )
    with pytest.raises(ValueError, match="tuple"):
        BudgetGuard.evaluate_batch(state, [_proposal()], now_utc=UTC_NOW)  # type: ignore[arg-type]


@pytest.mark.parametrize(
    "now_utc",
    [
        datetime(2026, 7, 23, 8, 30),
        datetime(2026, 7, 23, 16, 30, tzinfo=timezone(timedelta(hours=8))),
    ],
)
def test_now_must_be_aware_utc(now_utc: datetime) -> None:
    with pytest.raises(ValueError, match="UTC"):
        BudgetGuard.evaluate_batch(_state(), (_proposal(),), now_utc=now_utc)

    with pytest.raises(ValueError, match="UTC"):
        BudgetGuard.evaluate_batch(
            _state(), (_proposal(), _proposal()), now_utc=now_utc
        )


def test_failure_precedence_is_deadline_then_depth_then_concurrency_then_budget() -> None:
    zero_budget = _budget(
        resources=(),
        deadline_at=UTC_NOW,
        max_call_depth=0,
        max_concurrent_actions=1,
    )
    state = _state(
        budget=zero_budget,
        actions=(_existing_action("accepted", ActionStatus.ACCEPTED),),
    )
    action = _proposal(call_depth=1)
    assert BudgetGuard.evaluate_batch(state, (action,), now_utc=UTC_NOW).code == (
        "ATTEMPT_DEADLINE_EXPIRED"
    )

    state = _state(
        budget=zero_budget.model_copy(update={"deadline_at": None}),
        actions=state.actions,
    )
    assert BudgetGuard.evaluate_batch(state, (action,), now_utc=UTC_NOW).code == (
        "MAX_CALL_DEPTH_EXCEEDED"
    )

    action = _proposal(call_depth=0)
    assert BudgetGuard.evaluate_batch(state, (action,), now_utc=UTC_NOW).code == (
        "MAX_CONCURRENCY_EXCEEDED"
    )

    state = _state(budget=zero_budget.model_copy(update={"deadline_at": None}))
    assert BudgetGuard.evaluate_batch(state, (action,), now_utc=UTC_NOW).code == (
        "BUDGET_EXHAUSTED"
    )


def test_resource_failure_uses_enum_order_not_proposal_request_order() -> None:
    action = _proposal(
        requests=(
            ResourceRequest(resource=ResourceKind.MODEL_CALLS, amount=1),
            ResourceRequest(resource=ResourceKind.INPUT_TOKENS, amount=1),
        )
    )

    rejection = BudgetGuard.evaluate_batch(
        _state(budget=_budget(resources=())), (action,), now_utc=UTC_NOW
    )

    assert isinstance(rejection, GuardRejection)
    assert rejection.resource is ResourceKind.INPUT_TOKENS


def test_guard_results_and_budget_counter_view_are_immutable() -> None:
    plan = BudgetGuard.evaluate_batch(
        _state(), (_proposal(),), now_utc=UTC_NOW
    )
    assert isinstance(plan, ReservationPlan)

    with pytest.raises(ValidationError, match="frozen_instance"):
        plan.accepted = False  # type: ignore[misc]
    with pytest.raises(TypeError):
        plan.next_budget.counters[ResourceKind.MODEL_CALLS] = ResourceBudget(  # type: ignore[index]
            resource=ResourceKind.MODEL_CALLS.value,
            limit=1,
        )

    with pytest.raises(ValidationError):
        GuardRejection(accepted=0, code="BUDGET_EXHAUSTED")
    with pytest.raises(ValidationError):
        ReservationPlan(
            accepted=1,
            reservations=plan.reservations,
            next_budget=plan.next_budget,
        )


@pytest.mark.parametrize(
    "values",
    [
        {
            "code": "BUDGET_EXHAUSTED",
            "action_id": "action-1",
            "resource": ResourceKind.MODEL_CALLS,
        },
        {"code": "ATTEMPT_DEADLINE_EXPIRED"},
        {"code": "MAX_CALL_DEPTH_EXCEEDED", "action_id": "action-1"},
        {"code": "MAX_CONCURRENCY_EXCEEDED"},
        {
            "code": "TOPOLOGY_EDGE_FORBIDDEN",
            "action_id": "action-1",
            "target_id": "worker-a",
        },
    ],
)
def test_guard_rejection_accepts_only_the_diagnostics_for_its_code(
    values: dict[str, object],
) -> None:
    rejection = GuardRejection(**values)  # type: ignore[arg-type]
    assert rejection.code == values["code"]


@pytest.mark.parametrize(
    "values",
    [
        {"code": "BUDGET_EXHAUSTED", "resource": ResourceKind.MODEL_CALLS},
        {
            "code": "BUDGET_EXHAUSTED",
            "action_id": "action-1",
            "resource": ResourceKind.MODEL_CALLS,
            "target_id": "worker-a",
        },
        {"code": "ATTEMPT_DEADLINE_EXPIRED", "action_id": "action-1"},
        {"code": "MAX_CALL_DEPTH_EXCEEDED"},
        {
            "code": "MAX_CALL_DEPTH_EXCEEDED",
            "action_id": "action-1",
            "resource": ResourceKind.MODEL_CALLS,
        },
        {"code": "MAX_CONCURRENCY_EXCEEDED", "target_id": "worker-a"},
        {"code": "TOPOLOGY_EDGE_FORBIDDEN", "action_id": "action-1"},
        {
            "code": "TOPOLOGY_EDGE_FORBIDDEN",
            "action_id": "action-1",
            "target_id": "worker-a",
            "resource": ResourceKind.MODEL_CALLS,
        },
    ],
)
def test_guard_rejection_rejects_missing_or_contradictory_diagnostics(
    values: dict[str, object],
) -> None:
    with pytest.raises(ValidationError, match="diagnostic"):
        GuardRejection(**values)  # type: ignore[arg-type]


def test_topology_checks_only_exact_directed_edges_for_every_target() -> None:
    allowed_edges = frozenset({("root", "worker-a"), ("worker-a", "worker-b")})

    assert (
        TopologyGuard.evaluate(
            action=_proposal(target="worker-a"), allowed_edges=allowed_edges
        )
        is None
    )
    for action in (
        _proposal(actor="worker-a", target="root"),
        _proposal(actor="root", target="worker-b"),
        _proposal(actor="root", target="root"),
    ):
        rejection = TopologyGuard.evaluate(
            action=action, allowed_edges=allowed_edges
        )
        assert isinstance(rejection, GuardRejection)
        assert rejection.code == "TOPOLOGY_EDGE_FORBIDDEN"
        assert rejection.action_id == action.action_id
        assert rejection.target_id == action.target_ids[0]


def test_topology_allowed_edges_must_be_immutable_and_are_not_changed() -> None:
    allowed_edges = frozenset({("root", "worker-a")})
    before = allowed_edges

    TopologyGuard.evaluate(action=_proposal(), allowed_edges=allowed_edges)

    assert allowed_edges is before
    with pytest.raises(ValueError, match="frozenset"):
        TopologyGuard.evaluate(  # type: ignore[arg-type]
            action=_proposal(), allowed_edges={("root", "worker-a")}
        )


def test_duplicate_targets_are_rejected_by_action_model_before_topology_guard() -> None:
    values = _proposal().model_dump(mode="python")
    values["target_ids"] = ("worker-a", "worker-a")

    with pytest.raises(ValidationError, match="unique"):
        ActionProposal.model_validate(values)
