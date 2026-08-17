from __future__ import annotations

from datetime import datetime, timedelta
from typing import Literal

from pydantic import ConfigDict, StrictStr, field_validator, model_validator

from .actions import ActionProposal
from .state import (
    ActionStatus,
    AttemptState,
    BudgetState,
    FrozenModel,
    ResourceBudget,
    ResourceKind,
    ResourceRequest,
)


GuardRejectionCode = Literal[
    "BUDGET_EXHAUSTED",
    "ATTEMPT_DEADLINE_EXPIRED",
    "MAX_CALL_DEPTH_EXCEEDED",
    "MAX_CONCURRENCY_EXCEEDED",
    "TOPOLOGY_EDGE_FORBIDDEN",
]


class _StrictFrozenModel(FrozenModel):
    model_config = ConfigDict(extra="forbid", frozen=True, strict=True)


class GuardRejection(_StrictFrozenModel):
    accepted: Literal[False] = False
    code: GuardRejectionCode
    action_id: StrictStr | None = None
    resource: ResourceKind | None = None
    target_id: StrictStr | None = None

    @field_validator("accepted", mode="before")
    @classmethod
    def validate_accepted(cls, value: object) -> object:
        if value is not False:
            raise ValueError("accepted must be exactly false")
        return value

    @model_validator(mode="after")
    def validate_diagnostics(self) -> GuardRejection:
        contracts = {
            "BUDGET_EXHAUSTED": (
                {"action_id", "resource"},
                {"action_id", "resource"},
            ),
            "ATTEMPT_DEADLINE_EXPIRED": (set(), set()),
            "MAX_CALL_DEPTH_EXCEEDED": (
                {"action_id"},
                {"action_id"},
            ),
            "MAX_CONCURRENCY_EXCEEDED": (set(), set()),
            "TOPOLOGY_EDGE_FORBIDDEN": (
                {"action_id", "target_id"},
                {"action_id", "target_id"},
            ),
        }
        required, allowed = contracts[self.code]
        present = {
            name
            for name in ("action_id", "resource", "target_id")
            if getattr(self, name) is not None
        }
        if not required <= present or not present <= allowed:
            raise ValueError("guard rejection diagnostics do not match the code")
        return self


class ActionReservation(_StrictFrozenModel):
    action_id: StrictStr
    resource_requests: tuple[ResourceRequest, ...]


class ReservationPlan(_StrictFrozenModel):
    accepted: Literal[True] = True
    reservations: tuple[ActionReservation, ...]
    next_budget: BudgetState

    @field_validator("accepted", mode="before")
    @classmethod
    def validate_accepted(cls, value: object) -> object:
        if value is not True:
            raise ValueError("accepted must be exactly true")
        return value


class BudgetGuard:
    @staticmethod
    def evaluate_batch(
        state: AttemptState,
        actions: tuple[ActionProposal, ...],
        *,
        now_utc: datetime,
    ) -> ReservationPlan | GuardRejection:
        BudgetGuard._validate_inputs(state, actions, now_utc=now_utc)

        budget = state.budget
        if budget.deadline_at is not None and now_utc >= budget.deadline_at:
            return GuardRejection(code="ATTEMPT_DEADLINE_EXPIRED")

        for action in actions:
            if action.call_depth > budget.max_call_depth:
                return GuardRejection(
                    code="MAX_CALL_DEPTH_EXCEEDED",
                    action_id=action.action_id,
                )

        active_count = sum(
            action.status in {ActionStatus.ACCEPTED, ActionStatus.STARTED}
            for action in state.actions
        )
        if active_count + len(actions) > budget.max_concurrent_actions:
            return GuardRejection(code="MAX_CONCURRENCY_EXCEEDED")

        requested_totals = {kind: 0 for kind in ResourceKind}
        normalized_requests: list[tuple[ResourceRequest, ...]] = []
        for action in actions:
            by_resource = {
                request.resource: request for request in action.resource_requests
            }
            normalized = tuple(
                by_resource[kind] for kind in ResourceKind if kind in by_resource
            )
            normalized_requests.append(normalized)
            for request in normalized:
                requested_totals[request.resource] += request.amount

        counters = budget.counters
        for action, requests in zip(actions, normalized_requests, strict=True):
            for request in requests:
                counter = counters.get(request.resource.value)
                total_requested = requested_totals[request.resource]
                if counter is None:
                    exhausted = True
                else:
                    exhausted = (
                        counter.consumed + counter.reserved + total_requested
                        > counter.limit
                    )
                if exhausted:
                    return GuardRejection(
                        code="BUDGET_EXHAUSTED",
                        action_id=action.action_id,
                        resource=request.resource,
                    )

        next_resources = tuple(
            ResourceBudget(
                resource=counter.resource,
                limit=counter.limit,
                reserved=counter.reserved
                + requested_totals.get(counter.resource, 0),
                consumed=counter.consumed,
            )
            for counter in budget.resources
        )
        next_budget = BudgetState(
            resources=next_resources,
            deadline_at=budget.deadline_at,
            max_call_depth=budget.max_call_depth,
            max_concurrent_actions=budget.max_concurrent_actions,
        )
        reservations = tuple(
            ActionReservation(
                action_id=action.action_id,
                resource_requests=requests,
            )
            for action, requests in zip(actions, normalized_requests, strict=True)
        )
        return ReservationPlan(
            reservations=reservations,
            next_budget=next_budget,
        )

    @staticmethod
    def _validate_inputs(
        state: AttemptState,
        actions: tuple[ActionProposal, ...],
        *,
        now_utc: datetime,
    ) -> None:
        if (
            not isinstance(now_utc, datetime)
            or now_utc.tzinfo is None
            or now_utc.utcoffset() != timedelta(0)
        ):
            raise ValueError("now_utc must be timezone-aware UTC")
        if not isinstance(state, AttemptState):
            raise ValueError("state must be an AttemptState")
        if not isinstance(actions, tuple):
            raise ValueError("actions must be a tuple")
        if any(not isinstance(action, ActionProposal) for action in actions):
            raise ValueError("actions must contain only ActionProposal values")
        action_ids = [action.action_id for action in actions]
        if len(action_ids) != len(set(action_ids)):
            raise ValueError("batch action_id values must be unique")


__all__ = [
    "ActionReservation",
    "BudgetGuard",
    "GuardRejection",
    "ReservationPlan",
]
