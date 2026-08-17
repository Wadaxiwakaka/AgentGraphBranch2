from __future__ import annotations

import math
from collections.abc import Mapping
from enum import StrEnum
from types import MappingProxyType

from pydantic import ConfigDict, TypeAdapter

from .actions import ActionOutcome, ActionType, ActionUnknownOutcome, NormalizedAction
from .commands import ReportActionOutcome, ReportActionStarted, action_command_id
from .contract import (
    Backend,
    Clock,
    ExecutionContext,
    FaultInjector,
    FaultPoint,
    NoOpFaultInjector,
    ReconcilableBackend,
)
from .engine import AttemptEngine
from .state import (
    ActionState,
    ActionStatus,
    AttemptPhase,
    ErrorSummary,
    FrozenModel,
    RecoveryPolicy,
)
from .store import (
    AttemptRepository,
    ClaimedAction,
    OutboxActionStatus,
    RepositoryError,
)


_ACTION_OUTCOME_ADAPTER = TypeAdapter(ActionOutcome)
_OUTCOME_COMMIT_MAX_ATTEMPTS = 2


class ExecutorStepStatus(StrEnum):
    IDLE = "IDLE"
    COMPLETED = "COMPLETED"
    RECOVERY_REQUIRED = "RECOVERY_REQUIRED"


class ExecutorStepResult(FrozenModel):
    model_config = ConfigDict(extra="forbid", frozen=True, strict=True)

    status: ExecutorStepStatus


class ExecutorConfigurationError(ValueError):
    code = "executor_configuration_error"
    safe_message = "The Action executor is not configured for this Action type."

    def __init__(self) -> None:
        super().__init__(self.safe_message)


class ExecutorInvariantError(RuntimeError):
    code = "executor_invariant_error"
    safe_message = "The claimed Action does not match durable Attempt state."

    def __init__(self) -> None:
        super().__init__(self.safe_message)


class ActionExecutor:
    def __init__(
        self,
        *,
        backends: Mapping[ActionType, Backend],
        repository: AttemptRepository,
        engine: AttemptEngine,
        clock: Clock,
        lease_seconds: float,
        fault_injector: FaultInjector | None = None,
    ) -> None:
        if not isinstance(backends, Mapping):
            raise ValueError("backends must be a mapping keyed by ActionType")
        copied_backends: dict[ActionType, Backend] = {}
        for action_type, backend in backends.items():
            if not isinstance(action_type, ActionType):
                raise ValueError("backends must be keyed by ActionType")
            if not isinstance(backend, Backend):
                raise ValueError("every backend must implement Backend")
            copied_backends[action_type] = backend
        if (
            isinstance(lease_seconds, bool)
            or not isinstance(lease_seconds, (int, float))
            or not math.isfinite(lease_seconds)
            or lease_seconds <= 0
        ):
            raise ValueError("lease_seconds must be a positive finite number")
        if fault_injector is not None and not isinstance(
            fault_injector, FaultInjector
        ):
            raise ValueError("fault_injector must implement FaultInjector")
        if fault_injector is not None and not callable(
            getattr(fault_injector, "hit", None)
        ):
            raise ValueError("fault_injector.hit must be callable")
        self._backends = MappingProxyType(copied_backends)
        self._repository = repository
        self._engine = engine
        self._clock = clock
        self._lease_seconds = float(lease_seconds)
        self._fault_injector = (
            fault_injector
            if fault_injector is not None
            else NoOpFaultInjector()
        )

    async def run_once(self, worker_id: str) -> ExecutorStepResult:
        claim = await self._repository.claim_action(
            worker_id=worker_id,
            now_utc=self._clock.now_utc(),
            lease_seconds=self._lease_seconds,
        )
        if claim is None:
            return ExecutorStepResult(status=ExecutorStepStatus.IDLE)
        self._fault_injector.hit(
            FaultPoint.AFTER_CLAIM_BEFORE_ACTION_STARTED
        )

        loaded = await self._repository.load(claim.attempt_id)
        self._validate_claim(claim, loaded)
        if claim.action_status is OutboxActionStatus.STARTED:
            return ExecutorStepResult(status=ExecutorStepStatus.RECOVERY_REQUIRED)

        return await self._execute_accepted(claim, loaded)

    async def recover_once(self, claim: ClaimedAction) -> ExecutorStepResult:
        loaded = await self._repository.load(claim.attempt_id)
        state_action = self._validate_claim(claim, loaded)

        if claim.action_status is OutboxActionStatus.ACCEPTED:
            backend = self._backend_for(claim.action.action_type)
            self._validate_reconciliation_capability(state_action, backend)
            return await self._execute_accepted(claim, loaded, backend=backend)

        started_loaded = await self._confirm_started_claim(claim)
        if started_loaded is None:
            return ExecutorStepResult(status=ExecutorStepStatus.RECOVERY_REQUIRED)
        started_action = self._validate_started(claim, started_loaded)

        if started_action.recovery_policy is RecoveryPolicy.NON_REPLAYABLE:
            outcome_value = self._unknown_outcome(claim.action.action_id)
        else:
            backend = self._backend_for(claim.action.action_type)
            self._validate_reconciliation_capability(started_action, backend)
            context = self._execution_context(claim, started_loaded)
            self._fault_injector.hit(
                FaultPoint.AFTER_ACTION_STARTED_BEFORE_EXTERNAL_CALL
            )
            if started_action.recovery_policy is RecoveryPolicy.REPLAY_SAFE:
                outcome_value = await backend.execute(claim.action, context)
            else:
                assert isinstance(backend, ReconcilableBackend)
                outcome_value = await backend.reconcile(claim.action, context)
            self._fault_injector.hit(
                FaultPoint.AFTER_EXTERNAL_CALL_BEFORE_OUTCOME_COMMIT
            )
            if started_action.recovery_policy is RecoveryPolicy.RECONCILABLE:
                if outcome_value is None:
                    outcome_value = self._unknown_outcome(claim.action.action_id)
        outcome = self._validated_outcome(outcome_value, started_action)
        return await self._commit_outcome(claim, started_loaded, outcome)

    async def _execute_accepted(
        self,
        claim: ClaimedAction,
        loaded,
        *,
        backend: Backend | None = None,
    ) -> ExecutorStepResult:
        selected_backend = backend or self._backend_for(claim.action.action_type)
        self._validate_reconciliation_capability(
            self._state_action(loaded, claim.action.action_id),
            selected_backend,
        )

        delivery_claim = claim.delivery_claim()
        if self._clock.now_utc() >= delivery_claim.lease_expires_at:
            return ExecutorStepResult(status=ExecutorStepStatus.RECOVERY_REQUIRED)
        try:
            started_result = await self._engine.handle(
                ReportActionStarted(
                    schema_version=1,
                    command_type="REPORT_ACTION_STARTED",
                    command_id=action_command_id(claim.action.action_id, "started"),
                    attempt_id=claim.attempt_id,
                    expected_revision=loaded.state.revision,
                    action_id=claim.action.action_id,
                ),
                delivery_claim=delivery_claim,
            )
        except RepositoryError:
            return ExecutorStepResult(status=ExecutorStepStatus.RECOVERY_REQUIRED)
        if not started_result.accepted:
            return ExecutorStepResult(status=ExecutorStepStatus.RECOVERY_REQUIRED)

        started_loaded = await self._confirm_started_claim(claim)
        if started_loaded is None:
            return ExecutorStepResult(status=ExecutorStepStatus.RECOVERY_REQUIRED)
        started_action = self._validate_started(claim, started_loaded)
        context = self._execution_context(claim, started_loaded)
        self._fault_injector.hit(
            FaultPoint.AFTER_ACTION_STARTED_BEFORE_EXTERNAL_CALL
        )
        outcome_value = await selected_backend.execute(claim.action, context)
        self._fault_injector.hit(
            FaultPoint.AFTER_EXTERNAL_CALL_BEFORE_OUTCOME_COMMIT
        )
        outcome = self._validated_outcome(
            outcome_value,
            started_action,
        )
        return await self._commit_outcome(claim, started_loaded, outcome)

    async def _commit_outcome(
        self,
        claim: ClaimedAction,
        started_loaded,
        outcome: ActionOutcome,
    ) -> ExecutorStepResult:
        outcome_command_id = action_command_id(claim.action.action_id, "outcome")
        outcome_revision = started_loaded.state.revision
        for attempt in range(_OUTCOME_COMMIT_MAX_ATTEMPTS):
            try:
                outcome_result = await self._engine.handle(
                    ReportActionOutcome(
                        schema_version=1,
                        command_type="REPORT_ACTION_OUTCOME",
                        command_id=outcome_command_id,
                        attempt_id=claim.attempt_id,
                        expected_revision=outcome_revision,
                        action_id=claim.action.action_id,
                        outcome=outcome,
                    )
                )
            except RepositoryError:
                return ExecutorStepResult(
                    status=ExecutorStepStatus.RECOVERY_REQUIRED
                )
            if outcome_result.accepted:
                self._fault_injector.hit(
                    FaultPoint.AFTER_OUTCOME_COMMIT_BEFORE_RESPONSE_DELIVERY
                )
                break
            if (
                outcome_result.error is None
                or outcome_result.error.code != "REVISION_CONFLICT"
                or attempt + 1 == _OUTCOME_COMMIT_MAX_ATTEMPTS
            ):
                return ExecutorStepResult(
                    status=ExecutorStepStatus.RECOVERY_REQUIRED
                )
            try:
                latest = await self._repository.load(claim.attempt_id)
            except RepositoryError:
                return ExecutorStepResult(
                    status=ExecutorStepStatus.RECOVERY_REQUIRED
                )
            self._validate_started(claim, latest)
            outcome_revision = latest.state.revision

        try:
            completed = await self._repository.load(claim.attempt_id)
        except RepositoryError:
            return ExecutorStepResult(status=ExecutorStepStatus.RECOVERY_REQUIRED)
        completed_action = self._state_action(completed, claim.action.action_id)
        if completed_action.status is not outcome.status:
            raise ExecutorInvariantError()
        return ExecutorStepResult(status=ExecutorStepStatus.COMPLETED)

    def _backend_for(self, action_type: ActionType) -> Backend:
        try:
            return self._backends[action_type]
        except KeyError:
            raise ExecutorConfigurationError() from None

    @staticmethod
    def _validate_reconciliation_capability(
        action: ActionState,
        backend: Backend,
    ) -> None:
        if (
            action.recovery_policy is RecoveryPolicy.RECONCILABLE
            and not isinstance(backend, ReconcilableBackend)
        ):
            raise ExecutorConfigurationError()

    async def _confirm_started_claim(self, claim: ClaimedAction):
        try:
            current = await self._repository.confirm_action_claim(
                claim=claim.delivery_claim(),
                action_status=OutboxActionStatus.STARTED,
                now_utc=self._clock.now_utc(),
            )
            if not current:
                return None
            return await self._repository.load(claim.attempt_id)
        except RepositoryError:
            return None

    @staticmethod
    def _execution_context(claim: ClaimedAction, loaded) -> ExecutionContext:
        return ExecutionContext(
            attempt_id=claim.attempt_id,
            action_id=claim.action.action_id,
            invocation_id=claim.action.invocation_id,
            idempotency_key=claim.action.idempotency_key,
            deadline_at=loaded.state.budget.deadline_at,
            cancellation_requested=False,
        )

    @staticmethod
    def _validated_outcome(value: object, action: ActionState) -> ActionOutcome:
        outcome = _ACTION_OUTCOME_ADAPTER.validate_python(value)
        if outcome.action_id != action.action_id:
            raise ExecutorInvariantError()
        return outcome

    @staticmethod
    def _unknown_outcome(action_id: str) -> ActionUnknownOutcome:
        return ActionUnknownOutcome(
            status=ActionStatus.OUTCOME_UNKNOWN,
            action_id=action_id,
            error=ErrorSummary(
                code="RECOVERY_OUTCOME_UNKNOWN",
                retryable=False,
                safe_message=(
                    "The Action outcome could not be determined after recovery."
                ),
            ),
        )

    @classmethod
    def _validate_claim(cls, claim: ClaimedAction, loaded) -> ActionState:
        state_action = cls._state_action(loaded, claim.action.action_id)
        if not cls._same_action(state_action, claim.action):
            raise ExecutorInvariantError()
        expected_status = ActionStatus(claim.action_status.value)
        if state_action.status is not expected_status:
            raise ExecutorInvariantError()
        if (
            claim.action_status is OutboxActionStatus.ACCEPTED
            and loaded.state.phase is not AttemptPhase.RUNNING
        ):
            raise ExecutorInvariantError()
        return state_action

    @classmethod
    def _validate_started(cls, claim: ClaimedAction, loaded) -> ActionState:
        state_action = cls._state_action(loaded, claim.action.action_id)
        if (
            state_action.status is not ActionStatus.STARTED
            or not cls._same_action(state_action, claim.action)
        ):
            raise ExecutorInvariantError()
        return state_action

    @staticmethod
    def _state_action(loaded, action_id: str) -> ActionState:
        if loaded is None:
            raise ExecutorInvariantError()
        matching = tuple(
            action for action in loaded.state.actions if action.action_id == action_id
        )
        if len(matching) != 1:
            raise ExecutorInvariantError()
        return matching[0]

    @staticmethod
    def _same_action(state_action: ActionState, action: NormalizedAction) -> bool:
        return (
            state_action.action_id == action.action_id
            and state_action.action_type == action.action_type.value
            and state_action.actor_id == action.actor
            and state_action.target_ids == action.target_ids
            and state_action.causal_parent_id == action.causal_parent_id
            and state_action.invocation_id == action.invocation_id
            and state_action.payload_ref == action.payload_ref
            and state_action.idempotency_key == action.idempotency_key
            and state_action.recovery_policy is action.recovery_policy
            and state_action.reservation_id == action.reservation_id
            and state_action.retry_of_action_id == action.retry_of_action_id
            and state_action.requested_timeout == action.requested_timeout
            and state_action.batch_id == action.batch_id
            and state_action.call_depth == action.call_depth
            and state_action.resource_requests == action.resource_requests
        )


__all__ = [
    "ActionExecutor",
    "ExecutorConfigurationError",
    "ExecutorInvariantError",
    "ExecutorStepResult",
    "ExecutorStepStatus",
]
