from __future__ import annotations

import math
from collections.abc import Mapping as MappingABC
from enum import StrEnum
from types import MappingProxyType
from typing import Any, Mapping

from pydantic import (
    ConfigDict,
    Field,
    StrictInt,
    StrictStr,
    ValidationError,
    field_validator,
)

from .commands import (
    ApplyStrategyDecision,
    FinishAttempt,
    StartAttempt,
    transition_command_id,
)
from .contract import (
    FaultInjector,
    FaultPoint,
    NoOpFaultInjector,
    Strategy,
    StrategyDecision,
    StrategyDirective,
)
from .engine import AttemptEngine
from .events import (
    AttemptStarted,
    StrategyDecisionRecorded,
    effective_strategy_triggers,
)
from .executor import ActionExecutor, ExecutorStepStatus
from .state import (
    ActionStatus,
    AttemptPhase,
    FrozenModel,
    TERMINAL_ACTION_STATUSES,
    to_strategy_view,
)
from .store import AttemptRepository, LoadedAttempt


_START_OPERATION = "start-attempt"
_DECISION_OPERATION = "apply-strategy-decision"
_FINISH_OPERATION = "finish-attempt"
_TERMINAL_PHASES = frozenset(
    {
        AttemptPhase.SUCCEEDED,
        AttemptPhase.FAILED,
        AttemptPhase.CANCELLED,
        AttemptPhase.TIMED_OUT,
        AttemptPhase.INTERRUPTED,
    }
)
_CONTROL_PHASES = frozenset(
    {
        AttemptPhase.PAUSE_REQUESTED,
        AttemptPhase.PAUSED,
        AttemptPhase.WAITING_EXTERNAL,
        AttemptPhase.CANCEL_REQUESTED,
    }
)


def _freeze_topology(value: Any) -> Any:
    if value is None or type(value) in {bool, int, str}:
        return value
    if type(value) is float:
        if not math.isfinite(value):
            raise ValueError("legal_topology floats must be finite")
        return value
    if isinstance(value, MappingABC):
        frozen: dict[str, Any] = {}
        for key, item in value.items():
            if type(key) is not str:
                raise ValueError("legal_topology object keys must be strings")
            frozen[key] = _freeze_topology(item)
        return MappingProxyType(frozen)
    if isinstance(value, (list, tuple)):
        return tuple(_freeze_topology(item) for item in value)
    raise ValueError("legal_topology must contain only standard JSON values")


def _thaw_topology(value: Any) -> Any:
    if isinstance(value, MappingABC):
        return {key: _thaw_topology(item) for key, item in value.items()}
    if isinstance(value, tuple):
        return [_thaw_topology(item) for item in value]
    return value


class RunnerStatus(StrEnum):
    BLOCKED = "BLOCKED"
    QUIESCENT = "QUIESCENT"
    TERMINAL = "TERMINAL"


class RunnerResult(FrozenModel):
    model_config = ConfigDict(extra="forbid", frozen=True, strict=True)

    status: RunnerStatus
    attempt_id: StrictStr
    phase: AttemptPhase
    revision: StrictInt = Field(ge=1)
    steps: StrictInt = Field(ge=0)

    @field_validator("attempt_id")
    @classmethod
    def validate_attempt_id(cls, value: str) -> str:
        if not value:
            raise ValueError("attempt_id must not be empty")
        return value


class RunnerError(RuntimeError):
    code: str
    safe_message: str

    def __init__(self) -> None:
        super().__init__(self.safe_message)


class RunnerInvariantError(RunnerError):
    code = "runner_invariant_error"
    safe_message = "The deterministic runner encountered an invalid orchestration state."


class MissingStrategyError(RunnerInvariantError):
    code = "strategy_not_registered"
    safe_message = "The Attempt Strategy is not registered with the runner."


class StepLimitExceeded(RunnerError):
    code = "step_limit_exceeded"
    safe_message = "The deterministic runner step limit was exceeded."


class DeterministicAttemptRunner:
    def __init__(
        self,
        *,
        repository: AttemptRepository,
        engine: AttemptEngine,
        executor: ActionExecutor,
        strategies: Mapping[str, Strategy],
        legal_topology: Any,
        worker_id: str,
        fault_injector: FaultInjector | None = None,
    ) -> None:
        if not isinstance(repository, AttemptRepository):
            raise ValueError("repository must implement AttemptRepository")
        if not isinstance(engine, AttemptEngine):
            raise ValueError("engine must be an AttemptEngine")
        if not isinstance(executor, ActionExecutor):
            raise ValueError("executor must be an ActionExecutor")
        if type(worker_id) is not str or not worker_id:
            raise ValueError("worker_id must be a nonempty string")
        if fault_injector is not None and not isinstance(
            fault_injector, FaultInjector
        ):
            raise ValueError("fault_injector must implement FaultInjector")
        if fault_injector is not None and not callable(
            getattr(fault_injector, "hit", None)
        ):
            raise ValueError("fault_injector.hit must be callable")
        copied_registry = dict(strategies)
        if any(
            type(strategy_id) is not str
            or not strategy_id
            or not isinstance(strategy, Strategy)
            for strategy_id, strategy in copied_registry.items()
        ):
            raise ValueError("strategies must map nonempty ids to Strategy objects")
        self._repository = repository
        self._engine = engine
        self._executor = executor
        self._strategies = MappingProxyType(copied_registry)
        self._legal_topology = _freeze_topology(legal_topology)
        self._worker_id = worker_id
        self._fault_injector = (
            fault_injector
            if fault_injector is not None
            else NoOpFaultInjector()
        )

    async def run_until_blocked(
        self,
        attempt_id: str,
        max_steps: int,
    ) -> RunnerResult:
        if type(attempt_id) is not str or not attempt_id:
            raise ValueError("attempt_id must be a nonempty string")
        if type(max_steps) is not int or max_steps < 0:
            raise ValueError("max_steps must be a nonnegative integer")

        steps = 0
        while True:
            loaded = await self._load_required(attempt_id)
            state = loaded.state

            if state.phase in _TERMINAL_PHASES:
                return self._result(RunnerStatus.TERMINAL, loaded, steps)
            if state.phase in _CONTROL_PHASES:
                return self._result(RunnerStatus.BLOCKED, loaded, steps)

            if state.phase is AttemptPhase.PLANNED:
                if state.strategy_id not in self._strategies:
                    raise MissingStrategyError()
                self._require_step_capacity(steps, max_steps)
                steps += 1
                result = await self._engine.handle(
                    StartAttempt(
                        schema_version=1,
                        command_type="START_ATTEMPT",
                        command_id=transition_command_id(
                            attempt_id,
                            state.revision,
                            _START_OPERATION,
                        ),
                        attempt_id=attempt_id,
                        expected_revision=state.revision,
                    )
                )
                self._validate_command_result(result)
                continue

            if state.phase is not AttemptPhase.RUNNING:
                raise RunnerInvariantError()

            latest_decision = self._latest_decision(loaded)
            if (
                latest_decision is not None
                and latest_decision.directive is not StrategyDirective.CONTINUE
            ):
                if any(
                    action.status in {ActionStatus.ACCEPTED, ActionStatus.STARTED}
                    for action in state.actions
                ):
                    self._require_step_capacity(steps, max_steps)
                    steps += 1
                    executor_result = await self._executor.run_once(self._worker_id)
                    if executor_result.status is ExecutorStepStatus.COMPLETED:
                        continue
                    reloaded = await self._load_required(attempt_id)
                    if reloaded.state.revision != state.revision:
                        continue
                    if reloaded.state.phase in _TERMINAL_PHASES:
                        return self._result(
                            RunnerStatus.TERMINAL, reloaded, steps
                        )
                    if reloaded.state.phase in _CONTROL_PHASES:
                        return self._result(
                            RunnerStatus.BLOCKED, reloaded, steps
                        )
                    return self._result(
                        RunnerStatus.QUIESCENT, reloaded, steps
                    )
                if any(
                    action.status is ActionStatus.OUTCOME_UNKNOWN
                    and action.reconciled_status is None
                    for action in state.actions
                ):
                    return self._result(RunnerStatus.QUIESCENT, loaded, steps)
                if all(
                    action.status in TERMINAL_ACTION_STATUSES
                    for action in state.actions
                ):
                    self._require_step_capacity(steps, max_steps)
                    steps += 1
                    result = await self._engine.handle(
                        FinishAttempt(
                            schema_version=1,
                            command_type="FINISH_ATTEMPT",
                            command_id=transition_command_id(
                                attempt_id,
                                latest_decision.sequence_no,
                                _FINISH_OPERATION,
                            ),
                            attempt_id=attempt_id,
                            expected_revision=state.revision,
                            result_ref=latest_decision.result_ref,
                            error=latest_decision.error,
                        )
                    )
                    self._validate_command_result(result)
                    continue
                raise RunnerInvariantError()

            cursor = max(
                (
                    event.trigger_sequence_no
                    for event in loaded.events
                    if isinstance(event, StrategyDecisionRecorded)
                ),
                default=0,
            )
            trigger = next(
                (
                    event
                    for event in effective_strategy_triggers(loaded.events)
                    if event.sequence_no > cursor
                ),
                None,
            )
            if trigger is not None:
                strategy = self._strategies.get(state.strategy_id)
                if strategy is None:
                    raise MissingStrategyError()
                self._require_step_capacity(steps, max_steps)
                view = to_strategy_view(
                    state,
                    legal_topology=_thaw_topology(self._legal_topology),
                    visible_artifacts=tuple(
                        registration.ref
                        for registration in loaded.artifact_registrations
                    ),
                    latest_committed_event=trigger.model_dump(mode="json"),
                )
                if latest_decision is None and isinstance(trigger, AttemptStarted):
                    raw_decision = strategy.initialize(view)
                else:
                    raw_decision = strategy.on_event(state.strategy, trigger, view)
                try:
                    decision = StrategyDecision.model_validate(raw_decision)
                except (TypeError, ValidationError):
                    raise RunnerInvariantError() from None
                self._validate_decision(decision, loaded, trigger.sequence_no)
                steps += 1
                result = await self._engine.handle(
                    ApplyStrategyDecision(
                        schema_version=1,
                        command_type="APPLY_STRATEGY_DECISION",
                        command_id=transition_command_id(
                            attempt_id,
                            trigger.sequence_no,
                            _DECISION_OPERATION,
                        ),
                        attempt_id=attempt_id,
                        expected_revision=state.revision,
                        trigger_sequence_no=decision.trigger_sequence_no,
                        strategy=decision.strategy,
                        proposals=decision.proposals,
                        external_requirements=decision.external_requirements,
                        directive=decision.directive,
                        result_ref=decision.result_ref,
                        error=decision.error,
                    )
                )
                self._validate_command_result(result)
                if result.accepted and decision.proposals:
                    committed = await self._load_required(attempt_id)
                    proposed_ids = {
                        proposal.action_id for proposal in decision.proposals
                    }
                    if any(
                        action.action_id in proposed_ids
                        and action.status is ActionStatus.ACCEPTED
                        for action in committed.state.actions
                    ):
                        self._fault_injector.hit(
                            FaultPoint.AFTER_COMMIT_BEFORE_CLAIM
                        )
                continue

            if any(
                action.status in {ActionStatus.ACCEPTED, ActionStatus.STARTED}
                for action in state.actions
            ):
                self._require_step_capacity(steps, max_steps)
                steps += 1
                executor_result = await self._executor.run_once(self._worker_id)
                if executor_result.status is ExecutorStepStatus.COMPLETED:
                    continue
                reloaded = await self._load_required(attempt_id)
                if reloaded.state.revision != state.revision:
                    continue
                if reloaded.state.phase in _TERMINAL_PHASES:
                    return self._result(RunnerStatus.TERMINAL, reloaded, steps)
                if reloaded.state.phase in _CONTROL_PHASES:
                    return self._result(RunnerStatus.BLOCKED, reloaded, steps)
                return self._result(RunnerStatus.QUIESCENT, reloaded, steps)
            if any(
                action.status is ActionStatus.OUTCOME_UNKNOWN
                and action.reconciled_status is None
                for action in state.actions
            ):
                return self._result(RunnerStatus.QUIESCENT, loaded, steps)

            if any(
                action.status not in TERMINAL_ACTION_STATUSES
                for action in state.actions
            ):
                raise RunnerInvariantError()
            return self._result(RunnerStatus.QUIESCENT, loaded, steps)

    async def _load_required(self, attempt_id: str) -> LoadedAttempt:
        loaded = await self._repository.load(attempt_id)
        if loaded is None:
            raise RunnerInvariantError()
        return loaded

    @staticmethod
    def _latest_decision(
        loaded: LoadedAttempt,
    ) -> StrategyDecisionRecorded | None:
        return next(
            (
                event
                for event in reversed(loaded.events)
                if isinstance(event, StrategyDecisionRecorded)
            ),
            None,
        )

    @staticmethod
    def _validate_decision(
        decision: StrategyDecision,
        loaded: LoadedAttempt,
        trigger_sequence_no: int,
    ) -> None:
        if (
            decision.trigger_sequence_no != trigger_sequence_no
            or decision.strategy.strategy_id != loaded.state.strategy_id
            or any(
                proposal.causal_parent_id
                != str(loaded.events[trigger_sequence_no - 1].event_id)
                for proposal in decision.proposals
            )
        ):
            raise RunnerInvariantError()

    @staticmethod
    def _validate_command_result(result: object) -> None:
        accepted = getattr(result, "accepted", None)
        if accepted is True:
            return
        error = getattr(result, "error", None)
        if getattr(error, "code", None) == "REVISION_CONFLICT":
            return
        raise RunnerInvariantError()

    @staticmethod
    def _require_step_capacity(steps: int, max_steps: int) -> None:
        if steps >= max_steps:
            raise StepLimitExceeded()

    @staticmethod
    def _result(
        status: RunnerStatus,
        loaded: LoadedAttempt,
        steps: int,
    ) -> RunnerResult:
        return RunnerResult(
            status=status,
            attempt_id=loaded.state.attempt_id,
            phase=loaded.state.phase,
            revision=loaded.state.revision,
            steps=steps,
        )


__all__ = [
    "DeterministicAttemptRunner",
    "MissingStrategyError",
    "RunnerError",
    "RunnerInvariantError",
    "RunnerResult",
    "RunnerStatus",
    "StepLimitExceeded",
]
