from __future__ import annotations

import math

from pydantic import ConfigDict, field_validator, model_validator

from .commands import RecoverAttempt, transition_command_id
from .contract import Clock
from .engine import AttemptEngine
from .executor import (
    ActionExecutor,
    ExecutorConfigurationError,
    ExecutorStepStatus,
)
from .state import ActionStatus, AttemptPhase, FrozenModel
from .store import AttemptRepository


_RECOVERY_OPERATION = "recover-startup"
_TERMINAL_PHASES = frozenset(
    {
        AttemptPhase.SUCCEEDED,
        AttemptPhase.FAILED,
        AttemptPhase.CANCELLED,
        AttemptPhase.TIMED_OUT,
        AttemptPhase.INTERRUPTED,
    }
)


class RecoveryInvariantError(RuntimeError):
    code = "recovery_invariant_error"
    safe_message = "Startup recovery encountered inconsistent durable state."

    def __init__(self) -> None:
        super().__init__(self.safe_message)


class RecoverySummary(FrozenModel):
    model_config = ConfigDict(extra="forbid", frozen=True, strict=True)

    recovered_attempt_ids: tuple[str, ...] = ()
    waiting_attempt_ids: tuple[str, ...] = ()
    terminal_attempt_ids: tuple[str, ...] = ()
    failed_attempt_ids: tuple[str, ...] = ()

    @field_validator(
        "recovered_attempt_ids",
        "waiting_attempt_ids",
        "terminal_attempt_ids",
        "failed_attempt_ids",
    )
    @classmethod
    def validate_attempt_ids(cls, value: tuple[str, ...]) -> tuple[str, ...]:
        if any(type(attempt_id) is not str or not attempt_id for attempt_id in value):
            raise ValueError("recovery summary ids must be nonempty strings")
        if tuple(sorted(set(value))) != value:
            raise ValueError("recovery summary ids must be unique and sorted")
        return value

    @model_validator(mode="after")
    def validate_disjoint_categories(self) -> RecoverySummary:
        categories = (
            self.recovered_attempt_ids,
            self.waiting_attempt_ids,
            self.terminal_attempt_ids,
            self.failed_attempt_ids,
        )
        combined = tuple(
            attempt_id for category in categories for attempt_id in category
        )
        if len(combined) != len(set(combined)):
            raise ValueError("recovery summary categories must be mutually exclusive")
        return self


class RecoveryCoordinator:
    def __init__(
        self,
        *,
        repository: AttemptRepository,
        engine: AttemptEngine,
        executor: ActionExecutor,
        clock: Clock,
        worker_id: str,
        lease_seconds: float,
    ) -> None:
        if type(worker_id) is not str or not worker_id:
            raise ValueError("worker_id must be a nonempty string")
        if (
            isinstance(lease_seconds, bool)
            or not isinstance(lease_seconds, (int, float))
            or not math.isfinite(lease_seconds)
            or lease_seconds <= 0
        ):
            raise ValueError("lease_seconds must be a positive finite number")
        self._repository = repository
        self._engine = engine
        self._executor = executor
        self._clock = clock
        self._worker_id = worker_id
        self._lease_seconds = float(lease_seconds)

    async def recover_startup(self, max_actions: int) -> RecoverySummary:
        if type(max_actions) is not int or max_actions < 0:
            raise ValueError("max_actions must be a nonnegative integer")

        listed_ids = await self._repository.list_nonterminal_attempt_ids()
        if tuple(sorted(set(listed_ids))) != listed_ids:
            raise RecoveryInvariantError()

        terminal_ids: set[str] = set()
        failed_ids: set[str] = set()
        for attempt_id in listed_ids:
            loaded = await self._repository.load(attempt_id)
            if loaded is None or loaded.state.attempt_id != attempt_id:
                raise RecoveryInvariantError()
            if loaded.state.phase in _TERMINAL_PHASES:
                terminal_ids.add(attempt_id)
                continue
            result = await self._engine.handle(
                RecoverAttempt(
                    schema_version=1,
                    command_type="RECOVER_ATTEMPT",
                    command_id=transition_command_id(
                        attempt_id,
                        loaded.state.revision,
                        _RECOVERY_OPERATION,
                    ),
                    attempt_id=attempt_id,
                    expected_revision=loaded.state.revision,
                )
            )
            if not result.accepted:
                failed_ids.add(attempt_id)
                continue

        successful_ids: set[str] = set()
        for _ in range(max_actions):
            claim = await self._repository.claim_action(
                worker_id=self._worker_id,
                now_utc=self._clock.now_utc(),
                lease_seconds=self._lease_seconds,
            )
            if claim is None:
                break
            try:
                step = await self._executor.recover_once(claim)
            except ExecutorConfigurationError:
                failed_ids.add(claim.attempt_id)
                continue
            if step.status is ExecutorStepStatus.COMPLETED:
                successful_ids.add(claim.attempt_id)
            else:
                failed_ids.add(claim.attempt_id)

        recovered_ids: set[str] = set()
        waiting_ids: set[str] = set()
        for attempt_id in listed_ids:
            loaded = await self._repository.load(attempt_id)
            if loaded is None:
                raise RecoveryInvariantError()
            if loaded.state.phase in _TERMINAL_PHASES:
                terminal_ids.add(attempt_id)
                failed_ids.discard(attempt_id)
            elif attempt_id in failed_ids:
                continue
            elif (
                loaded.state.phase is AttemptPhase.WAITING_EXTERNAL
                or any(
                    action.status
                    in {ActionStatus.ACCEPTED, ActionStatus.STARTED}
                    for action in loaded.state.actions
                )
                or attempt_id not in successful_ids
            ):
                waiting_ids.add(attempt_id)
            else:
                recovered_ids.add(attempt_id)

        return RecoverySummary(
            recovered_attempt_ids=tuple(sorted(recovered_ids)),
            waiting_attempt_ids=tuple(sorted(waiting_ids)),
            terminal_attempt_ids=tuple(sorted(terminal_ids)),
            failed_attempt_ids=tuple(sorted(failed_ids)),
        )


__all__ = [
    "RecoveryCoordinator",
    "RecoveryInvariantError",
    "RecoverySummary",
]
