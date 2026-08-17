from __future__ import annotations

from datetime import datetime, timedelta, timezone
from enum import StrEnum
from typing import TYPE_CHECKING, Protocol, runtime_checkable
from uuid import UUID, uuid4

from pydantic import (
    ConfigDict,
    Field,
    StrictBool,
    StrictInt,
    StrictStr,
    field_validator,
    model_validator,
)

from .actions import (
    ActionOutcome,
    ActionProposal,
    ExternalInputRequirement,
    NormalizedAction,
    StableId,
    validate_strategy_external_requirements,
)
from .state import (
    ArtifactRef,
    AttemptPhase,
    ErrorSummary,
    FrozenModel,
    StrategyStateEnvelope,
    StrategyView,
)

if TYPE_CHECKING:
    from .events import DomainEvent


class StrategyDirective(StrEnum):
    CONTINUE = "CONTINUE"
    SUCCEED = "SUCCEED"
    FAIL = "FAIL"


class FaultPoint(StrEnum):
    BEFORE_TRANSACTION_COMMIT = "BEFORE_TRANSACTION_COMMIT"
    AFTER_COMMIT_BEFORE_CLAIM = "AFTER_COMMIT_BEFORE_CLAIM"
    AFTER_CLAIM_BEFORE_ACTION_STARTED = "AFTER_CLAIM_BEFORE_ACTION_STARTED"
    AFTER_ACTION_STARTED_BEFORE_EXTERNAL_CALL = (
        "AFTER_ACTION_STARTED_BEFORE_EXTERNAL_CALL"
    )
    AFTER_EXTERNAL_CALL_BEFORE_OUTCOME_COMMIT = (
        "AFTER_EXTERNAL_CALL_BEFORE_OUTCOME_COMMIT"
    )
    AFTER_OUTCOME_COMMIT_BEFORE_RESPONSE_DELIVERY = (
        "AFTER_OUTCOME_COMMIT_BEFORE_RESPONSE_DELIVERY"
    )


@runtime_checkable
class FaultInjector(Protocol):
    def hit(self, point: FaultPoint) -> None:
        raise NotImplementedError


class NoOpFaultInjector:
    def hit(self, point: FaultPoint) -> None:
        del point


class StrategyDecision(FrozenModel):
    model_config = ConfigDict(extra="forbid", frozen=True, strict=True)

    trigger_sequence_no: StrictInt = Field(ge=1)
    strategy: StrategyStateEnvelope
    proposals: tuple[ActionProposal, ...]
    external_requirements: tuple[ExternalInputRequirement, ...] = Field(
        default=(),
        exclude_if=lambda value: not value,
    )
    directive: StrategyDirective
    result_ref: ArtifactRef | None = None
    error: ErrorSummary | None = None

    @model_validator(mode="after")
    def validate_directive_shape(self) -> StrategyDecision:
        validate_strategy_external_requirements(
            proposals=self.proposals,
            directive=self.directive,
            external_requirements=self.external_requirements,
        )
        if self.directive is StrategyDirective.CONTINUE:
            valid = self.result_ref is None and self.error is None
        elif self.directive is StrategyDirective.SUCCEED:
            valid = (
                not self.proposals
                and self.result_ref is not None
                and self.error is None
            )
        else:
            valid = (
                not self.proposals
                and self.result_ref is None
                and self.error is not None
            )
        if not valid:
            raise ValueError("Strategy directive fields are inconsistent")
        return self


class ExecutionContext(FrozenModel):
    model_config = ConfigDict(extra="forbid", frozen=True, strict=True)

    attempt_id: StableId
    action_id: StableId
    invocation_id: StableId | None
    idempotency_key: StableId
    deadline_at: datetime | None
    cancellation_requested: StrictBool

    @field_validator("deadline_at", mode="before")
    @classmethod
    def validate_deadline(cls, value: object) -> datetime | None:
        if value is None:
            return None
        if type(value) is not datetime:
            raise ValueError("deadline_at must be a datetime or None")
        if value.tzinfo is None or value.utcoffset() != timedelta(0):
            raise ValueError("deadline_at must be timezone-aware UTC")
        return value


@runtime_checkable
class Strategy(Protocol):
    def initialize(self, view: StrategyView) -> StrategyDecision:
        raise NotImplementedError

    def on_event(
        self,
        state: StrategyStateEnvelope,
        event: DomainEvent,
        view: StrategyView,
    ) -> StrategyDecision:
        raise NotImplementedError


@runtime_checkable
class Backend(Protocol):
    async def execute(
        self,
        action: NormalizedAction,
        context: ExecutionContext,
    ) -> ActionOutcome:
        raise NotImplementedError


@runtime_checkable
class ReconcilableBackend(Protocol):
    async def reconcile(
        self,
        action: NormalizedAction,
        context: ExecutionContext,
    ) -> ActionOutcome | None:
        raise NotImplementedError


class ArtifactVerificationError(ValueError):
    code = "artifact_verification_failed"
    safe_message = "A referenced artifact could not be verified."

    def __init__(self) -> None:
        super().__init__(self.safe_message)


@runtime_checkable
class Clock(Protocol):
    def now_utc(self) -> datetime:
        raise NotImplementedError


@runtime_checkable
class IdFactory(Protocol):
    def new_uuid(self) -> UUID:
        raise NotImplementedError


@runtime_checkable
class ArtifactVerifier(Protocol):
    def verify(self, ref: ArtifactRef) -> None:
        raise NotImplementedError


class SystemClock:
    def now_utc(self) -> datetime:
        return datetime.now(timezone.utc)


class Uuid4IdFactory:
    def new_uuid(self) -> UUID:
        return uuid4()


class CommandResult(FrozenModel):
    command_id: UUID
    attempt_id: StrictStr
    accepted: StrictBool
    revision: StrictInt = Field(ge=0)
    phase: AttemptPhase | None
    error: ErrorSummary | None = None

    @field_validator("attempt_id")
    @classmethod
    def validate_attempt_id(cls, value: str) -> str:
        if not value:
            raise ValueError("attempt_id must not be empty")
        return value

    @model_validator(mode="after")
    def validate_result_shape(self) -> CommandResult:
        if self.accepted:
            if self.revision < 1 or self.phase is None or self.error is not None:
                raise ValueError(
                    "accepted Command result requires a committed revision and phase "
                    "without an error"
                )
        elif self.error is None:
            raise ValueError("rejected Command result requires an error")
        return self


__all__ = [
    "ArtifactVerificationError",
    "ArtifactVerifier",
    "Backend",
    "Clock",
    "CommandResult",
    "ExecutionContext",
    "FaultInjector",
    "FaultPoint",
    "IdFactory",
    "NoOpFaultInjector",
    "ReconcilableBackend",
    "SystemClock",
    "Strategy",
    "StrategyDecision",
    "StrategyDirective",
    "Uuid4IdFactory",
]
