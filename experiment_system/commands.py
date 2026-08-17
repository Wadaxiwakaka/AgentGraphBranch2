from __future__ import annotations

import json
from datetime import datetime, timedelta
from hashlib import sha256
from typing import Annotated, Any, Literal
from uuid import NAMESPACE_URL, UUID, uuid5

from pydantic import Field, StrictInt, StrictStr, field_validator, model_validator

from .actions import (
    ActionOutcome,
    ActionProposal,
    ExternalInputRequirement,
    StableId,
    validate_stable_id,
    validate_strategy_external_requirements,
)
from .contract import StrategyDirective
from .state import (
    ArtifactRef,
    BudgetState,
    ErrorSummary,
    ExternalResponseKind,
    FrozenModel,
    StrategyStateEnvelope,
)


_ACTION_COMMAND_NAMESPACE = uuid5(
    NAMESPACE_URL,
    "agentgraph:orchestration:action-command:v1",
)
_STRATEGY_ACTION_NAMESPACE = uuid5(
    NAMESPACE_URL,
    "agentgraph:orchestration:strategy-action:v1",
)
_STRATEGY_INVOCATION_NAMESPACE = uuid5(
    NAMESPACE_URL,
    "agentgraph:orchestration:strategy-invocation:v1",
)
_TRANSITION_COMMAND_NAMESPACE = uuid5(
    NAMESPACE_URL,
    "agentgraph:orchestration:transition-command:v1",
)
_OUTCOME_RECONCILIATION_REQUEST_NAMESPACE = uuid5(
    NAMESPACE_URL,
    "agentgraph:orchestration:outcome-reconciliation-request:v1",
)


def _validate_non_empty(value: object, *, field_name: str) -> str:
    if type(value) is not str:
        raise TypeError(f"{field_name} must be a string")
    if not value:
        raise ValueError(f"{field_name} must not be empty")
    return value


def _validate_utc(value: datetime) -> datetime:
    if value.tzinfo is None or value.utcoffset() != timedelta(0):
        raise ValueError("timestamp must be timezone-aware and use the UTC offset")
    return value


class CommandBase(FrozenModel):
    schema_version: Literal[1]
    command_type: StrictStr
    command_id: UUID
    attempt_id: StrictStr
    expected_revision: StrictInt = Field(ge=1)

    @field_validator("schema_version", mode="before")
    @classmethod
    def validate_schema_version(cls, value: object) -> object:
        if type(value) is not int:
            raise ValueError("schema_version must be the integer one")
        return value

    @field_validator("command_type", "attempt_id")
    @classmethod
    def validate_required_text(cls, value: str, info: Any) -> str:
        return _validate_non_empty(value, field_name=info.field_name)


class CreateAttempt(CommandBase):
    command_type: Literal["CREATE_ATTEMPT"]
    expected_revision: Literal[0]
    state_schema_version: StrictInt = Field(ge=1)
    experiment_id: StrictStr
    trial_id: StrictStr
    strategy_id: StrictStr
    manifest_ref: ArtifactRef
    strategy: StrategyStateEnvelope
    budget: BudgetState

    @field_validator("expected_revision", mode="before")
    @classmethod
    def validate_create_revision(cls, value: object) -> object:
        if type(value) is not int:
            raise ValueError("expected_revision must be the integer zero")
        return value

    @field_validator("experiment_id", "trial_id", "strategy_id")
    @classmethod
    def validate_domain_id(cls, value: str, info: Any) -> str:
        return _validate_non_empty(value, field_name=info.field_name)

    @model_validator(mode="after")
    def validate_strategy_identity(self) -> CreateAttempt:
        if self.strategy.strategy_id != self.strategy_id:
            raise ValueError("strategy_id must match the Strategy state envelope")
        return self


class StartAttempt(CommandBase):
    command_type: Literal["START_ATTEMPT"]


class ApplyStrategyDecision(CommandBase):
    command_type: Literal["APPLY_STRATEGY_DECISION"]
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
    def validate_directive_shape(self) -> ApplyStrategyDecision:
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


class ReportActionStarted(CommandBase):
    command_type: Literal["REPORT_ACTION_STARTED"]
    action_id: StableId


class ReportActionOutcome(CommandBase):
    command_type: Literal["REPORT_ACTION_OUTCOME"]
    action_id: StableId
    outcome: ActionOutcome

    @model_validator(mode="after")
    def validate_action_identity(self) -> ReportActionOutcome:
        if self.outcome.action_id != self.action_id:
            raise ValueError("action_id must match outcome.action_id")
        return self


class FinishAttempt(CommandBase):
    command_type: Literal["FINISH_ATTEMPT"]
    result_ref: ArtifactRef | None = None
    error: ErrorSummary | None = None

    @model_validator(mode="after")
    def validate_terminal_value(self) -> FinishAttempt:
        if (self.result_ref is None) == (self.error is None):
            raise ValueError(
                "FinishAttempt requires exactly one of result_ref or error"
            )
        return self


class PauseAttempt(CommandBase):
    command_type: Literal["PAUSE_ATTEMPT"]


class ResumeAttempt(CommandBase):
    command_type: Literal["RESUME_ATTEMPT"]


class CancelAttempt(CommandBase):
    command_type: Literal["CANCEL_ATTEMPT"]


class SubmitExternalInput(CommandBase):
    command_type: Literal["SUBMIT_EXTERNAL_INPUT"]
    request_id: StableId
    response_kind: ExternalResponseKind
    response_ref: ArtifactRef | None = None
    error: ErrorSummary | None = None

    @model_validator(mode="after")
    def validate_response_shape(self) -> SubmitExternalInput:
        no_details = self.response_ref is None and self.error is None
        if self.response_kind in {
            ExternalResponseKind.APPROVE,
            ExternalResponseKind.REJECT,
            ExternalResponseKind.ABANDON,
        }:
            valid = no_details
        elif self.response_kind in {
            ExternalResponseKind.PROVIDE_INPUT,
            ExternalResponseKind.CONFIRM_SUCCEEDED,
        }:
            valid = self.response_ref is not None and self.error is None
        else:
            valid = self.response_ref is None and self.error is not None
        if not valid:
            raise ValueError("external response fields are inconsistent")
        return self


class ExpireAttempt(CommandBase):
    command_type: Literal["EXPIRE_ATTEMPT"]
    deadline_at: datetime

    @field_validator("deadline_at")
    @classmethod
    def validate_deadline_at(cls, value: datetime) -> datetime:
        return _validate_utc(value)


class RecoverAttempt(CommandBase):
    command_type: Literal["RECOVER_ATTEMPT"]


Command = Annotated[
    CreateAttempt
    | StartAttempt
    | ApplyStrategyDecision
    | ReportActionStarted
    | ReportActionOutcome
    | FinishAttempt
    | PauseAttempt
    | ResumeAttempt
    | CancelAttempt
    | SubmitExternalInput
    | ExpireAttempt
    | RecoverAttempt,
    Field(discriminator="command_type"),
]


def command_request_hash(command: CommandBase) -> str:
    payload = command.model_dump(mode="json", exclude_none=True)
    canonical = json.dumps(
        payload,
        ensure_ascii=False,
        allow_nan=False,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")
    return sha256(canonical).hexdigest()


def _uuid5_name(*components: object) -> str:
    return json.dumps(
        components,
        ensure_ascii=False,
        allow_nan=False,
        separators=(",", ":"),
    )


def action_command_id(action_id: str, phase: str) -> UUID:
    _validate_non_empty(action_id, field_name="action_id")
    _validate_non_empty(phase, field_name="phase")
    return uuid5(_ACTION_COMMAND_NAMESPACE, _uuid5_name(action_id, phase))


def outcome_reconciliation_request_id(attempt_id: str, action_id: str) -> str:
    _validate_non_empty(attempt_id, field_name="attempt_id")
    validate_stable_id(action_id, field_name="action_id")
    name = f"{len(attempt_id)}:{attempt_id}{len(action_id)}:{action_id}"
    return str(uuid5(_OUTCOME_RECONCILIATION_REQUEST_NAMESPACE, name))


def strategy_action_id(attempt_id: str, strategy_id: str, ordinal: int) -> str:
    _validate_non_empty(attempt_id, field_name="attempt_id")
    _validate_non_empty(strategy_id, field_name="strategy_id")
    if type(ordinal) is not int:
        raise TypeError("ordinal must be an integer")
    if ordinal < 0:
        raise ValueError("ordinal must be nonnegative")
    return str(
        uuid5(
            _STRATEGY_ACTION_NAMESPACE,
            _uuid5_name(attempt_id, strategy_id, ordinal),
        )
    )


def strategy_invocation_id(attempt_id: str, strategy_id: str, ordinal: int) -> str:
    _validate_non_empty(attempt_id, field_name="attempt_id")
    _validate_non_empty(strategy_id, field_name="strategy_id")
    if type(ordinal) is not int:
        raise TypeError("ordinal must be an integer")
    if ordinal < 0:
        raise ValueError("ordinal must be nonnegative")
    return str(
        uuid5(
            _STRATEGY_INVOCATION_NAMESPACE,
            _uuid5_name(attempt_id, strategy_id, ordinal),
        )
    )


def transition_command_id(
    attempt_id: str,
    trigger_revision: int,
    operation: str,
) -> UUID:
    _validate_non_empty(attempt_id, field_name="attempt_id")
    _validate_non_empty(operation, field_name="operation")
    if type(trigger_revision) is not int:
        raise TypeError("trigger_revision must be an integer")
    if trigger_revision < 1:
        raise ValueError("trigger_revision must be positive")
    return uuid5(
        _TRANSITION_COMMAND_NAMESPACE,
        _uuid5_name(attempt_id, trigger_revision, operation),
    )


__all__ = [
    "ApplyStrategyDecision",
    "CancelAttempt",
    "Command",
    "CommandBase",
    "CreateAttempt",
    "ExpireAttempt",
    "FinishAttempt",
    "PauseAttempt",
    "RecoverAttempt",
    "ReportActionOutcome",
    "ReportActionStarted",
    "ResumeAttempt",
    "StartAttempt",
    "SubmitExternalInput",
    "action_command_id",
    "command_request_hash",
    "outcome_reconciliation_request_id",
    "strategy_action_id",
    "strategy_invocation_id",
    "transition_command_id",
]
