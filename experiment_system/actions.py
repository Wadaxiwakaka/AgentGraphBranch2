from __future__ import annotations

import re
from enum import StrEnum
from typing import Annotated, Literal

from pydantic import Field, StrictInt, StrictStr, field_validator, model_validator

from .state import (
    ActionStatus,
    ArtifactRef,
    ErrorSummary,
    ExternalRequestKind,
    FrozenModel,
    InvocationStatus,
    RecoveryPolicy,
    ResourceKind,
    ResourceRequest,
)


_STABLE_ID_PATTERN = r"^[A-Za-z0-9](?:[A-Za-z0-9._-]*[A-Za-z0-9])?$"
_STABLE_ID_REGEX = re.compile(_STABLE_ID_PATTERN)
StableId = Annotated[StrictStr, Field(pattern=_STABLE_ID_PATTERN)]


def validate_stable_id(value: object, *, field_name: str) -> str:
    if type(value) is not str or not _STABLE_ID_REGEX.fullmatch(value):
        raise ValueError(f"{field_name} must be a stable ASCII id")
    return value


class ActionType(StrEnum):
    INVOKE_AGENT = "INVOKE_AGENT"
    SEND_MESSAGE = "SEND_MESSAGE"


class ActionProposal(FrozenModel):
    action_id: StableId
    action_type: ActionType
    actor: StableId
    target_ids: tuple[StableId, ...]
    invocation_id: StableId | None
    causal_parent_id: StableId
    payload_ref: ArtifactRef | None
    recovery_policy: RecoveryPolicy
    requested_timeout: StrictInt = Field(gt=0)
    batch_id: StableId | None = None
    retry_of_action_id: StableId | None = None
    call_depth: StrictInt = Field(ge=0)
    resource_requests: tuple[ResourceRequest, ...]

    @field_validator("target_ids")
    @classmethod
    def validate_targets(cls, value: tuple[str, ...]) -> tuple[str, ...]:
        if len(value) != len(set(value)):
            raise ValueError("target ids must be unique")
        if len(value) != 1:
            raise ValueError("Action types in this slice require exactly one target")
        return value

    @field_validator("resource_requests")
    @classmethod
    def validate_resource_requests(
        cls, value: tuple[ResourceRequest, ...]
    ) -> tuple[ResourceRequest, ...]:
        resources = [request.resource for request in value]
        if len(resources) != len(set(resources)):
            raise ValueError("resource requests must use unique resources")
        return value

    @model_validator(mode="after")
    def validate_action_shape(self) -> ActionProposal:
        if self.action_type is ActionType.INVOKE_AGENT and self.invocation_id is None:
            raise ValueError("INVOKE_AGENT requires an invocation_id")
        if self.retry_of_action_id == self.action_id:
            raise ValueError("retry_of_action_id must differ from action_id")
        return self


class ExternalInputRequirement(FrozenModel):
    request_id: StableId
    request_kind: ExternalRequestKind
    action_id: StableId | None = None
    payload_ref: ArtifactRef | None = None

    @model_validator(mode="after")
    def validate_requirement_shape(self) -> ExternalInputRequirement:
        requires_action = self.request_kind in {
            ExternalRequestKind.ACTION_APPROVAL,
            ExternalRequestKind.OUTCOME_RECONCILIATION,
        }
        if requires_action != (self.action_id is not None):
            raise ValueError("external requirement action_id does not match request_kind")
        return self


def validate_strategy_external_requirements(
    *,
    proposals: tuple[ActionProposal, ...],
    directive: object,
    external_requirements: tuple[ExternalInputRequirement, ...],
) -> None:
    if not external_requirements:
        return
    if len(external_requirements) != 1:
        raise ValueError("external requirements allow exactly one pending request")
    if getattr(directive, "value", directive) != "CONTINUE":
        raise ValueError("external requirements require a CONTINUE decision")

    requirement = external_requirements[0]
    if requirement.request_kind is ExternalRequestKind.OUTCOME_RECONCILIATION:
        raise ValueError("external outcome reconciliation is Engine-owned")
    if requirement.request_kind is ExternalRequestKind.ACTION_APPROVAL:
        if (
            len(proposals) != 1
            or requirement.action_id != proposals[0].action_id
        ):
            raise ValueError(
                "external approval must name the decision's only Action proposal"
            )
        return
    if proposals:
        raise ValueError("external additional input cannot accompany Action proposals")


class NormalizedAction(ActionProposal):
    reservation_id: StableId
    idempotency_key: StableId


class ActionSucceededOutcome(FrozenModel):
    status: Literal[ActionStatus.SUCCEEDED]
    action_id: StableId
    result_ref: ArtifactRef


class ActionFailedOutcome(FrozenModel):
    status: Literal[ActionStatus.FAILED]
    action_id: StableId
    error: ErrorSummary


class ActionTimedOutOutcome(FrozenModel):
    status: Literal[ActionStatus.TIMED_OUT]
    action_id: StableId
    error: ErrorSummary


class ActionCancelledOutcome(FrozenModel):
    status: Literal[ActionStatus.CANCELLED]
    action_id: StableId
    error: ErrorSummary


class ActionUnknownOutcome(FrozenModel):
    status: Literal[ActionStatus.OUTCOME_UNKNOWN]
    action_id: StableId
    error: ErrorSummary


ActionOutcome = Annotated[
    ActionSucceededOutcome
    | ActionFailedOutcome
    | ActionTimedOutOutcome
    | ActionCancelledOutcome
    | ActionUnknownOutcome,
    Field(discriminator="status"),
]


ActionExecutionStatus = ActionStatus


__all__ = [
    "ActionCancelledOutcome",
    "ActionExecutionStatus",
    "ActionFailedOutcome",
    "ActionOutcome",
    "ActionProposal",
    "ActionSucceededOutcome",
    "ActionTimedOutOutcome",
    "ActionType",
    "ActionUnknownOutcome",
    "ExternalInputRequirement",
    "InvocationStatus",
    "NormalizedAction",
    "ResourceKind",
    "ResourceRequest",
    "validate_strategy_external_requirements",
    "validate_stable_id",
]
