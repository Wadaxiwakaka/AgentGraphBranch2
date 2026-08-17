from __future__ import annotations

import json
from collections.abc import Iterable, Mapping
from datetime import datetime, timedelta
from hashlib import sha256
from typing import Annotated, Any, ClassVar, Literal
from uuid import UUID

from pydantic import (
    BaseModel,
    ConfigDict,
    Field,
    PrivateAttr,
    StrictInt,
    StrictStr,
    TypeAdapter,
    field_validator,
    model_validator,
)

from .actions import (
    ActionOutcome,
    ActionProposal,
    ActionStatus,
    ActionUnknownOutcome,
    ExternalInputRequirement,
    NormalizedAction,
    StableId,
    validate_strategy_external_requirements,
)
from .contract import StrategyDirective
from .state import (
    ArtifactRef,
    BudgetState,
    ErrorSummary,
    ExternalRequest,
    ExternalRequestKind,
    ExternalResponseKind,
    FrozenModel,
    ResourceRequest,
    StrategyStateEnvelope,
)


class UnsupportedEventSchema(ValueError):
    code = "unsupported_event_schema"
    safe_message = "The event schema version is not supported."

    def __init__(self) -> None:
        super().__init__(self.safe_message)


class EventEnvelope(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    schema_version: Literal[1]
    event_id: UUID
    trial_id: StrictStr
    attempt_id: StrictStr
    sequence_no: StrictInt = Field(ge=1)
    event_type: StrictStr
    command_id: UUID
    causal_parent_id: UUID | None = Field(
        default=None,
        exclude_if=lambda value: value is None,
    )
    logical_time: StrictInt = Field(ge=0)
    wall_time_utc: datetime

    @field_validator("trial_id", "attempt_id", "event_type")
    @classmethod
    def validate_required_text(cls, value: str, info: Any) -> str:
        if not value:
            raise ValueError(f"{info.field_name} must not be empty")
        return value

    @field_validator("wall_time_utc")
    @classmethod
    def validate_wall_time_utc(cls, value: datetime) -> datetime:
        if value.tzinfo is None or value.utcoffset() != timedelta(0):
            raise ValueError("wall_time_utc must be timezone-aware and use the UTC offset")
        return value


class _NonRootEvent(EventEnvelope):
    @model_validator(mode="after")
    def validate_causal_parent(self) -> _NonRootEvent:
        if self.causal_parent_id is None:
            raise ValueError("non-root events require a causal parent")
        return self


class AttemptPlanned(EventEnvelope):
    event_type: Literal["ATTEMPT_PLANNED"]
    state_schema_version: StrictInt = Field(ge=1)
    experiment_id: StrictStr
    strategy_id: StrictStr
    manifest_ref: ArtifactRef
    strategy: StrategyStateEnvelope
    budget: BudgetState

    @field_validator("experiment_id", "strategy_id")
    @classmethod
    def validate_domain_id(cls, value: str, info: Any) -> str:
        if not value:
            raise ValueError(f"{info.field_name} must not be empty")
        return value

    @model_validator(mode="after")
    def validate_root_causality(self) -> AttemptPlanned:
        if self.causal_parent_id is not None:
            raise ValueError("AttemptPlanned must not have a causal parent")
        return self


class AttemptStarted(_NonRootEvent):
    event_type: Literal["ATTEMPT_STARTED"]


class AttemptRecoveryRequested(_NonRootEvent):
    event_type: Literal["ATTEMPT_RECOVERY_REQUESTED"]


class PauseRequested(_NonRootEvent):
    event_type: Literal["PAUSE_REQUESTED"]


class AttemptPaused(_NonRootEvent):
    event_type: Literal["ATTEMPT_PAUSED"]


class AttemptResumed(_NonRootEvent):
    event_type: Literal["ATTEMPT_RESUMED"]


class CancelRequested(_NonRootEvent):
    event_type: Literal["CANCEL_REQUESTED"]


class AttemptSucceeded(_NonRootEvent):
    event_type: Literal["ATTEMPT_SUCCEEDED"]
    result_ref: ArtifactRef


class AttemptFailed(_NonRootEvent):
    event_type: Literal["ATTEMPT_FAILED"]
    error: ErrorSummary


class AttemptCancelled(_NonRootEvent):
    event_type: Literal["ATTEMPT_CANCELLED"]


class AttemptTimedOut(_NonRootEvent):
    event_type: Literal["ATTEMPT_TIMED_OUT"]
    error: ErrorSummary


class AttemptInterrupted(_NonRootEvent):
    event_type: Literal["ATTEMPT_INTERRUPTED"]
    error: ErrorSummary


class StrategyDecisionRecorded(_NonRootEvent):
    _legacy_cursor_origin: bool = PrivateAttr(default=False)
    _legacy_cursor_fingerprint: str | None = PrivateAttr(default=None)

    event_type: Literal["STRATEGY_DECISION_RECORDED"]
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
    def validate_directive_shape(self) -> StrategyDecisionRecorded:
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


def _has_legacy_cursor_shape(event: StrategyDecisionRecorded) -> bool:
    return (
        event.trigger_sequence_no == event.sequence_no - 1
        and not event.proposals
        and not event.external_requirements
        and event.directive is StrategyDirective.CONTINUE
        and event.result_ref is None
        and event.error is None
    )


def _legacy_strategy_decision_fingerprint(
    event: StrategyDecisionRecorded,
) -> str:
    payload = event.model_dump(mode="json", exclude_none=False)
    normalized = json.dumps(
        payload,
        ensure_ascii=False,
        allow_nan=False,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")
    return sha256(normalized).hexdigest()


class BudgetReservationEntry(FrozenModel):
    action_id: StableId
    reservation_id: StableId
    resource_requests: tuple[ResourceRequest, ...]

    @field_validator("resource_requests")
    @classmethod
    def validate_resource_requests(
        cls,
        value: tuple[ResourceRequest, ...],
    ) -> tuple[ResourceRequest, ...]:
        resources = [request.resource for request in value]
        if len(resources) != len(set(resources)):
            raise ValueError("resource requests must use unique resources")
        return value


class BudgetReserved(_NonRootEvent):
    event_type: Literal["BUDGET_RESERVED"]
    budget: BudgetState
    reservations: tuple[BudgetReservationEntry, ...] = Field(min_length=1)

    @model_validator(mode="after")
    def validate_reservations(self) -> BudgetReserved:
        action_ids = [reservation.action_id for reservation in self.reservations]
        reservation_ids = [
            reservation.reservation_id for reservation in self.reservations
        ]
        if len(action_ids) != len(set(action_ids)):
            raise ValueError("budget reservations must use unique action ids")
        if len(reservation_ids) != len(set(reservation_ids)):
            raise ValueError("budget reservations must use unique reservation ids")
        return self


class BudgetSettled(_NonRootEvent):
    event_type: Literal["BUDGET_SETTLED"]
    budget: BudgetState
    reservation: BudgetReservationEntry


class BudgetReleased(_NonRootEvent):
    event_type: Literal["BUDGET_RELEASED"]
    budget: BudgetState
    reservation: BudgetReservationEntry


class BudgetUncertainSettled(_NonRootEvent):
    event_type: Literal["BUDGET_UNCERTAIN_SETTLED"]
    budget: BudgetState
    reservation: BudgetReservationEntry


class ExternalInputRequested(_NonRootEvent):
    event_type: Literal["EXTERNAL_INPUT_REQUESTED"]
    request: ExternalRequest


class ExternalInputReceived(_NonRootEvent):
    event_type: Literal["EXTERNAL_INPUT_RECEIVED"]
    request_id: StableId
    request_kind: ExternalRequestKind
    action_id: StableId | None = None
    response_kind: ExternalResponseKind
    response_ref: ArtifactRef | None = None
    error: ErrorSummary | None = None

    @model_validator(mode="after")
    def validate_response_shape(self) -> ExternalInputReceived:
        requires_action = self.request_kind in {
            ExternalRequestKind.ACTION_APPROVAL,
            ExternalRequestKind.OUTCOME_RECONCILIATION,
        }
        if requires_action != (self.action_id is not None):
            raise ValueError("external response action_id does not match request_kind")
        allowed = {
            ExternalRequestKind.ACTION_APPROVAL: {
                ExternalResponseKind.APPROVE,
                ExternalResponseKind.REJECT,
            },
            ExternalRequestKind.ADDITIONAL_INPUT: {
                ExternalResponseKind.PROVIDE_INPUT,
            },
            ExternalRequestKind.OUTCOME_RECONCILIATION: {
                ExternalResponseKind.CONFIRM_SUCCEEDED,
                ExternalResponseKind.CONFIRM_FAILED,
                ExternalResponseKind.ABANDON,
            },
        }
        if self.response_kind not in allowed[self.request_kind]:
            raise ValueError("external response kind does not match request kind")
        if self.response_kind in {
            ExternalResponseKind.APPROVE,
            ExternalResponseKind.REJECT,
            ExternalResponseKind.ABANDON,
        }:
            valid = self.response_ref is None and self.error is None
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


class _ExternalInputResolution(_NonRootEvent):
    request_id: StableId
    request_kind: ExternalRequestKind
    action_id: StableId | None = None

    @model_validator(mode="after")
    def validate_request_shape(self) -> _ExternalInputResolution:
        requires_action = self.request_kind in {
            ExternalRequestKind.ACTION_APPROVAL,
            ExternalRequestKind.OUTCOME_RECONCILIATION,
        }
        if requires_action != (self.action_id is not None):
            raise ValueError("external resolution action_id does not match request_kind")
        return self


class ExternalInputApproved(_ExternalInputResolution):
    event_type: Literal["EXTERNAL_INPUT_APPROVED"]


class ExternalInputRejected(_ExternalInputResolution):
    event_type: Literal["EXTERNAL_INPUT_REJECTED"]


class ExternalInputExpired(_ExternalInputResolution):
    event_type: Literal["EXTERNAL_INPUT_EXPIRED"]


class ActionProposed(_NonRootEvent):
    event_type: Literal["ACTION_PROPOSED"]
    proposal: ActionProposal

    @model_validator(mode="after")
    def validate_proposal_parent(self) -> ActionProposed:
        if str(self.causal_parent_id) != self.proposal.causal_parent_id:
            raise ValueError("proposal causal_parent_id must match the event envelope")
        return self


class ActionRejected(_NonRootEvent):
    event_type: Literal["ACTION_REJECTED"]
    action_id: StableId
    error: ErrorSummary


class ActionAccepted(_NonRootEvent):
    event_type: Literal["ACTION_ACCEPTED"]
    action: NormalizedAction


class ActionStarted(_NonRootEvent):
    event_type: Literal["ACTION_STARTED"]
    action_id: StableId


class ActionCancellationRequested(_NonRootEvent):
    event_type: Literal["ACTION_CANCELLATION_REQUESTED"]
    action_id: StableId


class _ActionTerminalEvent(_NonRootEvent):
    action_id: StableId
    outcome: ActionOutcome

    _expected_status: ClassVar[ActionStatus]

    @model_validator(mode="after")
    def validate_outcome(self) -> _ActionTerminalEvent:
        if self.outcome.action_id != self.action_id:
            raise ValueError("event action_id must match outcome action_id")
        if self.outcome.status is not self._expected_status:
            raise ValueError("outcome status does not match the event type")
        return self


class ActionSucceeded(_ActionTerminalEvent):
    event_type: Literal["ACTION_SUCCEEDED"]
    _expected_status = ActionStatus.SUCCEEDED


class ActionFailed(_ActionTerminalEvent):
    event_type: Literal["ACTION_FAILED"]
    _expected_status = ActionStatus.FAILED


class ActionTimedOut(_ActionTerminalEvent):
    event_type: Literal["ACTION_TIMED_OUT"]
    _expected_status = ActionStatus.TIMED_OUT


class ActionCancelled(_ActionTerminalEvent):
    event_type: Literal["ACTION_CANCELLED"]
    _expected_status = ActionStatus.CANCELLED


class ActionOutcomeUnknown(_ActionTerminalEvent):
    event_type: Literal["ACTION_OUTCOME_UNKNOWN"]
    _expected_status = ActionStatus.OUTCOME_UNKNOWN


class ActionOutcomeReconciled(_NonRootEvent):
    event_type: Literal["ACTION_OUTCOME_RECONCILED"]
    action_id: StableId
    outcome: ActionOutcome

    @model_validator(mode="after")
    def validate_reconciled_outcome(self) -> ActionOutcomeReconciled:
        if self.outcome.action_id != self.action_id:
            raise ValueError("event action_id must match outcome action_id")
        if isinstance(self.outcome, ActionUnknownOutcome):
            raise ValueError("reconciled outcome cannot be OUTCOME_UNKNOWN")
        return self


class InvocationRequested(_NonRootEvent):
    event_type: Literal["INVOCATION_REQUESTED"]
    action_id: StableId
    invocation_id: StableId
    agent_id: StableId
    conversation_id: StableId
    parent_invocation_id: StableId | None
    latest_context_ref: ArtifactRef | None


class InvocationStarted(_NonRootEvent):
    event_type: Literal["INVOCATION_STARTED"]
    action_id: StableId
    invocation_id: StableId


class InvocationCompleted(_NonRootEvent):
    event_type: Literal["INVOCATION_COMPLETED"]
    action_id: StableId
    invocation_id: StableId
    latest_context_ref: ArtifactRef | None


class InvocationFailed(_NonRootEvent):
    event_type: Literal["INVOCATION_FAILED"]
    action_id: StableId
    invocation_id: StableId
    error: ErrorSummary


DomainEvent = Annotated[
    AttemptPlanned
    | AttemptStarted
    | AttemptRecoveryRequested
    | PauseRequested
    | AttemptPaused
    | AttemptResumed
    | CancelRequested
    | AttemptSucceeded
    | AttemptFailed
    | AttemptCancelled
    | AttemptTimedOut
    | AttemptInterrupted
    | StrategyDecisionRecorded
    | BudgetReserved
    | BudgetSettled
    | BudgetReleased
    | BudgetUncertainSettled
    | ExternalInputRequested
    | ExternalInputReceived
    | ExternalInputApproved
    | ExternalInputRejected
    | ExternalInputExpired
    | ActionProposed
    | ActionRejected
    | ActionAccepted
    | ActionStarted
    | ActionCancellationRequested
    | ActionSucceeded
    | ActionFailed
    | ActionTimedOut
    | ActionCancelled
    | ActionOutcomeUnknown
    | ActionOutcomeReconciled
    | InvocationRequested
    | InvocationStarted
    | InvocationCompleted
    | InvocationFailed,
    Field(discriminator="event_type"),
]


STRATEGY_TRIGGER_EVENT_TYPES = (
    AttemptStarted,
    ActionRejected,
    ActionSucceeded,
    ActionFailed,
    ActionTimedOut,
    ActionCancelled,
    ActionOutcomeUnknown,
    ActionOutcomeReconciled,
)


def is_strategy_trigger(event: DomainEvent) -> bool:
    return isinstance(event, STRATEGY_TRIGGER_EVENT_TYPES) or (
        isinstance(event, ExternalInputReceived)
        and event.request_kind is ExternalRequestKind.ADDITIONAL_INPUT
    )


def effective_strategy_triggers(
    events: Iterable[DomainEvent],
) -> tuple[DomainEvent, ...]:
    values = tuple(events)
    effective: list[DomainEvent] = []
    for index, event in enumerate(values):
        if isinstance(event, ActionOutcomeUnknown) and index + 1 < len(values):
            following = values[index + 1]
            if (
                isinstance(following, ExternalInputRequested)
                and following.request.request_kind
                is ExternalRequestKind.OUTCOME_RECONCILIATION
                and following.request.action_id == event.action_id
                and following.command_id == event.command_id
                and following.causal_parent_id == event.event_id
            ):
                continue
        if is_strategy_trigger(event):
            effective.append(event)
    return tuple(effective)


class _SchemaVersionProbe(BaseModel):
    model_config = ConfigDict(extra="allow")

    schema_version: StrictInt


_DOMAIN_EVENT_ADAPTER = TypeAdapter(DomainEvent)
_LEGACY_STRATEGY_DECISION_FIELDS = frozenset(
    {
        "schema_version",
        "event_id",
        "trial_id",
        "attempt_id",
        "sequence_no",
        "event_type",
        "command_id",
        "causal_parent_id",
        "logical_time",
        "wall_time_utc",
        "strategy",
    }
)
_STRATEGY_DECISION_V1_ADDITIONS = frozenset(
    {
        "trigger_sequence_no",
        "proposals",
        "external_requirements",
        "directive",
        "result_ref",
        "error",
    }
)


def _to_validation_payload(value: object) -> object:
    if isinstance(value, BaseModel):
        declared_fields = type(value).model_fields
        payload: dict[str, object] = {}
        for name, item in vars(value).items():
            field = declared_fields.get(name)
            if field is not None:
                if field.exclude:
                    continue
                if field.exclude_if is not None and field.exclude_if(item):
                    continue
            payload[name] = _to_validation_payload(item)
        return payload
    if isinstance(value, Mapping):
        return {
            key: _to_validation_payload(item)
            for key, item in value.items()
        }
    if isinstance(value, (list, tuple)):
        return [_to_validation_payload(item) for item in value]
    return value


def _upcast_legacy_strategy_decision(payload: object) -> object:
    if not _is_legacy_strategy_decision_payload(payload):
        return payload
    sequence_no = payload.get("sequence_no")
    return {
        **payload,
        "trigger_sequence_no": sequence_no - 1,
        "proposals": (),
        "directive": StrategyDirective.CONTINUE,
    }


def _is_legacy_strategy_decision_payload(payload: object) -> bool:
    if not isinstance(payload, Mapping):
        return False
    if (
        payload.get("schema_version") != 1
        or payload.get("event_type") != "STRATEGY_DECISION_RECORDED"
        or set(payload) != _LEGACY_STRATEGY_DECISION_FIELDS
        or set(payload).intersection(_STRATEGY_DECISION_V1_ADDITIONS)
    ):
        return False
    sequence_no = payload.get("sequence_no")
    return type(sequence_no) is int and sequence_no >= 2


def parse_domain_event(value: object) -> DomainEvent:
    inherited_legacy_origin = False
    inherited_fingerprint: str | None = None
    if isinstance(value, StrategyDecisionRecorded):
        inherited_legacy_origin = (
            value._legacy_cursor_origin
            and _has_legacy_cursor_shape(value)
            and value._legacy_cursor_fingerprint is not None
        )
        if inherited_legacy_origin:
            inherited_fingerprint = value._legacy_cursor_fingerprint
    validation_payload = _to_validation_payload(value)
    raw_legacy_origin = _is_legacy_strategy_decision_payload(validation_payload)
    payload = _upcast_legacy_strategy_decision(validation_payload)
    if isinstance(payload, Mapping):
        probe = _SchemaVersionProbe.model_validate(payload)
        if probe.schema_version != 1:
            raise UnsupportedEventSchema()
    event = _DOMAIN_EVENT_ADAPTER.validate_python(payload)
    if isinstance(event, StrategyDecisionRecorded):
        fingerprint = _legacy_strategy_decision_fingerprint(event)
        legacy_cursor_origin = raw_legacy_origin or (
            inherited_legacy_origin and inherited_fingerprint == fingerprint
        )
        if legacy_cursor_origin:
            event._legacy_cursor_origin = True
            event._legacy_cursor_fingerprint = fingerprint
    elif raw_legacy_origin or inherited_legacy_origin:
        raise TypeError("legacy Strategy decision parsed as another event type")
    return event
