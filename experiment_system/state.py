from __future__ import annotations

import json
import math
import re
from collections.abc import Iterator, Mapping
from datetime import datetime, timedelta
from enum import StrEnum
from hashlib import sha256
from pathlib import PurePosixPath
from types import MappingProxyType
from typing import Annotated, Any, Literal

from pydantic import (
    BaseModel,
    BeforeValidator,
    ConfigDict,
    Field,
    PlainSerializer,
    StrictBool,
    StrictInt,
    StrictStr,
)
from pydantic import field_validator, model_validator


_SHA256_PATTERN = re.compile(r"^[0-9a-f]{64}$")
_STABLE_ID_PATTERN = re.compile(r"^[A-Za-z0-9](?:[A-Za-z0-9._-]*[A-Za-z0-9])?$")


class _MissingValue:
    def __copy__(self) -> _MissingValue:
        return self

    def __deepcopy__(self, memo: dict[int, object]) -> _MissingValue:
        return self


_MISSING_VALUE = _MissingValue()


class _FrozenMapping(Mapping[str, Any]):
    __slots__ = ("__data",)

    def __init__(self, values: Mapping[str, Any]) -> None:
        object.__setattr__(self, "_FrozenMapping__data", MappingProxyType(dict(values)))

    def __getitem__(self, key: str) -> Any:
        return self.__data[key]

    def __iter__(self) -> Iterator[str]:
        return iter(self.__data)

    def __len__(self) -> int:
        return len(self.__data)

    def __eq__(self, other: object) -> bool:
        if isinstance(other, Mapping):
            return dict(self.items()) == dict(other.items())
        return NotImplemented

    def __repr__(self) -> str:
        return repr(dict(self.items()))

    def __setattr__(self, name: str, value: object) -> None:
        raise TypeError("frozen JSON objects cannot be mutated")


def _freeze_json(value: Any, *, location: str = "value") -> Any:
    if value is None or isinstance(value, (bool, int, str)):
        return value
    if isinstance(value, float):
        if not math.isfinite(value):
            raise ValueError(f"{location} JSON floats must be finite")
        return value
    if isinstance(value, (list, tuple)):
        return tuple(
            _freeze_json(item, location=f"{location}[{index}]")
            for index, item in enumerate(value)
        )
    if isinstance(value, _FrozenMapping):
        return value
    if isinstance(value, dict):
        frozen: dict[str, Any] = {}
        for key, item in value.items():
            if not isinstance(key, str):
                raise ValueError(f"{location} JSON object keys must be strings")
            frozen[key] = _freeze_json(item, location=f"{location}.{key}")
        return _FrozenMapping(frozen)
    raise ValueError(f"{location} must contain only standard JSON values")


def _json_for_serialization(value: Any) -> Any:
    if isinstance(value, _FrozenMapping):
        return {
            key: _json_for_serialization(item)
            for key, item in value.items()
        }
    if isinstance(value, tuple):
        return [_json_for_serialization(item) for item in value]
    return value


_FrozenJson = Annotated[
    Any,
    BeforeValidator(_freeze_json),
    PlainSerializer(_json_for_serialization, return_type=Any, when_used="json"),
]


def _canonical_json_bytes(value: Any) -> bytes:
    return json.dumps(
        _json_for_serialization(value),
        ensure_ascii=False,
        allow_nan=False,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")


def _validate_sha256(value: str) -> str:
    if not _SHA256_PATTERN.fullmatch(value):
        raise ValueError("content hash must be a lowercase 64-character SHA-256 hex digest")
    return value


def _validate_utc(value: datetime | None) -> datetime | None:
    if value is None:
        return None
    if value.tzinfo is None or value.utcoffset() != timedelta(0):
        raise ValueError("timestamp must be timezone-aware and use the UTC offset")
    return value


def _validate_non_empty(value: str, *, field_name: str) -> str:
    if not value:
        raise ValueError(f"{field_name} must not be empty")
    return value


def _validate_stable_id(value: str, *, field_name: str) -> str:
    if not _STABLE_ID_PATTERN.fullmatch(value):
        raise ValueError(f"{field_name} must be a stable ASCII id")
    return value


class FrozenModel(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)


class AttemptPhase(StrEnum):
    PLANNED = "PLANNED"
    RUNNING = "RUNNING"
    PAUSE_REQUESTED = "PAUSE_REQUESTED"
    PAUSED = "PAUSED"
    WAITING_EXTERNAL = "WAITING_EXTERNAL"
    CANCEL_REQUESTED = "CANCEL_REQUESTED"
    SUCCEEDED = "SUCCEEDED"
    FAILED = "FAILED"
    CANCELLED = "CANCELLED"
    TIMED_OUT = "TIMED_OUT"
    INTERRUPTED = "INTERRUPTED"


class ActionStatus(StrEnum):
    PROPOSED = "PROPOSED"
    REJECTED = "REJECTED"
    ACCEPTED = "ACCEPTED"
    STARTED = "STARTED"
    SUCCEEDED = "SUCCEEDED"
    FAILED = "FAILED"
    TIMED_OUT = "TIMED_OUT"
    CANCELLED = "CANCELLED"
    OUTCOME_UNKNOWN = "OUTCOME_UNKNOWN"


TERMINAL_ACTION_STATUSES: frozenset[ActionStatus] = frozenset(
    {
        ActionStatus.REJECTED,
        ActionStatus.SUCCEEDED,
        ActionStatus.FAILED,
        ActionStatus.TIMED_OUT,
        ActionStatus.CANCELLED,
        ActionStatus.OUTCOME_UNKNOWN,
    }
)


class InvocationStatus(StrEnum):
    REQUESTED = "REQUESTED"
    RUNNING = "RUNNING"
    COMPLETED = "COMPLETED"
    FAILED = "FAILED"


class RecoveryPolicy(StrEnum):
    REPLAY_SAFE = "REPLAY_SAFE"
    RECONCILABLE = "RECONCILABLE"
    NON_REPLAYABLE = "NON_REPLAYABLE"


class ResourceKind(StrEnum):
    INPUT_TOKENS = "INPUT_TOKENS"
    OUTPUT_TOKENS = "OUTPUT_TOKENS"
    MODEL_CALLS = "MODEL_CALLS"
    DOMAIN_TOOL_CALLS = "DOMAIN_TOOL_CALLS"
    COORDINATION_ACTIONS = "COORDINATION_ACTIONS"


class ExternalRequestKind(StrEnum):
    ACTION_APPROVAL = "ACTION_APPROVAL"
    ADDITIONAL_INPUT = "ADDITIONAL_INPUT"
    OUTCOME_RECONCILIATION = "OUTCOME_RECONCILIATION"


class ExternalResponseKind(StrEnum):
    APPROVE = "APPROVE"
    REJECT = "REJECT"
    PROVIDE_INPUT = "PROVIDE_INPUT"
    CONFIRM_SUCCEEDED = "CONFIRM_SUCCEEDED"
    CONFIRM_FAILED = "CONFIRM_FAILED"
    ABANDON = "ABANDON"


class ResourceRequest(FrozenModel):
    resource: ResourceKind
    amount: StrictInt = Field(ge=0)


class ArtifactRef(FrozenModel):
    capture_class: Literal["full", "hashed", "metadata_only"]
    content_hash: StrictStr | None = None
    media_type: StrictStr
    byte_size: StrictInt = Field(ge=0)
    relative_path: StrictStr | None = None

    @field_validator("content_hash")
    @classmethod
    def validate_content_hash(cls, value: str | None) -> str | None:
        return None if value is None else _validate_sha256(value)

    @field_validator("media_type")
    @classmethod
    def validate_media_type(cls, value: str) -> str:
        return _validate_non_empty(value, field_name="media_type")

    @field_validator("relative_path")
    @classmethod
    def validate_relative_path(cls, value: str | None) -> str | None:
        if value is None:
            return None
        path = PurePosixPath(value)
        raw_parts = value.split("/")
        unsafe = (
            not value
            or value.startswith(("/", "\\"))
            or "\\" in value
            or ":" in value
            or path.is_absolute()
            or any(part in {"", ".", ".."} for part in raw_parts)
        )
        if unsafe:
            raise ValueError("artifact relative path must be a safe POSIX path below the artifact root")
        return value

    @model_validator(mode="after")
    def validate_capture_contract(self) -> ArtifactRef:
        if self.capture_class == "full":
            if self.content_hash is None or self.relative_path is None:
                raise ValueError("full capture requires a content hash and relative path")
        elif self.capture_class == "hashed":
            if self.content_hash is None or self.relative_path is not None:
                raise ValueError("hashed capture requires a content hash and forbids a relative path")
        elif self.content_hash is not None or self.relative_path is not None:
            raise ValueError("metadata_only capture forbids a content hash and relative path")
        return self


class ErrorSummary(FrozenModel):
    code: StrictStr
    retryable: StrictBool
    safe_message: StrictStr
    detail_ref: ArtifactRef | None = None

    @field_validator("code", "safe_message")
    @classmethod
    def validate_text(cls, value: str, info: Any) -> str:
        return _validate_non_empty(value, field_name=info.field_name)


class ResourceBudget(FrozenModel):
    resource: StrictStr
    limit: StrictInt = Field(ge=0)
    reserved: StrictInt = Field(default=0, ge=0)
    consumed: StrictInt = Field(default=0, ge=0)

    @field_validator("resource")
    @classmethod
    def validate_resource(cls, value: str) -> str:
        return _validate_non_empty(value, field_name="resource")

    @model_validator(mode="after")
    def validate_ledger(self) -> ResourceBudget:
        if self.reserved + self.consumed > self.limit:
            raise ValueError("reserved plus consumed must not exceed the resource limit")
        return self


class BudgetState(FrozenModel):
    resources: tuple[ResourceBudget, ...]
    deadline_at: datetime | None
    max_call_depth: StrictInt = Field(ge=0)
    max_concurrent_actions: StrictInt = Field(ge=1)

    @property
    def counters(self) -> Mapping[str, ResourceBudget]:
        return MappingProxyType(
            {resource.resource: resource for resource in self.resources}
        )

    @field_validator("deadline_at")
    @classmethod
    def validate_deadline(cls, value: datetime | None) -> datetime | None:
        return _validate_utc(value)

    @model_validator(mode="after")
    def validate_unique_resources(self) -> BudgetState:
        names = [resource.resource for resource in self.resources]
        if len(names) != len(set(names)):
            raise ValueError("resource names must be unique")
        return self


class StrategyStateEnvelope(FrozenModel):
    strategy_id: StrictStr
    strategy_schema_version: StrictInt = Field(ge=1)
    value: _FrozenJson = Field(
        default=_MISSING_VALUE,
        exclude_if=lambda value: value is _MISSING_VALUE,
    )
    artifact_ref: ArtifactRef | None = None
    content_hash: StrictStr
    byte_size: StrictInt = Field(ge=0)

    @field_validator("strategy_id")
    @classmethod
    def validate_strategy_id(cls, value: str) -> str:
        return _validate_non_empty(value, field_name="strategy_id")

    @field_validator("content_hash")
    @classmethod
    def validate_content_hash(cls, value: str) -> str:
        return _validate_sha256(value)

    @model_validator(mode="after")
    def validate_storage(self) -> StrategyStateEnvelope:
        has_value = self.value is not _MISSING_VALUE
        has_artifact = self.artifact_ref is not None
        if has_value == has_artifact:
            raise ValueError("strategy state must provide exactly one of value or artifact_ref")

        if has_value:
            payload = _canonical_json_bytes(self.value)
            expected_hash = sha256(payload).hexdigest()
            if self.byte_size != len(payload):
                raise ValueError("byte_size does not match the canonical strategy value")
            if self.content_hash != expected_hash:
                raise ValueError("content_hash does not match the canonical strategy value")
        else:
            assert self.artifact_ref is not None
            if self.artifact_ref.content_hash is None:
                raise ValueError("strategy artifact_ref must include a content_hash")
            if self.content_hash != self.artifact_ref.content_hash:
                raise ValueError("content_hash does not match strategy artifact_ref")
            if self.byte_size != self.artifact_ref.byte_size:
                raise ValueError("byte_size does not match strategy artifact_ref")
        return self


class ActionState(FrozenModel):
    action_id: StrictStr
    action_type: StrictStr
    actor_id: StrictStr
    target_ids: tuple[StrictStr, ...]
    status: ActionStatus
    causal_parent_id: StrictStr | None
    invocation_id: StrictStr | None
    payload_ref: ArtifactRef | None
    result_ref: ArtifactRef | None
    idempotency_key: StrictStr | None
    recovery_policy: RecoveryPolicy
    reservation_id: StrictStr | None
    retry_of_action_id: StrictStr | None
    error_code: StrictStr | None
    requested_timeout: StrictInt = Field(default=1, gt=0)
    batch_id: StrictStr | None = None
    call_depth: StrictInt = Field(default=0, ge=0)
    resource_requests: tuple[ResourceRequest, ...] = ()
    error: ErrorSummary | None = None
    reconciled_status: ActionStatus | None = None
    reconciled_result_ref: ArtifactRef | None = None
    reconciled_error: ErrorSummary | None = None

    @field_validator("action_id", "actor_id")
    @classmethod
    def validate_required_id(cls, value: str, info: Any) -> str:
        return _validate_stable_id(value, field_name=info.field_name)

    @field_validator("action_type")
    @classmethod
    def validate_action_type(cls, value: str) -> str:
        return _validate_non_empty(value, field_name="action_type")

    @field_validator(
        "causal_parent_id",
        "invocation_id",
        "idempotency_key",
        "reservation_id",
        "retry_of_action_id",
        "batch_id",
    )
    @classmethod
    def validate_optional_id(cls, value: str | None, info: Any) -> str | None:
        if value is None:
            return None
        return _validate_stable_id(value, field_name=info.field_name)

    @field_validator("target_ids")
    @classmethod
    def validate_target_ids(cls, value: tuple[str, ...]) -> tuple[str, ...]:
        for target_id in value:
            _validate_stable_id(target_id, field_name="target_ids")
        if len(value) != len(set(value)):
            raise ValueError("target_ids must be unique")
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
    def validate_status_lifecycle(self) -> ActionState:
        has_acceptance = (
            self.reservation_id is not None and self.idempotency_key is not None
        )
        has_no_acceptance = (
            self.reservation_id is None and self.idempotency_key is None
        )
        has_no_outcome = (
            self.result_ref is None
            and self.error is None
            and self.error_code is None
        )
        error_matches = (
            self.error is not None
            and self.error_code is not None
            and self.error_code == self.error.code
        )
        error_statuses = {
            ActionStatus.FAILED,
            ActionStatus.TIMED_OUT,
            ActionStatus.CANCELLED,
            ActionStatus.OUTCOME_UNKNOWN,
        }

        if self.status is ActionStatus.PROPOSED:
            valid_status = has_no_acceptance and has_no_outcome
        elif self.status is ActionStatus.REJECTED:
            valid_status = (
                has_no_acceptance
                and self.result_ref is None
                and error_matches
            )
        elif self.status in {ActionStatus.ACCEPTED, ActionStatus.STARTED}:
            valid_status = has_acceptance and has_no_outcome
        elif self.status is ActionStatus.SUCCEEDED:
            valid_status = (
                has_acceptance
                and self.result_ref is not None
                and self.error is None
                and self.error_code is None
            )
        else:
            valid_status = (
                self.status in error_statuses
                and has_acceptance
                and self.result_ref is None
                and error_matches
            )
        if not valid_status:
            raise ValueError("Action status lifecycle fields are inconsistent")
        return self

    @model_validator(mode="after")
    def validate_reconciliation(self) -> ActionState:
        has_reconciliation = any(
            value is not None
            for value in (
                self.reconciled_status,
                self.reconciled_result_ref,
                self.reconciled_error,
            )
        )
        if has_reconciliation and self.status is not ActionStatus.OUTCOME_UNKNOWN:
            raise ValueError("only outcome-unknown Actions may carry reconciliation")
        valid_reconciled_statuses = {
            ActionStatus.SUCCEEDED,
            ActionStatus.FAILED,
            ActionStatus.TIMED_OUT,
            ActionStatus.CANCELLED,
        }
        if (
            self.reconciled_status is not None
            and self.reconciled_status not in valid_reconciled_statuses
        ):
            raise ValueError("reconciled status must be a known terminal outcome")
        if self.reconciled_status is ActionStatus.SUCCEEDED:
            if self.reconciled_result_ref is None or self.reconciled_error is not None:
                raise ValueError("successful reconciliation requires only a result_ref")
        elif self.reconciled_status is not None:
            if self.reconciled_error is None or self.reconciled_result_ref is not None:
                raise ValueError("non-success reconciliation requires only an error")
        elif self.reconciled_result_ref is not None or self.reconciled_error is not None:
            raise ValueError("reconciliation details require a reconciled status")
        return self


class InvocationState(FrozenModel):
    invocation_id: StrictStr
    agent_id: StrictStr
    conversation_id: StrictStr
    parent_invocation_id: StrictStr | None
    latest_context_ref: ArtifactRef | None
    last_action_id: StrictStr | None
    status: InvocationStatus

    @field_validator("invocation_id", "agent_id", "conversation_id")
    @classmethod
    def validate_required_id(cls, value: str, info: Any) -> str:
        return _validate_stable_id(value, field_name=info.field_name)

    @field_validator("parent_invocation_id", "last_action_id")
    @classmethod
    def validate_optional_id(cls, value: str | None, info: Any) -> str | None:
        if value is None:
            return None
        return _validate_stable_id(value, field_name=info.field_name)


class ExternalRequest(FrozenModel):
    request_id: StrictStr
    request_kind: ExternalRequestKind
    action_id: StrictStr | None = None
    payload_ref: ArtifactRef | None = None

    @field_validator("request_id")
    @classmethod
    def validate_request_id(cls, value: str) -> str:
        return _validate_stable_id(value, field_name="request_id")

    @field_validator("action_id")
    @classmethod
    def validate_action_id(cls, value: str | None) -> str | None:
        if value is None:
            return None
        return _validate_stable_id(value, field_name="action_id")

    @model_validator(mode="after")
    def validate_request_shape(self) -> ExternalRequest:
        requires_action = self.request_kind in {
            ExternalRequestKind.ACTION_APPROVAL,
            ExternalRequestKind.OUTCOME_RECONCILIATION,
        }
        if requires_action != (self.action_id is not None):
            raise ValueError("external request action_id does not match request_kind")
        return self


class AttemptState(FrozenModel):
    schema_version: StrictInt = Field(ge=1)
    experiment_id: StrictStr
    trial_id: StrictStr
    attempt_id: StrictStr
    strategy_id: StrictStr

    revision: StrictInt = Field(ge=1)
    phase: AttemptPhase
    manifest_ref: ArtifactRef

    strategy: StrategyStateEnvelope
    budget: BudgetState
    actions: tuple[ActionState, ...]
    invocations: tuple[InvocationState, ...]
    pending_external: tuple[ExternalRequest, ...]

    result_ref: ArtifactRef | None
    terminal_error: ErrorSummary | None
    started_at: datetime | None
    finished_at: datetime | None

    @field_validator("experiment_id", "trial_id", "attempt_id", "strategy_id")
    @classmethod
    def validate_required_id(cls, value: str, info: Any) -> str:
        return _validate_non_empty(value, field_name=info.field_name)

    @field_validator("started_at", "finished_at")
    @classmethod
    def validate_timestamp(cls, value: datetime | None) -> datetime | None:
        return _validate_utc(value)

    @model_validator(mode="after")
    def validate_aggregate_shape(self) -> AttemptState:
        active_phases = {
            AttemptPhase.RUNNING,
            AttemptPhase.PAUSE_REQUESTED,
            AttemptPhase.PAUSED,
            AttemptPhase.WAITING_EXTERNAL,
            AttemptPhase.CANCEL_REQUESTED,
        }
        failure_phases = {
            AttemptPhase.FAILED,
            AttemptPhase.TIMED_OUT,
            AttemptPhase.INTERRUPTED,
        }
        if self.phase is AttemptPhase.PLANNED:
            valid_lifecycle = all(
                value is None
                for value in (
                    self.started_at,
                    self.finished_at,
                    self.result_ref,
                    self.terminal_error,
                )
            )
        elif self.phase in active_phases:
            valid_lifecycle = self.started_at is not None and all(
                value is None
                for value in (
                    self.finished_at,
                    self.result_ref,
                    self.terminal_error,
                )
            )
        elif self.phase is AttemptPhase.SUCCEEDED:
            valid_lifecycle = (
                self.started_at is not None
                and self.finished_at is not None
                and self.result_ref is not None
                and self.terminal_error is None
            )
        elif self.phase in failure_phases:
            valid_lifecycle = (
                self.started_at is not None
                and self.finished_at is not None
                and self.result_ref is None
                and self.terminal_error is not None
            )
        else:
            valid_lifecycle = (
                self.started_at is not None
                and self.finished_at is not None
                and self.result_ref is None
                and self.terminal_error is None
            )
        if not valid_lifecycle:
            raise ValueError("Attempt lifecycle fields are inconsistent with phase")
        terminal_phases = {
            AttemptPhase.SUCCEEDED,
            AttemptPhase.FAILED,
            AttemptPhase.CANCELLED,
            AttemptPhase.TIMED_OUT,
            AttemptPhase.INTERRUPTED,
        }
        if self.phase in terminal_phases and any(
            action.status not in TERMINAL_ACTION_STATUSES for action in self.actions
        ):
            raise ValueError(
                "terminal Attempt requires a terminal Action observation for every Action"
            )
        if (
            self.started_at is not None
            and self.finished_at is not None
            and self.finished_at < self.started_at
        ):
            raise ValueError("finished_at must not precede started_at")

        if self.strategy.strategy_id != self.strategy_id:
            raise ValueError("strategy_id must match the strategy envelope strategy_id")

        action_ids = [action.action_id for action in self.actions]
        if len(action_ids) != len(set(action_ids)):
            raise ValueError("action_id values must be unique within an Attempt")

        request_ids = [request.request_id for request in self.pending_external]
        if len(request_ids) != len(set(request_ids)):
            raise ValueError("pending external request ids must be unique")
        if self.phase is AttemptPhase.WAITING_EXTERNAL:
            if len(self.pending_external) != 1:
                raise ValueError(
                    "WAITING_EXTERNAL requires exactly one pending external request"
                )
        elif self.pending_external:
            raise ValueError(
                "only WAITING_EXTERNAL may contain a pending external request"
            )
        for request in self.pending_external:
            if request.request_kind is ExternalRequestKind.ADDITIONAL_INPUT:
                continue
            action = next(
                (
                    candidate
                    for candidate in self.actions
                    if candidate.action_id == request.action_id
                ),
                None,
            )
            if action is None:
                raise ValueError(
                    "pending external request must name an Action in this Attempt"
                )
            if (
                request.request_kind is ExternalRequestKind.ACTION_APPROVAL
                and action.status is not ActionStatus.PROPOSED
            ):
                raise ValueError(
                    "external approval must name a PROPOSED Action"
                )
            if request.request_kind is ExternalRequestKind.OUTCOME_RECONCILIATION:
                if (
                    action.status is not ActionStatus.OUTCOME_UNKNOWN
                    or action.reconciled_status is not None
                ):
                    raise ValueError(
                        "external reconciliation requires an unreconciled "
                        "OUTCOME_UNKNOWN Action"
                    )

        action_invocation_ids = [
            action.invocation_id
            for action in self.actions
            if action.invocation_id is not None
        ]
        if len(action_invocation_ids) != len(set(action_invocation_ids)):
            raise ValueError(
                "Action invocation_id values must be unique within an Attempt"
            )

        invocation_ids = [invocation.invocation_id for invocation in self.invocations]
        if len(invocation_ids) != len(set(invocation_ids)):
            raise ValueError("invocation_id values must be unique within an Attempt")

        for invocation in self.invocations:
            owners = [
                action
                for action in self.actions
                if action.invocation_id == invocation.invocation_id
            ]
            if len(owners) != 1:
                raise ValueError("Invocation owner Action must exist and be unique")
            owner = owners[0]
            if owner.action_type not in {"INVOKE_AGENT", "invoke_agent"}:
                raise ValueError("Invocation owner Action must invoke an Agent")
            if invocation.last_action_id != owner.action_id:
                raise ValueError("Invocation owner action_id must match last_action_id")
            if owner.target_ids != (invocation.agent_id,):
                raise ValueError("Invocation owner target must match agent_id")

        for action in self.actions:
            if action.action_type not in {"INVOKE_AGENT", "invoke_agent"}:
                continue
            if action.invocation_id is None:
                raise ValueError(
                    "INVOKE_AGENT Action/Invocation lifecycle is inconsistent"
                )
            matching = [
                invocation
                for invocation in self.invocations
                if invocation.invocation_id == action.invocation_id
            ]
            invocation_status = matching[0].status if len(matching) == 1 else None
            if action.status is ActionStatus.PROPOSED:
                valid_joint_lifecycle = not matching or (
                    len(matching) == 1
                    and invocation_status is InvocationStatus.REQUESTED
                )
            elif action.status is ActionStatus.REJECTED:
                valid_joint_lifecycle = not matching
            else:
                allowed_invocation_statuses = {
                    ActionStatus.ACCEPTED: {InvocationStatus.REQUESTED},
                    ActionStatus.STARTED: set(InvocationStatus),
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
                    ActionStatus.OUTCOME_UNKNOWN: set(InvocationStatus),
                }
                valid_joint_lifecycle = (
                    len(matching) == 1
                    and invocation_status in allowed_invocation_statuses[action.status]
                )
            if not valid_joint_lifecycle:
                raise ValueError(
                    "INVOKE_AGENT Action/Invocation lifecycle is inconsistent"
                )
        return self


class StrategyView(FrozenModel):
    attempt_id: StrictStr
    strategy_id: StrictStr
    revision: StrictInt = Field(ge=1)
    remaining_budget: tuple[tuple[StrictStr, StrictInt], ...]
    legal_topology: _FrozenJson
    actions: tuple[ActionState, ...]
    invocations: tuple[InvocationState, ...]
    visible_artifacts: tuple[ArtifactRef, ...]
    latest_committed_event: _FrozenJson

    @field_validator("attempt_id", "strategy_id")
    @classmethod
    def validate_identity(cls, value: str, info: Any) -> str:
        return _validate_non_empty(value, field_name=info.field_name)


class AgentView(FrozenModel):
    current_task: _FrozenJson
    agent_definition: _FrozenJson
    authorized_context: _FrozenJson
    artifact_refs: tuple[ArtifactRef, ...]


class OperatorView(FrozenModel):
    phase: AttemptPhase
    revision: StrictInt
    progress: tuple[InvocationState, ...]
    budget: BudgetState
    pending_external: tuple[ExternalRequest, ...]
    actions: tuple[ActionState, ...]
    failures: tuple[ErrorSummary, ...]


def to_strategy_view(
    state: AttemptState,
    *,
    legal_topology: Any,
    visible_artifacts: tuple[ArtifactRef, ...] | list[ArtifactRef],
    latest_committed_event: Any,
) -> StrategyView:
    remaining_budget = tuple(
        (resource.resource, resource.limit - resource.reserved - resource.consumed)
        for resource in state.budget.resources
    )
    return StrategyView(
        attempt_id=state.attempt_id,
        strategy_id=state.strategy_id,
        revision=state.revision,
        remaining_budget=remaining_budget,
        legal_topology=legal_topology,
        actions=state.actions,
        invocations=state.invocations,
        visible_artifacts=tuple(visible_artifacts),
        latest_committed_event=latest_committed_event,
    )


def to_agent_view(
    *,
    current_task: Any,
    agent_definition: Any,
    authorized_context: Any,
    artifact_refs: tuple[ArtifactRef, ...] | list[ArtifactRef],
) -> AgentView:
    return AgentView(
        current_task=current_task,
        agent_definition=agent_definition,
        authorized_context=authorized_context,
        artifact_refs=tuple(artifact_refs),
    )


def to_operator_view(state: AttemptState) -> OperatorView:
    failures = () if state.terminal_error is None else (state.terminal_error,)
    return OperatorView(
        phase=state.phase,
        revision=state.revision,
        progress=state.invocations,
        budget=state.budget,
        pending_external=state.pending_external,
        actions=state.actions,
        failures=failures,
    )


def canonical_state_bytes(state: AttemptState) -> bytes:
    payload = state.model_dump(mode="json", exclude_none=True)
    return json.dumps(
        payload,
        ensure_ascii=False,
        allow_nan=False,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")
