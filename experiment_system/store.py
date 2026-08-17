from __future__ import annotations

import re
from datetime import datetime, timedelta
from enum import StrEnum
from hashlib import sha256
from typing import Any, AsyncContextManager, Protocol, runtime_checkable
from uuid import UUID

from pydantic import Field, StrictBool, StrictInt, StrictStr
from pydantic import field_validator, model_validator

from .actions import NormalizedAction, StableId
from .contract import CommandResult
from .events import DomainEvent
from .state import (
    ArtifactRef,
    AttemptState,
    FrozenModel,
    canonical_state_bytes,
)


_SHA256_PATTERN = re.compile(r"^[0-9a-f]{64}$")


class RepositoryError(ValueError):
    code: str
    safe_message: str

    def __init__(self) -> None:
        super().__init__(self.safe_message)


class CommandReuseError(RepositoryError):
    code = "command_reuse"
    safe_message = "The Command identifier was already used for a different request."


class RevisionConflict(RepositoryError):
    code = "revision_conflict"
    safe_message = "The Attempt revision changed before the Command could commit."


class InvalidCommit(RepositoryError):
    code = "invalid_commit"
    safe_message = "The repository commit is inconsistent with the Attempt transition."


class StaleDeliveryClaim(InvalidCommit):
    code = "stale_delivery_claim"
    safe_message = "The Action delivery claim is no longer current."


class RepositoryOperationUncertain(RepositoryError):
    code = "repository_operation_uncertain"
    safe_message = "The repository operation did not complete reliably."


class CorruptEventStream(RepositoryError):
    code = "corrupt_event_stream"
    safe_message = "The Attempt Event stream is corrupt."


class UnsupportedRepositorySchema(RepositoryError):
    code = "unsupported_repository_schema"
    safe_message = "The repository schema version is not supported."


class OutboxActionStatus(StrEnum):
    ACCEPTED = "ACCEPTED"
    STARTED = "STARTED"


def _validate_non_empty(value: str, *, field_name: str) -> str:
    if not value:
        raise ValueError(f"{field_name} must not be empty")
    return value


def _validate_hash(value: str) -> str:
    if not _SHA256_PATTERN.fullmatch(value):
        raise ValueError("request/state hash must be a lowercase SHA-256 digest")
    return value


def _validate_utc(value: datetime) -> datetime:
    if value.tzinfo is None or value.utcoffset() != timedelta(0):
        raise ValueError("timestamp must be timezone-aware UTC")
    return value


class ArtifactRegistration(FrozenModel):
    ref: ArtifactRef


class DeliveryClaim(FrozenModel):
    attempt_id: StrictStr
    action_id: StableId
    worker_id: StrictStr
    lease_expires_at: datetime

    @field_validator("attempt_id", "worker_id")
    @classmethod
    def validate_required_text(cls, value: str, info: Any) -> str:
        return _validate_non_empty(value, field_name=info.field_name)

    @field_validator("lease_expires_at")
    @classmethod
    def validate_lease_expires_at(cls, value: datetime) -> datetime:
        return _validate_utc(value)


class CommandRecord(FrozenModel):
    command_id: UUID
    request_hash: StrictStr
    committed_revision: StrictInt = Field(ge=0)
    result: CommandResult

    @field_validator("request_hash")
    @classmethod
    def validate_request_hash(cls, value: str) -> str:
        return _validate_hash(value)

    @model_validator(mode="after")
    def validate_record(self) -> CommandRecord:
        if self.result.command_id != self.command_id:
            raise ValueError("Command record identity does not match its result")
        if self.result.revision != self.committed_revision:
            raise ValueError("Command record revision does not match its result")
        return self


class RejectedCommandRequest(FrozenModel):
    command_id: UUID
    request_hash: StrictStr
    attempt_id: StrictStr
    result: CommandResult

    @field_validator("request_hash")
    @classmethod
    def validate_request_hash(cls, value: str) -> str:
        return _validate_hash(value)

    @field_validator("attempt_id")
    @classmethod
    def validate_attempt_id(cls, value: str) -> str:
        return _validate_non_empty(value, field_name="attempt_id")

    @model_validator(mode="after")
    def validate_rejection(self) -> RejectedCommandRequest:
        if self.result.accepted:
            raise ValueError("a rejected Command record requires a rejected result")
        if self.result.command_id != self.command_id:
            raise ValueError("command_id must match the rejected Command result")
        if self.result.attempt_id != self.attempt_id:
            raise ValueError("attempt_id must match the rejected Command result")
        return self


class CommitRequest(FrozenModel):
    command_id: UUID
    request_hash: StrictStr
    attempt_id: StrictStr
    expected_revision: StrictInt = Field(ge=0)
    events: tuple[DomainEvent, ...]
    outbox_actions: tuple[NormalizedAction, ...]
    artifact_registrations: tuple[ArtifactRegistration, ...]
    completed_delivery_action_ids: tuple[StableId, ...]
    result: CommandResult
    checkpoint: StrictBool
    delivery_claim: DeliveryClaim | None = None

    @field_validator("request_hash")
    @classmethod
    def validate_request_hash(cls, value: str) -> str:
        return _validate_hash(value)

    @field_validator("attempt_id")
    @classmethod
    def validate_attempt_id(cls, value: str) -> str:
        return _validate_non_empty(value, field_name="attempt_id")

    @model_validator(mode="after")
    def validate_request_shape(self) -> CommitRequest:
        if not self.events:
            raise ValueError("a commit requires at least one Event")
        if not self.result.accepted:
            raise ValueError("only accepted Command results may be committed")
        if self.result.command_id != self.command_id:
            raise ValueError("command_id must match the Command result")
        if self.result.attempt_id != self.attempt_id:
            raise ValueError("attempt_id must match the Command result")
        if self.result.revision != self.expected_revision + len(self.events):
            raise ValueError("Command result revision must include every committed Event")
        if any(event.command_id != self.command_id for event in self.events):
            raise ValueError("every Event command_id must match the commit Command")
        if any(event.attempt_id != self.attempt_id for event in self.events):
            raise ValueError("every Event attempt_id must match the commit Attempt")

        event_ids = [event.event_id for event in self.events]
        if len(event_ids) != len(set(event_ids)):
            raise ValueError("Event ids must be unique within a commit")
        outbox_ids = [action.action_id for action in self.outbox_actions]
        if len(outbox_ids) != len(set(outbox_ids)):
            raise ValueError("outbox Action ids must be unique within a commit")
        completed_ids = self.completed_delivery_action_ids
        if len(completed_ids) != len(set(completed_ids)):
            raise ValueError("completed delivery Action ids must be unique")
        if set(completed_ids).intersection(outbox_ids):
            raise ValueError("a delivery cannot be added and completed together")
        return self


class CommitResult(FrozenModel):
    state: AttemptState
    state_hash: StrictStr
    events: tuple[DomainEvent, ...]
    latest_event_id: UUID
    command_result: CommandResult
    checkpoint_written: StrictBool

    @field_validator("state_hash")
    @classmethod
    def validate_state_hash(cls, value: str) -> str:
        return _validate_hash(value)

    @model_validator(mode="after")
    def validate_commit_result(self) -> CommitResult:
        if not self.events or self.events[-1].event_id != self.latest_event_id:
            raise ValueError("latest_event_id must identify the final committed Event")
        if self.events[-1].sequence_no != self.state.revision:
            raise ValueError("the final Event sequence must match the state revision")
        if self.command_result.attempt_id != self.state.attempt_id:
            raise ValueError("Command result Attempt does not match committed state")
        if self.command_result.revision != self.state.revision:
            raise ValueError("Command result revision does not match committed state")
        if self.command_result.phase is not self.state.phase:
            raise ValueError("Command result phase does not match committed state")
        expected_hash = sha256(canonical_state_bytes(self.state)).hexdigest()
        if self.state_hash != expected_hash:
            raise ValueError("state_hash does not match canonical Attempt state")
        return self


class LoadedAttempt(FrozenModel):
    state: AttemptState
    state_hash: StrictStr
    events: tuple[DomainEvent, ...]
    latest_event_id: UUID
    command_records: tuple[CommandRecord, ...]
    artifact_registrations: tuple[ArtifactRegistration, ...]
    checkpoint_revisions: tuple[StrictInt, ...]

    @field_validator("state_hash")
    @classmethod
    def validate_state_hash(cls, value: str) -> str:
        return _validate_hash(value)

    @field_validator("checkpoint_revisions")
    @classmethod
    def validate_checkpoint_revisions(
        cls, value: tuple[int, ...]
    ) -> tuple[int, ...]:
        if any(revision < 1 for revision in value):
            raise ValueError("checkpoint revisions must be positive")
        if tuple(sorted(set(value))) != value:
            raise ValueError("checkpoint revisions must be unique and sorted")
        return value

    @model_validator(mode="after")
    def validate_loaded_attempt(self) -> LoadedAttempt:
        if not self.events or self.events[-1].event_id != self.latest_event_id:
            raise ValueError("latest_event_id must identify the final loaded Event")
        if self.events[-1].sequence_no != self.state.revision:
            raise ValueError("the final Event sequence must match the state revision")
        if any(event.attempt_id != self.state.attempt_id for event in self.events):
            raise ValueError("loaded Events must belong to the loaded Attempt")
        expected_hash = sha256(canonical_state_bytes(self.state)).hexdigest()
        if self.state_hash != expected_hash:
            raise ValueError("state_hash does not match canonical Attempt state")
        if any(
            record.result.attempt_id != self.state.attempt_id
            for record in self.command_records
        ):
            raise ValueError("Command records must belong to the loaded Attempt")
        return self


class ClaimedAction(FrozenModel):
    attempt_id: StrictStr
    action: NormalizedAction
    accepted_sequence_no: StrictInt = Field(ge=1)
    action_status: OutboxActionStatus
    worker_id: StrictStr
    lease_expires_at: datetime

    @field_validator("attempt_id", "worker_id")
    @classmethod
    def validate_required_text(cls, value: str, info: Any) -> str:
        return _validate_non_empty(value, field_name=info.field_name)

    @field_validator("lease_expires_at")
    @classmethod
    def validate_lease_expires_at(cls, value: datetime) -> datetime:
        return _validate_utc(value)

    def delivery_claim(self) -> DeliveryClaim:
        return DeliveryClaim(
            attempt_id=self.attempt_id,
            action_id=self.action.action_id,
            worker_id=self.worker_id,
            lease_expires_at=self.lease_expires_at,
        )


@runtime_checkable
class AttemptRepository(Protocol):
    def command_scope(self, attempt_id: str) -> AsyncContextManager[None]:
        raise NotImplementedError

    async def load(self, attempt_id: str) -> LoadedAttempt | None:
        raise NotImplementedError

    async def find_command(
        self,
        attempt_id: str,
        command_id: UUID,
    ) -> CommandRecord | None:
        raise NotImplementedError

    async def record_rejection(
        self,
        request: RejectedCommandRequest,
    ) -> CommandResult:
        raise NotImplementedError

    async def commit(self, request: CommitRequest) -> CommitResult:
        raise NotImplementedError

    async def list_events(
        self,
        attempt_id: str,
        *,
        after_sequence_no: int = 0,
    ) -> tuple[DomainEvent, ...]:
        raise NotImplementedError

    async def claim_action(
        self,
        *,
        worker_id: str,
        now_utc: datetime,
        lease_seconds: float,
    ) -> ClaimedAction | None:
        raise NotImplementedError

    async def confirm_action_claim(
        self,
        *,
        claim: DeliveryClaim,
        action_status: OutboxActionStatus,
        now_utc: datetime,
    ) -> bool:
        raise NotImplementedError

    async def list_nonterminal_attempt_ids(self) -> tuple[str, ...]:
        raise NotImplementedError


__all__ = [
    "ArtifactRegistration",
    "AttemptRepository",
    "ClaimedAction",
    "CommandRecord",
    "CommandReuseError",
    "CommitRequest",
    "CommitResult",
    "CorruptEventStream",
    "DeliveryClaim",
    "InvalidCommit",
    "LoadedAttempt",
    "OutboxActionStatus",
    "RejectedCommandRequest",
    "RepositoryError",
    "RepositoryOperationUncertain",
    "RevisionConflict",
    "StaleDeliveryClaim",
    "UnsupportedRepositorySchema",
]
