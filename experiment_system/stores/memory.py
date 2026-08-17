from __future__ import annotations

import asyncio
import math
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from dataclasses import dataclass, replace
from datetime import datetime, timedelta
from hashlib import sha256
from uuid import UUID

from ..events import (
    ActionAccepted,
    ActionCancelled,
    ActionFailed,
    ActionOutcomeUnknown,
    ActionStarted,
    ActionSucceeded,
    ActionTimedOut,
    BudgetReleased,
    BudgetSettled,
    DomainEvent,
    parse_domain_event,
)
from ..contract import CommandResult
from ..reducer import replay_events
from ..state import (
    ActionStatus,
    AttemptPhase,
    AttemptState,
    TERMINAL_ACTION_STATUSES,
    canonical_state_bytes,
)
from ..store import (
    ArtifactRegistration,
    ClaimedAction,
    CommandRecord,
    CommandReuseError,
    CommitRequest,
    CommitResult,
    DeliveryClaim,
    InvalidCommit,
    LoadedAttempt,
    OutboxActionStatus,
    RejectedCommandRequest,
    RevisionConflict,
    StaleDeliveryClaim,
)
from ..actions import NormalizedAction


_TERMINAL_PHASES = frozenset(
    {
        AttemptPhase.SUCCEEDED,
        AttemptPhase.FAILED,
        AttemptPhase.CANCELLED,
        AttemptPhase.TIMED_OUT,
        AttemptPhase.INTERRUPTED,
    }
)
_TERMINAL_ACTION_EVENT_TYPES = (
    ActionSucceeded,
    ActionFailed,
    ActionTimedOut,
    ActionCancelled,
    ActionOutcomeUnknown,
)


@dataclass(frozen=True, slots=True)
class _StoredCommand:
    request_hash: str
    command_result: CommandResult
    commit_result: CommitResult | None


@dataclass(frozen=True, slots=True)
class _OutboxEntry:
    attempt_id: str
    action: NormalizedAction
    accepted_sequence_no: int
    action_status: OutboxActionStatus = OutboxActionStatus.ACCEPTED
    worker_id: str | None = None
    lease_expires_at: datetime | None = None


@dataclass(slots=True)
class _CommandGate:
    lock: asyncio.Lock
    users: int = 0


def _clone_request(request: CommitRequest) -> CommitRequest:
    return CommitRequest.model_validate_json(request.model_dump_json())


def _clone_rejected_request(
    request: RejectedCommandRequest,
) -> RejectedCommandRequest:
    return RejectedCommandRequest.model_validate_json(request.model_dump_json())


def _clone_command_result(result: CommandResult) -> CommandResult:
    return CommandResult.model_validate_json(result.model_dump_json())


def _clone_commit_result(result: CommitResult) -> CommitResult:
    return CommitResult.model_validate_json(result.model_dump_json())


def _clone_loaded_attempt(loaded: LoadedAttempt) -> LoadedAttempt:
    return LoadedAttempt.model_validate_json(loaded.model_dump_json())


def _clone_claimed_action(claimed: ClaimedAction) -> ClaimedAction:
    return ClaimedAction.model_validate_json(claimed.model_dump_json())


def _clone_events(events: tuple[DomainEvent, ...]) -> tuple[DomainEvent, ...]:
    return tuple(
        parse_domain_event(event.model_dump(mode="json")) for event in events
    )


class InMemoryAttemptRepository:
    def __init__(self) -> None:
        self._lock = asyncio.Lock()
        self._command_gates: dict[str, _CommandGate] = {}
        self._events: dict[str, tuple[DomainEvent, ...]] = {}
        self._heads: dict[str, AttemptState] = {}
        self._state_hashes: dict[str, str] = {}
        self._commands: dict[tuple[str, UUID], _StoredCommand] = {}
        self._artifacts: dict[str, tuple[ArtifactRegistration, ...]] = {}
        self._checkpoints: dict[str, dict[int, AttemptState]] = {}
        self._outbox: dict[tuple[str, str], _OutboxEntry] = {}
        self._event_owners: dict[UUID, str] = {}

    @asynccontextmanager
    async def command_scope(self, attempt_id: str) -> AsyncIterator[None]:
        self._validate_attempt_id(attempt_id)
        async with self._lock:
            gate = self._command_gates.setdefault(
                attempt_id,
                _CommandGate(lock=asyncio.Lock()),
            )
            gate.users += 1
        try:
            async with gate.lock:
                yield
        finally:
            async with self._lock:
                gate.users -= 1
                if gate.users == 0:
                    del self._command_gates[attempt_id]

    async def find_command(
        self,
        attempt_id: str,
        command_id: UUID,
    ) -> CommandRecord | None:
        self._validate_attempt_id(attempt_id)
        if not isinstance(command_id, UUID):
            raise ValueError("command_id must be a UUID")
        async with self._lock:
            stored = self._commands.get((attempt_id, command_id))
            if stored is None:
                return None
            return CommandRecord(
                command_id=command_id,
                request_hash=stored.request_hash,
                committed_revision=stored.command_result.revision,
                result=_clone_command_result(stored.command_result),
            )

    async def record_rejection(
        self,
        request: RejectedCommandRequest,
    ) -> CommandResult:
        validated = _clone_rejected_request(request)
        async with self._lock:
            command_key = (validated.attempt_id, validated.command_id)
            stored = self._commands.get(command_key)
            if stored is not None:
                if stored.request_hash != validated.request_hash:
                    raise CommandReuseError()
                return _clone_command_result(stored.command_result)

            current_state = self._heads.get(validated.attempt_id)
            current_revision = 0 if current_state is None else current_state.revision
            current_phase = None if current_state is None else current_state.phase
            if validated.result.revision != current_revision:
                raise RevisionConflict()
            if validated.result.phase is not current_phase:
                raise InvalidCommit()

            next_commands = dict(self._commands)
            next_commands[command_key] = _StoredCommand(
                request_hash=validated.request_hash,
                command_result=validated.result,
                commit_result=None,
            )
            self._commands = next_commands
            return _clone_command_result(validated.result)

    async def load(self, attempt_id: str) -> LoadedAttempt | None:
        self._validate_attempt_id(attempt_id)
        async with self._lock:
            state = self._heads.get(attempt_id)
            if state is None:
                return None
            events = self._events[attempt_id]
            command_records = tuple(
                sorted(
                    (
                        CommandRecord(
                            command_id=command_id,
                            request_hash=stored.request_hash,
                            committed_revision=stored.command_result.revision,
                            result=stored.command_result,
                        )
                        for (record_attempt_id, command_id), stored in self._commands.items()
                        if record_attempt_id == attempt_id
                    ),
                    key=lambda record: (record.committed_revision, record.command_id.int),
                )
            )
            loaded = LoadedAttempt(
                state=state,
                state_hash=self._state_hashes[attempt_id],
                events=events,
                latest_event_id=events[-1].event_id,
                command_records=command_records,
                artifact_registrations=self._artifacts.get(attempt_id, ()),
                checkpoint_revisions=tuple(
                    sorted(self._checkpoints.get(attempt_id, {}))
                ),
            )
            return _clone_loaded_attempt(loaded)

    async def commit(self, request: CommitRequest) -> CommitResult:
        validated = _clone_request(request)
        async with self._lock:
            command_key = (validated.attempt_id, validated.command_id)
            stored_command = self._commands.get(command_key)
            if stored_command is not None:
                if stored_command.request_hash != validated.request_hash:
                    raise CommandReuseError()
                if stored_command.commit_result is None:
                    raise InvalidCommit()
                return _clone_commit_result(stored_command.commit_result)

            current_state = self._heads.get(validated.attempt_id)
            current_revision = 0 if current_state is None else current_state.revision
            if validated.expected_revision != current_revision:
                raise RevisionConflict()

            prior_events = self._events.get(validated.attempt_id, ())
            candidate_events = prior_events + validated.events
            candidate_state = replay_events(candidate_events)
            candidate_hash = sha256(
                canonical_state_bytes(candidate_state)
            ).hexdigest()
            self._validate_projection(validated, candidate_state)
            self._validate_new_event_ids(validated.events)

            next_outbox = dict(self._outbox)
            self._apply_outbox_changes(
                next_outbox,
                request=validated,
                candidate_state=candidate_state,
            )
            next_artifacts = dict(self._artifacts)
            next_artifacts[validated.attempt_id] = self._merge_artifacts(
                next_artifacts.get(validated.attempt_id, ()),
                validated.artifact_registrations,
            )
            next_checkpoints = {
                attempt_id: dict(checkpoints)
                for attempt_id, checkpoints in self._checkpoints.items()
            }
            if validated.checkpoint:
                next_checkpoints.setdefault(validated.attempt_id, {})[
                    candidate_state.revision
                ] = candidate_state

            committed = CommitResult(
                state=candidate_state,
                state_hash=candidate_hash,
                events=candidate_events,
                latest_event_id=candidate_events[-1].event_id,
                command_result=validated.result,
                checkpoint_written=validated.checkpoint,
            )
            next_commands = dict(self._commands)
            next_commands[command_key] = _StoredCommand(
                request_hash=validated.request_hash,
                command_result=committed.command_result,
                commit_result=committed,
            )
            next_event_owners = dict(self._event_owners)
            next_event_owners.update(
                {
                    event.event_id: validated.attempt_id
                    for event in validated.events
                }
            )
            next_events = dict(self._events)
            next_events[validated.attempt_id] = candidate_events
            next_heads = dict(self._heads)
            next_heads[validated.attempt_id] = candidate_state
            next_hashes = dict(self._state_hashes)
            next_hashes[validated.attempt_id] = candidate_hash

            self._events = next_events
            self._heads = next_heads
            self._state_hashes = next_hashes
            self._commands = next_commands
            self._artifacts = next_artifacts
            self._checkpoints = next_checkpoints
            self._outbox = next_outbox
            self._event_owners = next_event_owners
            return _clone_commit_result(committed)

    async def list_events(
        self,
        attempt_id: str,
        *,
        after_sequence_no: int = 0,
    ) -> tuple[DomainEvent, ...]:
        self._validate_attempt_id(attempt_id)
        if type(after_sequence_no) is not int or after_sequence_no < 0:
            raise ValueError("after_sequence_no must be a nonnegative integer")
        async with self._lock:
            selected = tuple(
                event
                for event in self._events.get(attempt_id, ())
                if event.sequence_no > after_sequence_no
            )
            return _clone_events(selected)

    async def claim_action(
        self,
        *,
        worker_id: str,
        now_utc: datetime,
        lease_seconds: float,
    ) -> ClaimedAction | None:
        if type(worker_id) is not str or not worker_id:
            raise ValueError("worker_id must be a nonempty string")
        if (
            not isinstance(now_utc, datetime)
            or now_utc.tzinfo is None
            or now_utc.utcoffset() != timedelta(0)
        ):
            raise ValueError("now_utc must be timezone-aware UTC")
        if (
            isinstance(lease_seconds, bool)
            or not isinstance(lease_seconds, (int, float))
            or not math.isfinite(lease_seconds)
            or lease_seconds <= 0
        ):
            raise ValueError("lease_seconds must be a positive finite number")

        async with self._lock:
            eligible = sorted(
                (
                    entry
                    for entry in self._outbox.values()
                    if self._is_dispatchable(entry)
                    and (
                        entry.lease_expires_at is None
                        or entry.lease_expires_at <= now_utc
                    )
                ),
                key=lambda entry: (
                    entry.accepted_sequence_no,
                    entry.action.action_id,
                    entry.attempt_id,
                ),
            )
            if not eligible:
                return None
            current = eligible[0]
            claimed_entry = replace(
                current,
                worker_id=worker_id,
                lease_expires_at=now_utc + timedelta(seconds=lease_seconds),
            )
            self._outbox[(current.attempt_id, current.action.action_id)] = claimed_entry
            claimed = ClaimedAction(
                attempt_id=current.attempt_id,
                action=current.action,
                accepted_sequence_no=current.accepted_sequence_no,
                action_status=current.action_status,
                worker_id=worker_id,
                lease_expires_at=claimed_entry.lease_expires_at,
            )
            return _clone_claimed_action(claimed)

    async def confirm_action_claim(
        self,
        *,
        claim: DeliveryClaim,
        action_status: OutboxActionStatus,
        now_utc: datetime,
    ) -> bool:
        validated = DeliveryClaim.model_validate_json(claim.model_dump_json())
        if not isinstance(action_status, OutboxActionStatus):
            raise ValueError("action_status must be an OutboxActionStatus")
        if (
            not isinstance(now_utc, datetime)
            or now_utc.tzinfo is None
            or now_utc.utcoffset() != timedelta(0)
        ):
            raise ValueError("now_utc must be timezone-aware UTC")
        async with self._lock:
            entry = self._outbox.get(
                (validated.attempt_id, validated.action_id)
            )
            return (
                entry is not None
                and entry.action_status is action_status
                and entry.worker_id == validated.worker_id
                and entry.lease_expires_at == validated.lease_expires_at
                and now_utc < validated.lease_expires_at
            )

    def _is_dispatchable(self, entry: _OutboxEntry) -> bool:
        state = self._heads[entry.attempt_id]
        action = next(
            (
                candidate
                for candidate in state.actions
                if candidate.action_id == entry.action.action_id
            ),
            None,
        )
        if action is None or action.reservation_id != entry.action.reservation_id:
            raise InvalidCommit()
        expected_status = ActionStatus(entry.action_status.value)
        if action.status is not expected_status:
            raise InvalidCommit()
        if (
            entry.action_status is OutboxActionStatus.ACCEPTED
            and state.phase is not AttemptPhase.RUNNING
        ):
            return False
        return not any(
            isinstance(event, (BudgetSettled, BudgetReleased))
            and event.reservation.reservation_id == action.reservation_id
            for event in self._events[entry.attempt_id]
        )

    async def list_nonterminal_attempt_ids(self) -> tuple[str, ...]:
        async with self._lock:
            return tuple(
                sorted(
                    attempt_id
                    for attempt_id, state in self._heads.items()
                    if state.phase not in _TERMINAL_PHASES
                )
            )

    @staticmethod
    def _validate_attempt_id(attempt_id: str) -> None:
        if type(attempt_id) is not str or not attempt_id:
            raise ValueError("attempt_id must be a nonempty string")

    @staticmethod
    def _validate_projection(
        request: CommitRequest,
        candidate_state: AttemptState,
    ) -> None:
        if candidate_state.attempt_id != request.attempt_id:
            raise InvalidCommit()
        if candidate_state.revision != request.result.revision:
            raise InvalidCommit()
        if candidate_state.phase is not request.result.phase:
            raise InvalidCommit()

    def _validate_new_event_ids(self, events: tuple[DomainEvent, ...]) -> None:
        if any(event.event_id in self._event_owners for event in events):
            raise InvalidCommit()

    @staticmethod
    def _apply_outbox_changes(
        outbox: dict[tuple[str, str], _OutboxEntry],
        *,
        request: CommitRequest,
        candidate_state: AttemptState,
    ) -> None:
        accepted_events = tuple(
            event for event in request.events if isinstance(event, ActionAccepted)
        )
        accepted_action_ids = tuple(
            event.action.action_id for event in accepted_events
        )
        outbox_action_ids = tuple(
            action.action_id for action in request.outbox_actions
        )
        if (
            len(accepted_action_ids) != len(set(accepted_action_ids))
            or len(outbox_action_ids) != len(set(outbox_action_ids))
            or set(accepted_action_ids) != set(outbox_action_ids)
        ):
            raise InvalidCommit()

        for action in request.outbox_actions:
            key = (request.attempt_id, action.action_id)
            if key in outbox:
                raise InvalidCommit()
            matching_accepted_events = [
                event
                for event in accepted_events
                if event.action.action_id == action.action_id
                and event.action == action
            ]
            state_actions = [
                state_action
                for state_action in candidate_state.actions
                if state_action.action_id == action.action_id
                and state_action.status is ActionStatus.ACCEPTED
            ]
            if len(matching_accepted_events) != 1 or len(state_actions) != 1:
                raise InvalidCommit()
            outbox[key] = _OutboxEntry(
                attempt_id=request.attempt_id,
                action=action,
                accepted_sequence_no=matching_accepted_events[0].sequence_no,
            )

        started_events = tuple(
            event for event in request.events if isinstance(event, ActionStarted)
        )
        if not started_events and request.delivery_claim is not None:
            raise InvalidCommit()
        for event in started_events:
            key = (request.attempt_id, event.action_id)
            entry = outbox.get(key)
            claim = request.delivery_claim
            state_action = next(
                (
                    action
                    for action in candidate_state.actions
                    if action.action_id == event.action_id
                ),
                None,
            )
            if (
                entry is None
                or claim is None
                or claim.attempt_id != request.attempt_id
                or claim.action_id != event.action_id
                or claim.worker_id != entry.worker_id
                or claim.lease_expires_at != entry.lease_expires_at
                or event.wall_time_utc >= claim.lease_expires_at
                or entry.action_status is not OutboxActionStatus.ACCEPTED
                or entry.worker_id is None
                or entry.lease_expires_at is None
                or state_action is None
                or (
                    state_action.status is not ActionStatus.STARTED
                    and state_action.status not in TERMINAL_ACTION_STATUSES
                )
            ):
                raise StaleDeliveryClaim()
            outbox[key] = replace(
                entry,
                action_status=OutboxActionStatus.STARTED,
            )

        completed_action_ids = request.completed_delivery_action_ids
        terminal_events = tuple(
            event
            for event in request.events
            if isinstance(event, _TERMINAL_ACTION_EVENT_TYPES)
            and (request.attempt_id, event.action_id) in outbox
        )
        if tuple(event.action_id for event in terminal_events) != completed_action_ids:
            raise InvalidCommit()
        completed_keys: list[tuple[str, str]] = []
        for event in terminal_events:
            key = (request.attempt_id, event.action_id)
            entry = outbox.get(key)
            if entry is None:
                raise InvalidCommit()
            predispatch_cancellation = (
                isinstance(event, ActionCancelled)
                and entry.action_status is OutboxActionStatus.ACCEPTED
            )
            if (
                entry.action_status is not OutboxActionStatus.STARTED
                and not predispatch_cancellation
            ):
                raise InvalidCommit()
            completed_keys.append(key)
        for key in completed_keys:
            del outbox[key]

        if candidate_state.phase in _TERMINAL_PHASES and any(
            attempt_id == request.attempt_id for attempt_id, _ in outbox
        ):
            raise InvalidCommit()

    @staticmethod
    def _merge_artifacts(
        existing: tuple[ArtifactRegistration, ...],
        additions: tuple[ArtifactRegistration, ...],
    ) -> tuple[ArtifactRegistration, ...]:
        merged = list(existing)
        for registration in additions:
            if registration not in merged:
                merged.append(registration)
        return tuple(merged)


__all__ = ["InMemoryAttemptRepository"]
