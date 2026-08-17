from __future__ import annotations

import asyncio
import json
from collections.abc import Callable
from datetime import datetime, timedelta, timezone
from hashlib import sha256
from itertools import count
from pathlib import Path
from uuid import UUID

import pytest
from pydantic import ValidationError

from experiment_system.actions import (
    ActionCancelledOutcome,
    ActionProposal,
    ActionSucceededOutcome,
    ActionType,
    NormalizedAction,
)
from experiment_system.commands import ReportActionStarted, action_command_id
from experiment_system.contract import CommandResult
from experiment_system.events import (
    ActionAccepted,
    ActionCancelled,
    ActionProposed,
    ActionStarted,
    ActionSucceeded,
    AttemptPlanned,
    AttemptStarted,
    AttemptSucceeded,
    BudgetReleased,
    BudgetReservationEntry,
    BudgetReserved,
    BudgetSettled,
    InvocationCompleted,
    InvocationRequested,
    InvocationStarted,
)
from experiment_system.reducer import StateTransitionError
from experiment_system.state import (
    ArtifactRef,
    ActionStatus,
    AttemptPhase,
    BudgetState,
    ErrorSummary,
    RecoveryPolicy,
    ResourceBudget,
    ResourceKind,
    ResourceRequest,
    StrategyStateEnvelope,
    canonical_state_bytes,
)
from experiment_system.store import (
    ArtifactRegistration,
    AttemptRepository,
    CommandReuseError,
    CommitRequest,
    CommitResult,
    InvalidCommit,
    OutboxActionStatus,
    RejectedCommandRequest,
    RevisionConflict,
)
from experiment_system.stores.memory import InMemoryAttemptRepository
from experiment_system.stores.sqlite import SQLiteAttemptRepository
from tests.test_experiment_engine import (
    _create_command as _engine_create_command,
    _decision_command as _engine_decision_command,
    _engine as _attempt_engine,
    _proposal as _engine_proposal,
    _start_command as _engine_start_command,
)


UTC_NOW = datetime(2026, 7, 23, 12, 0, tzinfo=timezone.utc)


def _uuid(value: int) -> UUID:
    return UUID(int=value)


def _request_hash(label: str) -> str:
    return sha256(label.encode("ascii")).hexdigest()


def _artifact(attempt_id: str, name: str) -> ArtifactRef:
    return ArtifactRef(
        capture_class="full",
        content_hash=sha256(f"{attempt_id}:{name}".encode("ascii")).hexdigest(),
        media_type="application/json",
        byte_size=12,
        relative_path=f"attempts/{attempt_id}/{name}.json",
    )


def _strategy() -> StrategyStateEnvelope:
    value = {"stage": "READY"}
    payload = json.dumps(
        value,
        ensure_ascii=False,
        allow_nan=False,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")
    return StrategyStateEnvelope(
        strategy_id="router",
        strategy_schema_version=1,
        value=value,
        content_hash=sha256(payload).hexdigest(),
        byte_size=len(payload),
    )


def _budget(
    *,
    reserved_model_calls: int = 0,
    consumed_model_calls: int = 0,
) -> BudgetState:
    return BudgetState(
        resources=tuple(
            ResourceBudget(
                resource=kind.value,
                limit=10,
                reserved=(
                    reserved_model_calls
                    if kind is ResourceKind.MODEL_CALLS
                    else 0
                ),
                consumed=(
                    consumed_model_calls
                    if kind is ResourceKind.MODEL_CALLS
                    else 0
                ),
            )
            for kind in ResourceKind
        ),
        deadline_at=UTC_NOW + timedelta(hours=1),
        max_call_depth=4,
        max_concurrent_actions=2,
    )


def _envelope(
    *,
    attempt_id: str,
    sequence_no: int,
    command_id: UUID,
    event_id: UUID,
    causal_parent_id: UUID | None,
) -> dict[str, object]:
    return {
        "schema_version": 1,
        "event_id": event_id,
        "trial_id": f"trial-{attempt_id}",
        "attempt_id": attempt_id,
        "sequence_no": sequence_no,
        "command_id": command_id,
        "causal_parent_id": causal_parent_id,
        "logical_time": sequence_no,
        "wall_time_utc": UTC_NOW + timedelta(seconds=sequence_no),
    }


def _create_request(
    *,
    attempt_id: str = "attempt-1",
    seed: int = 100,
    request_hash: str | None = None,
) -> CommitRequest:
    command_id = _uuid(seed)
    manifest_ref = _artifact(attempt_id, "manifest")
    event = AttemptPlanned(
        **_envelope(
            attempt_id=attempt_id,
            sequence_no=1,
            command_id=command_id,
            event_id=_uuid(seed + 1),
            causal_parent_id=None,
        ),
        event_type="ATTEMPT_PLANNED",
        state_schema_version=1,
        experiment_id="experiment-1",
        strategy_id="router",
        manifest_ref=manifest_ref,
        strategy=_strategy(),
        budget=_budget(),
    )
    result = CommandResult(
        command_id=command_id,
        attempt_id=attempt_id,
        accepted=True,
        revision=1,
        phase=AttemptPhase.PLANNED,
    )
    return CommitRequest(
        command_id=command_id,
        request_hash=request_hash or _request_hash(f"create:{attempt_id}"),
        attempt_id=attempt_id,
        expected_revision=0,
        events=(event,),
        outbox_actions=(),
        artifact_registrations=(ArtifactRegistration(ref=manifest_ref),),
        completed_delivery_action_ids=(),
        result=result,
        checkpoint=False,
    )


def _start_request(
    created: CommitResult,
    *,
    seed: int = 200,
) -> CommitRequest:
    command_id = _uuid(seed)
    event = AttemptStarted(
        **_envelope(
            attempt_id=created.state.attempt_id,
            sequence_no=2,
            command_id=command_id,
            event_id=_uuid(seed + 1),
            causal_parent_id=created.latest_event_id,
        ),
        event_type="ATTEMPT_STARTED",
    )
    return CommitRequest(
        command_id=command_id,
        request_hash=_request_hash(f"start:{created.state.attempt_id}:{seed}"),
        attempt_id=created.state.attempt_id,
        expected_revision=1,
        events=(event,),
        outbox_actions=(),
        artifact_registrations=(),
        completed_delivery_action_ids=(),
        result=CommandResult(
            command_id=command_id,
            attempt_id=created.state.attempt_id,
            accepted=True,
            revision=2,
            phase=AttemptPhase.RUNNING,
        ),
        checkpoint=False,
    )


def _accepted_action_request(
    started: CommitResult,
    *,
    seed: int = 300,
) -> tuple[CommitRequest, NormalizedAction]:
    attempt_id = started.state.attempt_id
    command_id = _uuid(seed)
    proposed_event_id = _uuid(seed + 1)
    invocation_event_id = _uuid(seed + 2)
    reserved_event_id = _uuid(seed + 3)
    accepted_event_id = _uuid(seed + 4)
    payload_ref = _artifact(attempt_id, "action-1-payload")
    proposal = ActionProposal(
        action_id="action-1",
        action_type=ActionType.INVOKE_AGENT,
        actor="engine",
        target_ids=("worker-a",),
        invocation_id="invocation-1",
        causal_parent_id=str(started.latest_event_id),
        payload_ref=payload_ref,
        recovery_policy=RecoveryPolicy.REPLAY_SAFE,
        requested_timeout=30,
        call_depth=0,
        resource_requests=(
            ResourceRequest(resource=ResourceKind.MODEL_CALLS, amount=1),
        ),
    )
    normalized = NormalizedAction(
        **proposal.model_dump(mode="python"),
        reservation_id="reservation-1",
        idempotency_key="idem-action-1",
    )
    events = (
        ActionProposed(
            **_envelope(
                attempt_id=attempt_id,
                sequence_no=3,
                command_id=command_id,
                event_id=proposed_event_id,
                causal_parent_id=started.latest_event_id,
            ),
            event_type="ACTION_PROPOSED",
            proposal=proposal,
        ),
        InvocationRequested(
            **_envelope(
                attempt_id=attempt_id,
                sequence_no=4,
                command_id=command_id,
                event_id=invocation_event_id,
                causal_parent_id=proposed_event_id,
            ),
            event_type="INVOCATION_REQUESTED",
            action_id="action-1",
            invocation_id="invocation-1",
            agent_id="worker-a",
            conversation_id="conversation-1",
            parent_invocation_id=None,
            latest_context_ref=None,
        ),
        BudgetReserved(
            **_envelope(
                attempt_id=attempt_id,
                sequence_no=5,
                command_id=command_id,
                event_id=reserved_event_id,
                causal_parent_id=invocation_event_id,
            ),
            event_type="BUDGET_RESERVED",
            budget=_budget(reserved_model_calls=1),
            reservations=(
                BudgetReservationEntry(
                    action_id=normalized.action_id,
                    reservation_id=normalized.reservation_id,
                    resource_requests=normalized.resource_requests,
                ),
            ),
        ),
        ActionAccepted(
            **_envelope(
                attempt_id=attempt_id,
                sequence_no=6,
                command_id=command_id,
                event_id=accepted_event_id,
                causal_parent_id=reserved_event_id,
            ),
            event_type="ACTION_ACCEPTED",
            action=normalized,
        ),
    )
    request = CommitRequest(
        command_id=command_id,
        request_hash=_request_hash(f"accept:{attempt_id}:{seed}"),
        attempt_id=attempt_id,
        expected_revision=2,
        events=events,
        outbox_actions=(normalized,),
        artifact_registrations=(ArtifactRegistration(ref=payload_ref),),
        completed_delivery_action_ids=(),
        result=CommandResult(
            command_id=command_id,
            attempt_id=attempt_id,
            accepted=True,
            revision=6,
            phase=AttemptPhase.RUNNING,
        ),
        checkpoint=False,
    )
    return request, normalized


def _finish_request(
    started: CommitResult,
    *,
    seed: int = 400,
) -> CommitRequest:
    attempt_id = started.state.attempt_id
    command_id = _uuid(seed)
    result_ref = _artifact(attempt_id, "result")
    event = AttemptSucceeded(
        **_envelope(
            attempt_id=attempt_id,
            sequence_no=3,
            command_id=command_id,
            event_id=_uuid(seed + 1),
            causal_parent_id=started.latest_event_id,
        ),
        event_type="ATTEMPT_SUCCEEDED",
        result_ref=result_ref,
    )
    return CommitRequest(
        command_id=command_id,
        request_hash=_request_hash(f"finish:{attempt_id}:{seed}"),
        attempt_id=attempt_id,
        expected_revision=2,
        events=(event,),
        outbox_actions=(),
        artifact_registrations=(ArtifactRegistration(ref=result_ref),),
        completed_delivery_action_ids=(),
        result=CommandResult(
            command_id=command_id,
            attempt_id=attempt_id,
            accepted=True,
            revision=3,
            phase=AttemptPhase.SUCCEEDED,
        ),
        checkpoint=True,
    )


@pytest.fixture(params=("memory", "sqlite"))
def repository_factory(
    request: pytest.FixtureRequest,
    tmp_path: Path,
) -> Callable[[], AttemptRepository]:
    if request.param == "memory":
        return InMemoryAttemptRepository
    database_counter = count()
    return lambda: SQLiteAttemptRepository(
        tmp_path / f"repository-{next(database_counter)}.sqlite3"
    )


def test_commit_request_rejects_duplicate_completed_delivery_action_ids() -> None:
    payload = _create_request().model_dump(mode="python")
    payload["completed_delivery_action_ids"] = ("action-1", "action-1")

    with pytest.raises(ValidationError, match="unique"):
        CommitRequest.model_validate(payload)


@pytest.mark.asyncio
async def test_commit_request_forbids_adding_and_completing_same_delivery() -> None:
    repository = InMemoryAttemptRepository()
    created = await repository.commit(_create_request())
    started = await repository.commit(_start_request(created))
    request, normalized = _accepted_action_request(started)
    payload = request.model_dump(mode="python")
    payload["completed_delivery_action_ids"] = (normalized.action_id,)

    with pytest.raises(ValidationError, match="added and completed"):
        CommitRequest.model_validate(payload)


@pytest.mark.asyncio
async def test_create_duplicate_load_and_command_lookup(
    repository_factory: Callable[[], AttemptRepository],
) -> None:
    repository = repository_factory()
    request = _create_request()

    created = await repository.commit(request)
    duplicate = await repository.commit(request)
    loaded = await repository.load("attempt-1")

    assert created.state.revision == 1
    assert created.state.phase is AttemptPhase.PLANNED
    assert created.state_hash == sha256(canonical_state_bytes(created.state)).hexdigest()
    assert created.latest_event_id == request.events[-1].event_id
    assert duplicate == created
    assert duplicate is not created
    assert loaded is not None
    assert loaded.state_hash == created.state_hash
    assert loaded.events[-1].sequence_no == loaded.state.revision
    assert loaded.latest_event_id == created.latest_event_id
    assert len(loaded.command_records) == 1
    assert loaded.command_records[0].command_id == request.command_id
    assert loaded.command_records[0].request_hash == request.request_hash
    assert loaded.command_records[0].result == request.result
    assert loaded.artifact_registrations == request.artifact_registrations


@pytest.mark.asyncio
async def test_command_reuse_is_checked_before_revision_and_appends_nothing(
    repository_factory: Callable[[], AttemptRepository],
) -> None:
    repository = repository_factory()
    request = _create_request()
    original = await repository.commit(request)
    conflict = request.model_copy(update={"request_hash": _request_hash("other-payload")})

    with pytest.raises(CommandReuseError) as caught:
        await repository.commit(conflict)

    assert caught.value.code == "command_reuse"
    assert str(caught.value) == caught.value.safe_message
    assert await repository.list_events("attempt-1") == original.events
    loaded = await repository.load("attempt-1")
    assert loaded is not None
    assert len(loaded.command_records) == 1
    assert await repository.commit(request) == original


@pytest.mark.asyncio
async def test_stale_revision_has_no_events_or_command_record(
    repository_factory: Callable[[], AttemptRepository],
) -> None:
    repository = repository_factory()
    created = await repository.commit(_create_request())
    started = await repository.commit(_start_request(created))
    stale = _create_request(seed=500, request_hash=_request_hash("stale"))

    with pytest.raises(RevisionConflict) as caught:
        await repository.commit(stale)

    assert caught.value.code == "revision_conflict"
    assert str(caught.value) == caught.value.safe_message
    loaded = await repository.load("attempt-1")
    assert loaded is not None
    assert loaded.state.revision == 2
    assert loaded.events == started.events
    assert {record.command_id for record in loaded.command_records} == {
        created.command_result.command_id,
        started.command_result.command_id,
    }


@pytest.mark.asyncio
async def test_concurrent_same_revision_commits_have_exactly_one_winner(
    repository_factory: Callable[[], AttemptRepository],
) -> None:
    repository = repository_factory()
    created = await repository.commit(_create_request())
    first = _start_request(created, seed=600)
    second = _start_request(created, seed=700)

    outcomes = await asyncio.gather(
        repository.commit(first),
        repository.commit(second),
        return_exceptions=True,
    )

    assert sum(isinstance(outcome, CommitResult) for outcome in outcomes) == 1
    assert sum(isinstance(outcome, RevisionConflict) for outcome in outcomes) == 1
    loaded = await repository.load("attempt-1")
    assert loaded is not None
    assert loaded.state.revision == 2
    assert len(loaded.events) == 2
    assert len(loaded.command_records) == 2


@pytest.mark.parametrize(
    ("attempt_ids", "expected_peak"),
    [
        pytest.param(("attempt-1", "attempt-1"), 1, id="same-attempt"),
        pytest.param(("attempt-1", "attempt-2"), 2, id="different-attempts"),
    ],
)
@pytest.mark.asyncio
async def test_command_scope_serializes_only_the_same_attempt(
    repository_factory: Callable[[], AttemptRepository],
    attempt_ids: tuple[str, str],
    expected_peak: int,
) -> None:
    repository = repository_factory()
    active = 0
    peak = 0

    async def observe(attempt_id: str) -> None:
        nonlocal active, peak
        async with repository.command_scope(attempt_id):
            active += 1
            peak = max(peak, active)
            await asyncio.sleep(0)
            active -= 1

    await asyncio.gather(*(observe(attempt_id) for attempt_id in attempt_ids))

    assert peak == expected_peak


@pytest.mark.asyncio
async def test_rejected_command_ledger_is_durable_without_events(
    repository_factory: Callable[[], AttemptRepository],
) -> None:
    repository = repository_factory()
    command_id = _uuid(1400)
    result = CommandResult(
        command_id=command_id,
        attempt_id="attempt-1",
        accepted=False,
        revision=0,
        phase=None,
        error=ErrorSummary(
            code="ATTEMPT_NOT_FOUND",
            retryable=False,
            safe_message="The Attempt does not exist.",
        ),
    )
    request = RejectedCommandRequest(
        command_id=command_id,
        request_hash=_request_hash("missing-attempt"),
        attempt_id="attempt-1",
        result=result,
    )

    first = await repository.record_rejection(request)
    duplicate = await repository.record_rejection(request)
    record = await repository.find_command("attempt-1", command_id)

    assert first == result
    assert duplicate == first
    assert record is not None
    assert record.command_id == command_id
    assert record.request_hash == request.request_hash
    assert record.committed_revision == 0
    assert record.result == result
    assert await repository.load("attempt-1") is None
    assert await repository.list_events("attempt-1") == ()

    object.__setattr__(first, "revision", 999)
    object.__setattr__(record.result, "revision", 999)
    persisted_record = await repository.find_command("attempt-1", command_id)
    assert persisted_record is not None
    assert persisted_record.result.revision == 0

    with pytest.raises(CommandReuseError):
        await repository.record_rejection(
            request.model_copy(
                update={"request_hash": _request_hash("different-payload")}
            )
        )

    create = _create_request()
    conflicting_event = create.events[0].model_copy(
        update={"command_id": command_id}
    )
    conflicting_commit = create.model_copy(
        update={
            "command_id": command_id,
            "events": (conflicting_event,),
            "result": create.result.model_copy(update={"command_id": command_id}),
        }
    )
    with pytest.raises(CommandReuseError):
        await repository.commit(conflicting_commit)

    await repository.commit(create)
    loaded = await repository.load("attempt-1")
    assert loaded is not None
    assert tuple(item.command_id for item in loaded.command_records) == (
        command_id,
        create.command_id,
    )


@pytest.mark.parametrize(
    "resource_requests",
    [
        pytest.param(
            (ResourceRequest(resource=ResourceKind.MODEL_CALLS, amount=1),),
            id="resource-bearing",
        ),
        pytest.param((), id="empty-reservation"),
    ],
)
@pytest.mark.asyncio
async def test_resource_action_without_reservation_rolls_back_every_projection(
    repository_factory: Callable[[], AttemptRepository],
    resource_requests: tuple[ResourceRequest, ...],
) -> None:
    repository = repository_factory()
    created = await repository.commit(_create_request())
    started = await repository.commit(_start_request(created))
    reserved_request, _ = _accepted_action_request(started)
    proposed, invocation_requested, _, accepted = reserved_request.events
    proposed = proposed.model_copy(
        update={
            "proposal": proposed.proposal.model_copy(
                update={"resource_requests": resource_requests}
            )
        }
    )
    accepted = accepted.model_copy(
        update={
            "action": accepted.action.model_copy(
                update={"resource_requests": resource_requests}
            )
        }
    )
    unreserved_accepted = accepted.model_copy(
        update={
            "sequence_no": 5,
            "logical_time": 5,
            "causal_parent_id": invocation_requested.event_id,
        }
    )
    unreserved_request = reserved_request.model_copy(
        update={
            "events": (proposed, invocation_requested, unreserved_accepted),
            "outbox_actions": (unreserved_accepted.action,),
            "result": reserved_request.result.model_copy(update={"revision": 5}),
        }
    )

    with pytest.raises(StateTransitionError) as caught:
        await repository.commit(unreserved_request)
    assert caught.value.code == "invalid_budget_transition"

    loaded = await repository.load("attempt-1")
    assert loaded is not None
    assert loaded.events == started.events
    assert reserved_request.command_id not in {
        record.command_id for record in loaded.command_records
    }
    assert reserved_request.artifact_registrations[0] not in (
        loaded.artifact_registrations
    )
    assert (
        await repository.claim_action(
            worker_id="worker-a",
            now_utc=UTC_NOW,
            lease_seconds=10,
        )
        is None
    )


@pytest.mark.asyncio
async def test_budget_reservation_without_acceptance_rolls_back_every_projection(
    repository_factory: Callable[[], AttemptRepository],
) -> None:
    repository = repository_factory()
    created = await repository.commit(_create_request())
    started = await repository.commit(_start_request(created))
    accepted_request, _ = _accepted_action_request(started)
    proposed, invocation_requested, reserved, _ = accepted_request.events
    half_transaction = accepted_request.model_copy(
        update={
            "events": (proposed, invocation_requested, reserved),
            "outbox_actions": (),
            "result": accepted_request.result.model_copy(update={"revision": 5}),
        }
    )

    with pytest.raises(StateTransitionError) as caught:
        await repository.commit(half_transaction)
    assert caught.value.code == "invalid_budget_transition"

    loaded = await repository.load("attempt-1")
    assert loaded is not None
    assert loaded.events == started.events
    assert accepted_request.command_id not in {
        record.command_id for record in loaded.command_records
    }
    assert loaded.state.budget == _budget()
    assert (
        await repository.claim_action(
            worker_id="worker-a",
            now_utc=UTC_NOW,
            lease_seconds=10,
        )
        is None
    )


@pytest.mark.asyncio
async def test_accepted_action_without_outbox_rolls_back_every_projection(
    repository_factory: Callable[[], AttemptRepository],
) -> None:
    repository = repository_factory()
    created = await repository.commit(_create_request())
    started = await repository.commit(_start_request(created))
    accepted_request, _ = _accepted_action_request(started)
    missing_outbox = accepted_request.model_copy(update={"outbox_actions": ()})

    with pytest.raises(InvalidCommit):
        await repository.commit(missing_outbox)

    loaded = await repository.load("attempt-1")
    assert loaded is not None
    assert loaded.events == started.events
    assert accepted_request.command_id not in {
        record.command_id for record in loaded.command_records
    }
    assert accepted_request.artifact_registrations[0] not in (
        loaded.artifact_registrations
    )
    assert (
        await repository.claim_action(
            worker_id="worker-a",
            now_utc=UTC_NOW,
            lease_seconds=10,
        )
        is None
    )

@pytest.mark.asyncio
async def test_invalid_event_batch_rolls_back_ledger_outbox_and_artifacts(
    repository_factory: Callable[[], AttemptRepository],
) -> None:
    repository = repository_factory()
    created = await repository.commit(_create_request())
    started = await repository.commit(_start_request(created))
    action_request, _ = _accepted_action_request(started)
    accepted = await repository.commit(action_request)
    command_id = _uuid(800)
    invalid_event = ActionStarted(
        **_envelope(
            attempt_id="attempt-1",
            sequence_no=8,
            command_id=command_id,
            event_id=_uuid(801),
            causal_parent_id=accepted.latest_event_id,
        ),
        event_type="ACTION_STARTED",
        action_id="action-1",
    )
    extra_ref = _artifact("attempt-1", "should-not-commit")
    invalid_request = CommitRequest(
        command_id=command_id,
        request_hash=_request_hash("invalid-completion"),
        attempt_id="attempt-1",
        expected_revision=6,
        events=(invalid_event,),
        outbox_actions=(),
        artifact_registrations=(ArtifactRegistration(ref=extra_ref),),
        completed_delivery_action_ids=("action-1",),
        result=CommandResult(
            command_id=command_id,
            attempt_id="attempt-1",
            accepted=True,
            revision=7,
            phase=AttemptPhase.RUNNING,
        ),
        checkpoint=True,
    )

    with pytest.raises(StateTransitionError, match="sequence"):
        await repository.commit(invalid_request)

    loaded = await repository.load("attempt-1")
    assert loaded is not None
    assert loaded.events == accepted.events
    assert command_id not in {record.command_id for record in loaded.command_records}
    assert extra_ref not in {
        registration.ref for registration in loaded.artifact_registrations
    }
    assert 7 not in loaded.checkpoint_revisions
    claim = await repository.claim_action(
        worker_id="worker-a",
        now_utc=UTC_NOW,
        lease_seconds=10,
    )
    assert claim is not None
    assert claim.action.action_id == "action-1"


@pytest.mark.asyncio
async def test_event_listing_filters_after_sequence_and_missing_attempts_are_empty(
    repository_factory: Callable[[], AttemptRepository],
) -> None:
    repository = repository_factory()
    assert await repository.load("missing") is None
    assert await repository.list_events("missing") == ()

    created = await repository.commit(_create_request())
    started = await repository.commit(_start_request(created))

    assert await repository.list_events("attempt-1", after_sequence_no=0) == started.events
    assert tuple(
        event.sequence_no
        for event in await repository.list_events(
            "attempt-1", after_sequence_no=1
        )
    ) == (2,)
    assert await repository.list_events("attempt-1", after_sequence_no=2) == ()
    with pytest.raises(ValueError, match="after_sequence_no"):
        await repository.list_events("attempt-1", after_sequence_no=-1)


@pytest.mark.asyncio
async def test_returned_values_are_deep_validated_copies(
    repository_factory: Callable[[], AttemptRepository],
) -> None:
    repository = repository_factory()
    request = _create_request()
    created = await repository.commit(request)
    loaded = await repository.load("attempt-1")
    assert loaded is not None
    found = await repository.find_command("attempt-1", request.command_id)
    listed = await repository.list_events("attempt-1")
    assert found is not None

    object.__setattr__(created.state, "revision", 999)
    object.__setattr__(created.events[0], "sequence_no", 999)
    object.__setattr__(loaded.command_records[0].result, "revision", 999)
    object.__setattr__(loaded.artifact_registrations[0].ref, "byte_size", 999)
    object.__setattr__(found.result, "revision", 999)
    object.__setattr__(listed[0], "sequence_no", 999)

    reloaded = await repository.load("attempt-1")
    refound = await repository.find_command("attempt-1", request.command_id)
    relisted = await repository.list_events("attempt-1")
    assert reloaded is not None
    assert refound is not None
    assert reloaded.state.revision == 1
    assert reloaded.events[0].sequence_no == 1
    assert reloaded.command_records[0].result.revision == 1
    assert reloaded.artifact_registrations[0].ref.byte_size == 12
    assert refound.result.revision == 1
    assert relisted[0].sequence_no == 1


@pytest.mark.asyncio
async def test_action_claim_lease_blocks_then_reclaims_in_stable_order(
    repository_factory: Callable[[], AttemptRepository],
) -> None:
    repository = repository_factory()
    created = await repository.commit(_create_request())
    started = await repository.commit(_start_request(created))
    action_request, normalized = _accepted_action_request(started)
    await repository.commit(action_request)

    first = await repository.claim_action(
        worker_id="worker-a",
        now_utc=UTC_NOW,
        lease_seconds=10,
    )
    blocked = await repository.claim_action(
        worker_id="worker-b",
        now_utc=UTC_NOW + timedelta(seconds=9),
        lease_seconds=10,
    )
    assert first is not None
    assert first.action == normalized
    assert first.action_status is OutboxActionStatus.ACCEPTED
    object.__setattr__(first.action, "action_id", "mutated-action")
    reclaimed = await repository.claim_action(
        worker_id="worker-b",
        now_utc=UTC_NOW + timedelta(seconds=10),
        lease_seconds=5,
    )

    assert first.accepted_sequence_no == 6
    assert first.worker_id == "worker-a"
    assert first.lease_expires_at == UTC_NOW + timedelta(seconds=10)
    assert blocked is None
    assert reclaimed is not None
    assert reclaimed.action.action_id == "action-1"
    assert reclaimed.action_status is OutboxActionStatus.ACCEPTED
    assert reclaimed.worker_id == "worker-b"
    assert reclaimed.lease_expires_at == UTC_NOW + timedelta(seconds=15)


@pytest.mark.asyncio
async def test_predispatch_cancellation_releases_budget_and_removes_outbox_atomically(
    repository_factory: Callable[[], AttemptRepository],
) -> None:
    repository = repository_factory()
    created = await repository.commit(_create_request())
    started = await repository.commit(_start_request(created))
    action_request, normalized = _accepted_action_request(started)
    accepted = await repository.commit(action_request)
    command_id = _uuid(850)
    cancelled_event_id = _uuid(851)
    error = ErrorSummary(
        code="DISPATCH_CANCELLED",
        retryable=False,
        safe_message="The Action was cancelled before dispatch.",
    )
    cancelled = ActionCancelled(
        **_envelope(
            attempt_id="attempt-1",
            sequence_no=7,
            command_id=command_id,
            event_id=cancelled_event_id,
            causal_parent_id=accepted.latest_event_id,
        ),
        event_type="ACTION_CANCELLED",
        action_id=normalized.action_id,
        outcome=ActionCancelledOutcome(
            status=ActionStatus.CANCELLED,
            action_id=normalized.action_id,
            error=error,
        ),
    )
    released = BudgetReleased(
        **_envelope(
            attempt_id="attempt-1",
            sequence_no=8,
            command_id=command_id,
            event_id=_uuid(852),
            causal_parent_id=cancelled_event_id,
        ),
        event_type="BUDGET_RELEASED",
        budget=_budget(),
        reservation=BudgetReservationEntry(
            action_id=normalized.action_id,
            reservation_id=normalized.reservation_id,
            resource_requests=normalized.resource_requests,
        ),
    )
    request = CommitRequest(
        command_id=command_id,
        request_hash=_request_hash("cancel-before-dispatch"),
        attempt_id="attempt-1",
        expected_revision=6,
        events=(cancelled, released),
        outbox_actions=(),
        artifact_registrations=(),
        completed_delivery_action_ids=(normalized.action_id,),
        result=CommandResult(
            command_id=command_id,
            attempt_id="attempt-1",
            accepted=True,
            revision=8,
            phase=AttemptPhase.RUNNING,
        ),
        checkpoint=False,
    )

    committed = await repository.commit(request)

    assert committed.state.actions[0].status is ActionStatus.CANCELLED
    assert committed.state.budget == _budget()
    assert tuple(type(event) for event in committed.events[-2:]) == (
        ActionCancelled,
        BudgetReleased,
    )
    assert (
        await repository.claim_action(
            worker_id="worker-a",
            now_utc=UTC_NOW,
            lease_seconds=10,
        )
        is None
    )

    stale_repository = repository_factory()
    stale_created = await stale_repository.commit(_create_request())
    stale_started = await stale_repository.commit(_start_request(stale_created))
    stale_action_request, _ = _accepted_action_request(stale_started)
    stale_accepted = await stale_repository.commit(stale_action_request)

    with pytest.raises(InvalidCommit):
        await stale_repository.commit(
            request.model_copy(update={"completed_delivery_action_ids": ()})
        )

    reloaded = await stale_repository.load("attempt-1")
    assert reloaded is not None
    assert reloaded.events == stale_accepted.events
    assert request.command_id not in {
        record.command_id for record in reloaded.command_records
    }
    assert reloaded.state.actions[0].status is ActionStatus.ACCEPTED
    assert reloaded.state.budget == _budget(reserved_model_calls=1)
    claim = await stale_repository.claim_action(
        worker_id="worker-a",
        now_utc=UTC_NOW,
        lease_seconds=10,
    )
    assert claim is not None
    assert claim.action.action_id == normalized.action_id

    mismatch_repository = repository_factory()
    mismatch_created = await mismatch_repository.commit(_create_request())
    mismatch_started = await mismatch_repository.commit(
        _start_request(mismatch_created)
    )
    mismatch_action_request, _ = _accepted_action_request(mismatch_started)
    mismatch_accepted = await mismatch_repository.commit(mismatch_action_request)
    mismatched_completion = request.model_copy(
        update={"completed_delivery_action_ids": ("different-action",)}
    )

    with pytest.raises(InvalidCommit):
        await mismatch_repository.commit(mismatched_completion)

    mismatch_loaded = await mismatch_repository.load("attempt-1")
    assert mismatch_loaded is not None
    assert mismatch_loaded.events == mismatch_accepted.events
    assert request.command_id not in {
        record.command_id for record in mismatch_loaded.command_records
    }
    assert mismatch_loaded.state.actions[0].status is ActionStatus.ACCEPTED
    assert mismatch_loaded.state.budget == _budget(reserved_model_calls=1)
    mismatch_claim = await mismatch_repository.claim_action(
        worker_id="worker-mismatch",
        now_utc=UTC_NOW,
        lease_seconds=10,
    )
    assert mismatch_claim is not None
    assert mismatch_claim.action.action_id == normalized.action_id

    settled_before_dispatch = BudgetSettled(
        **_envelope(
            attempt_id="attempt-1",
            sequence_no=8,
            command_id=command_id,
            event_id=_uuid(853),
            causal_parent_id=cancelled_event_id,
        ),
        event_type="BUDGET_SETTLED",
        budget=_budget(consumed_model_calls=1),
        reservation=BudgetReservationEntry(
            action_id=normalized.action_id,
            reservation_id=normalized.reservation_id,
            resource_requests=normalized.resource_requests,
        ),
    )
    invalid_settlement = request.model_copy(
        update={"events": (cancelled, settled_before_dispatch)}
    )

    with pytest.raises(StateTransitionError) as caught:
        await stale_repository.commit(invalid_settlement)
    assert caught.value.code == "invalid_budget_transition"

    after_invalid_settlement = await stale_repository.load("attempt-1")
    assert after_invalid_settlement is not None
    assert after_invalid_settlement.events == stale_accepted.events
    assert request.command_id not in {
        record.command_id
        for record in after_invalid_settlement.command_records
    }
    assert after_invalid_settlement.state.actions[0].status is ActionStatus.ACCEPTED
    assert after_invalid_settlement.state.budget == _budget(reserved_model_calls=1)


@pytest.mark.asyncio
async def test_mixed_started_outcome_and_accepted_cancellation_have_store_parity(
    repository_factory: Callable[[], AttemptRepository],
) -> None:
    repository = repository_factory()
    engine, _, _, _, _ = _attempt_engine(repository=repository)
    assert (await engine.handle(_engine_create_command())).accepted is True
    assert (await engine.handle(_engine_start_command())).accepted is True
    proposals = tuple(
        _engine_proposal(ordinal=index, batch_id="mixed-terminal-batch")
        for index in range(2)
    )
    decision = _engine_decision_command(proposals[0]).model_copy(
        update={"proposals": proposals}
    )
    assert (await engine.handle(decision)).accepted is True
    accepted = await repository.load("attempt-1")
    assert accepted is not None
    claim = await repository.claim_action(
        worker_id="mixed-terminal-worker",
        now_utc=UTC_NOW,
        lease_seconds=10,
    )
    assert claim is not None
    assert (
        await engine.handle(
            ReportActionStarted(
                schema_version=1,
                command_type="REPORT_ACTION_STARTED",
                command_id=action_command_id(claim.action.action_id, "started"),
                attempt_id="attempt-1",
                expected_revision=accepted.state.revision,
                action_id=claim.action.action_id,
            ),
            delivery_claim=claim.delivery_claim(),
        )
    ).accepted is True
    before = await repository.load("attempt-1")
    assert before is not None
    started_action = next(
        action
        for action in before.state.actions
        if action.status is ActionStatus.STARTED
    )
    accepted_action = next(
        action
        for action in before.state.actions
        if action.status is ActionStatus.ACCEPTED
    )
    command_id = _uuid(15000)
    result_ref = _artifact("attempt-1", "mixed-terminal-result")
    error = ErrorSummary(
        code="DISPATCH_CANCELLED",
        retryable=False,
        safe_message="The unstarted Action was cancelled.",
    )
    invocation_completed_id = _uuid(15001)
    action_succeeded_id = _uuid(15002)
    settled_id = _uuid(15003)
    action_cancelled_id = _uuid(15004)
    invocation_completed = InvocationCompleted(
        **(
            _envelope(
                attempt_id="attempt-1",
                sequence_no=before.state.revision + 1,
                command_id=command_id,
                event_id=invocation_completed_id,
                causal_parent_id=before.latest_event_id,
            )
            | {"trial_id": before.state.trial_id}
        ),
        event_type="INVOCATION_COMPLETED",
        action_id=started_action.action_id,
        invocation_id=started_action.invocation_id,
        latest_context_ref=result_ref,
    )
    action_succeeded = ActionSucceeded(
        **(
            _envelope(
                attempt_id="attempt-1",
                sequence_no=before.state.revision + 2,
                command_id=command_id,
                event_id=action_succeeded_id,
                causal_parent_id=invocation_completed_id,
            )
            | {"trial_id": before.state.trial_id}
        ),
        event_type="ACTION_SUCCEEDED",
        action_id=started_action.action_id,
        outcome=ActionSucceededOutcome(
            status=ActionStatus.SUCCEEDED,
            action_id=started_action.action_id,
            result_ref=result_ref,
        ),
    )
    request_amount = started_action.resource_requests[0].amount
    reserved_after_settlement = before.state.budget.resources[0].reserved - request_amount
    consumed_after_settlement = before.state.budget.resources[0].consumed + request_amount
    settled_budget = before.state.budget.model_copy(
        update={
            "resources": (
                before.state.budget.resources[0].model_copy(
                    update={
                        "reserved": reserved_after_settlement,
                        "consumed": consumed_after_settlement,
                    }
                ),
            )
        }
    )
    settled = BudgetSettled(
        **(
            _envelope(
                attempt_id="attempt-1",
                sequence_no=before.state.revision + 3,
                command_id=command_id,
                event_id=settled_id,
                causal_parent_id=action_succeeded_id,
            )
            | {"trial_id": before.state.trial_id}
        ),
        event_type="BUDGET_SETTLED",
        budget=settled_budget,
        reservation=BudgetReservationEntry(
            action_id=started_action.action_id,
            reservation_id=started_action.reservation_id,
            resource_requests=started_action.resource_requests,
        ),
    )
    action_cancelled = ActionCancelled(
        **(
            _envelope(
                attempt_id="attempt-1",
                sequence_no=before.state.revision + 4,
                command_id=command_id,
                event_id=action_cancelled_id,
                causal_parent_id=settled_id,
            )
            | {"trial_id": before.state.trial_id}
        ),
        event_type="ACTION_CANCELLED",
        action_id=accepted_action.action_id,
        outcome=ActionCancelledOutcome(
            status=ActionStatus.CANCELLED,
            action_id=accepted_action.action_id,
            error=error,
        ),
    )
    released_budget = settled_budget.model_copy(
        update={
            "resources": (
                settled_budget.resources[0].model_copy(
                    update={
                        "reserved": settled_budget.resources[0].reserved
                        - accepted_action.resource_requests[0].amount,
                    }
                ),
            )
        }
    )
    released = BudgetReleased(
        **(
            _envelope(
                attempt_id="attempt-1",
                sequence_no=before.state.revision + 5,
                command_id=command_id,
                event_id=_uuid(15005),
                causal_parent_id=action_cancelled_id,
            )
            | {"trial_id": before.state.trial_id}
        ),
        event_type="BUDGET_RELEASED",
        budget=released_budget,
        reservation=BudgetReservationEntry(
            action_id=accepted_action.action_id,
            reservation_id=accepted_action.reservation_id,
            resource_requests=accepted_action.resource_requests,
        ),
    )
    request = CommitRequest(
        command_id=command_id,
        request_hash=_request_hash("mixed-terminal-completion"),
        attempt_id="attempt-1",
        expected_revision=before.state.revision,
        events=(
            invocation_completed,
            action_succeeded,
            settled,
            action_cancelled,
            released,
        ),
        outbox_actions=(),
        artifact_registrations=(ArtifactRegistration(ref=result_ref),),
        completed_delivery_action_ids=(
            started_action.action_id,
            accepted_action.action_id,
        ),
        result=CommandResult(
            command_id=command_id,
            attempt_id="attempt-1",
            accepted=True,
            revision=before.state.revision + 5,
            phase=AttemptPhase.RUNNING,
        ),
        checkpoint=False,
    )

    with pytest.raises(InvalidCommit):
        await repository.commit(
            request.model_copy(
                update={
                    "completed_delivery_action_ids": (started_action.action_id,)
                }
            )
        )

    rolled_back = await repository.load("attempt-1")
    assert rolled_back is not None
    assert rolled_back.events == before.events
    assert rolled_back.state == before.state
    assert command_id not in {
        record.command_id for record in rolled_back.command_records
    }
    committed = await repository.commit(request)

    assert tuple(action.status for action in committed.state.actions) == (
        ActionStatus.SUCCEEDED,
        ActionStatus.CANCELLED,
    )
    assert committed.state.budget.resources[0].reserved == 0
    assert committed.state.budget.resources[0].consumed == 2
    assert tuple(event.event_type for event in committed.events[-5:]) == (
        "INVOCATION_COMPLETED",
        "ACTION_SUCCEEDED",
        "BUDGET_SETTLED",
        "ACTION_CANCELLED",
        "BUDGET_RELEASED",
    )
    assert (
        await repository.claim_action(
            worker_id="after-mixed-terminal",
            now_utc=UTC_NOW + timedelta(seconds=10),
            lease_seconds=10,
        )
        is None
    )


@pytest.mark.asyncio
async def test_started_action_cannot_use_predispatch_budget_release(
    repository_factory: Callable[[], AttemptRepository],
) -> None:
    repository = repository_factory()
    created = await repository.commit(_create_request())
    started = await repository.commit(_start_request(created))
    action_request, normalized = _accepted_action_request(started)
    accepted = await repository.commit(action_request)
    command_id = _uuid(860)
    started_event_id = _uuid(861)
    cancelled_event_id = _uuid(862)
    error = ErrorSummary(
        code="EXECUTION_CANCELLED",
        retryable=False,
        safe_message="The started Action was cancelled.",
    )
    action_started = ActionStarted(
        **_envelope(
            attempt_id="attempt-1",
            sequence_no=7,
            command_id=command_id,
            event_id=started_event_id,
            causal_parent_id=accepted.latest_event_id,
        ),
        event_type="ACTION_STARTED",
        action_id=normalized.action_id,
    )
    action_cancelled = ActionCancelled(
        **_envelope(
            attempt_id="attempt-1",
            sequence_no=8,
            command_id=command_id,
            event_id=cancelled_event_id,
            causal_parent_id=started_event_id,
        ),
        event_type="ACTION_CANCELLED",
        action_id=normalized.action_id,
        outcome=ActionCancelledOutcome(
            status=ActionStatus.CANCELLED,
            action_id=normalized.action_id,
            error=error,
        ),
    )
    released = BudgetReleased(
        **_envelope(
            attempt_id="attempt-1",
            sequence_no=9,
            command_id=command_id,
            event_id=_uuid(863),
            causal_parent_id=cancelled_event_id,
        ),
        event_type="BUDGET_RELEASED",
        budget=_budget(),
        reservation=BudgetReservationEntry(
            action_id=normalized.action_id,
            reservation_id=normalized.reservation_id,
            resource_requests=normalized.resource_requests,
        ),
    )
    invalid = CommitRequest(
        command_id=command_id,
        request_hash=_request_hash("release-started-action"),
        attempt_id="attempt-1",
        expected_revision=6,
        events=(action_started, action_cancelled, released),
        outbox_actions=(),
        artifact_registrations=(),
        completed_delivery_action_ids=(normalized.action_id,),
        result=CommandResult(
            command_id=command_id,
            attempt_id="attempt-1",
            accepted=True,
            revision=9,
            phase=AttemptPhase.RUNNING,
        ),
        checkpoint=False,
    )

    with pytest.raises(StateTransitionError) as caught:
        await repository.commit(invalid)
    assert caught.value.code == "invalid_budget_transition"

    loaded = await repository.load("attempt-1")
    assert loaded is not None
    assert loaded.events == accepted.events
    assert command_id not in {
        record.command_id for record in loaded.command_records
    }
    assert loaded.state.actions[0].status is ActionStatus.ACCEPTED
    assert loaded.state.budget == _budget(reserved_model_calls=1)
    claim = await repository.claim_action(
        worker_id="worker-a",
        now_utc=UTC_NOW,
        lease_seconds=10,
    )
    assert claim is not None
    assert claim.action.action_id == normalized.action_id

    settled = BudgetSettled(
        **_envelope(
            attempt_id="attempt-1",
            sequence_no=9,
            command_id=command_id,
            event_id=_uuid(864),
            causal_parent_id=cancelled_event_id,
        ),
        event_type="BUDGET_SETTLED",
        budget=_budget(consumed_model_calls=1),
        reservation=BudgetReservationEntry(
            action_id=normalized.action_id,
            reservation_id=normalized.reservation_id,
            resource_requests=normalized.resource_requests,
        ),
    )
    valid = invalid.model_copy(
        update={
            "events": (action_started, action_cancelled, settled),
            "delivery_claim": claim.delivery_claim(),
        }
    )

    committed = await repository.commit(valid)

    assert committed.state.actions[0].status is ActionStatus.CANCELLED
    assert committed.state.budget == _budget(consumed_model_calls=1)
    assert (
        await repository.claim_action(
            worker_id="worker-b",
            now_utc=UTC_NOW + timedelta(seconds=10),
            lease_seconds=10,
        )
        is None
    )


@pytest.mark.asyncio
async def test_claim_validation_and_completed_delivery_removal(
    repository_factory: Callable[[], AttemptRepository],
) -> None:
    repository = repository_factory()
    created = await repository.commit(_create_request())
    started = await repository.commit(_start_request(created))
    action_request, _ = _accepted_action_request(started)
    accepted = await repository.commit(action_request)

    with pytest.raises(ValueError, match="worker_id"):
        await repository.claim_action(
            worker_id="",
            now_utc=UTC_NOW,
            lease_seconds=10,
        )
    with pytest.raises(ValueError, match="UTC"):
        await repository.claim_action(
            worker_id="worker-a",
            now_utc=UTC_NOW.replace(tzinfo=None),
            lease_seconds=10,
        )
    with pytest.raises(ValueError, match="lease_seconds"):
        await repository.claim_action(
            worker_id="worker-a",
            now_utc=UTC_NOW,
            lease_seconds=0,
        )

    start_command_id = _uuid(900)
    action_started_id = _uuid(901)
    started_event = ActionStarted(
        **_envelope(
            attempt_id="attempt-1",
            sequence_no=7,
            command_id=start_command_id,
            event_id=action_started_id,
            causal_parent_id=accepted.latest_event_id,
        ),
        event_type="ACTION_STARTED",
        action_id="action-1",
    )
    invocation_started = InvocationStarted(
        **_envelope(
            attempt_id="attempt-1",
            sequence_no=8,
            command_id=start_command_id,
            event_id=_uuid(902),
            causal_parent_id=action_started_id,
        ),
        event_type="INVOCATION_STARTED",
        action_id="action-1",
        invocation_id="invocation-1",
    )
    start_request = CommitRequest(
        command_id=start_command_id,
        request_hash=_request_hash("start-delivery"),
        attempt_id="attempt-1",
        expected_revision=6,
        events=(started_event, invocation_started),
        outbox_actions=(),
        artifact_registrations=(),
        completed_delivery_action_ids=(),
        result=CommandResult(
            command_id=start_command_id,
            attempt_id="attempt-1",
            accepted=True,
            revision=8,
            phase=AttemptPhase.RUNNING,
        ),
        checkpoint=False,
    )
    with pytest.raises(InvalidCommit):
        await repository.commit(start_request)

    claimed = await repository.claim_action(
        worker_id="worker-a",
        now_utc=UTC_NOW,
        lease_seconds=10,
    )
    assert claimed is not None
    assert claimed.action_status is OutboxActionStatus.ACCEPTED

    delivery_started = await repository.commit(
        start_request.model_copy(
            update={"delivery_claim": claimed.delivery_claim()}
        )
    )
    recovery_claim = await repository.claim_action(
        worker_id="worker-b",
        now_utc=UTC_NOW + timedelta(seconds=10),
        lease_seconds=10,
    )
    assert recovery_claim is not None
    assert recovery_claim.action_status is OutboxActionStatus.STARTED

    outcome_command_id = _uuid(910)
    invocation_completed_id = _uuid(911)
    result_ref = _artifact("attempt-1", "action-1-result")
    invocation_completed = InvocationCompleted(
        **_envelope(
            attempt_id="attempt-1",
            sequence_no=9,
            command_id=outcome_command_id,
            event_id=invocation_completed_id,
            causal_parent_id=delivery_started.latest_event_id,
        ),
        event_type="INVOCATION_COMPLETED",
        action_id="action-1",
        invocation_id="invocation-1",
        latest_context_ref=None,
    )
    action_succeeded = ActionSucceeded(
        **_envelope(
            attempt_id="attempt-1",
            sequence_no=10,
            command_id=outcome_command_id,
            event_id=_uuid(912),
            causal_parent_id=invocation_completed_id,
        ),
        event_type="ACTION_SUCCEEDED",
        action_id="action-1",
        outcome=ActionSucceededOutcome(
            status=ActionStatus.SUCCEEDED,
            action_id="action-1",
            result_ref=result_ref,
        ),
    )
    incomplete_completion = CommitRequest(
        command_id=outcome_command_id,
        request_hash=_request_hash("complete-delivery"),
        attempt_id="attempt-1",
        expected_revision=8,
        events=(invocation_completed, action_succeeded),
        outbox_actions=(),
        artifact_registrations=(ArtifactRegistration(ref=result_ref),),
        completed_delivery_action_ids=("action-1",),
        result=CommandResult(
            command_id=outcome_command_id,
            attempt_id="attempt-1",
            accepted=True,
            revision=10,
            phase=AttemptPhase.RUNNING,
        ),
        checkpoint=False,
    )

    with pytest.raises(StateTransitionError) as caught:
        await repository.commit(incomplete_completion)
    assert caught.value.code == "invalid_budget_transition"

    after_incomplete = await repository.load("attempt-1")
    assert after_incomplete is not None
    assert after_incomplete.events == delivery_started.events
    assert outcome_command_id not in {
        record.command_id for record in after_incomplete.command_records
    }
    assert incomplete_completion.artifact_registrations[0] not in (
        after_incomplete.artifact_registrations
    )

    reservation_id = accepted.state.actions[0].reservation_id
    assert reservation_id is not None
    settled = BudgetSettled(
        **_envelope(
            attempt_id="attempt-1",
            sequence_no=11,
            command_id=outcome_command_id,
            event_id=_uuid(913),
            causal_parent_id=action_succeeded.event_id,
        ),
        event_type="BUDGET_SETTLED",
        budget=_budget(consumed_model_calls=1),
        reservation=BudgetReservationEntry(
            action_id=accepted.state.actions[0].action_id,
            reservation_id=reservation_id,
            resource_requests=accepted.state.actions[0].resource_requests,
        ),
    )
    completion = incomplete_completion.model_copy(
        update={
            "events": (invocation_completed, action_succeeded, settled),
            "result": incomplete_completion.result.model_copy(
                update={"revision": 11}
            ),
        }
    )
    completed = await repository.commit(completion)

    assert completed.state.budget == _budget(consumed_model_calls=1)

    assert (
        await repository.claim_action(
            worker_id="worker-a",
            now_utc=UTC_NOW,
            lease_seconds=10,
        )
        is None
    )


@pytest.mark.asyncio
async def test_list_nonterminal_attempt_ids_is_sorted_and_excludes_terminal(
    repository_factory: Callable[[], AttemptRepository],
) -> None:
    repository = repository_factory()
    created_b = await repository.commit(
        _create_request(attempt_id="attempt-b", seed=1000)
    )
    created_a = await repository.commit(
        _create_request(attempt_id="attempt-a", seed=1100)
    )
    started_a = await repository.commit(_start_request(created_a, seed=1200))
    await repository.commit(_finish_request(started_a, seed=1300))

    assert await repository.list_nonterminal_attempt_ids() == ("attempt-b",)
    assert created_b.state.phase is AttemptPhase.PLANNED


def test_repository_value_contracts_are_strict_frozen_and_use_string_attempt_ids() -> None:
    request = _create_request()
    registration = request.artifact_registrations[0]

    assert request.attempt_id == "attempt-1"
    assert registration.ref == request.events[0].manifest_ref
    with pytest.raises(ValidationError, match="extra_forbidden"):
        ArtifactRegistration(
            ref=registration.ref,
            storage_path="not-owned-by-task-7",
        )
    with pytest.raises(ValidationError, match="frozen"):
        registration.ref = _artifact("attempt-1", "other")
