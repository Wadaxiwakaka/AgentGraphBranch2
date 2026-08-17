from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone
from hashlib import sha256

import pytest
from pydantic import ValidationError

from experiment_system.state import (
    ActionState,
    ActionStatus,
    AgentView,
    ArtifactRef,
    AttemptPhase,
    AttemptState,
    BudgetState,
    ErrorSummary,
    ExternalRequest,
    ExternalRequestKind,
    InvocationState,
    OperatorView,
    RecoveryPolicy,
    ResourceBudget,
    StrategyStateEnvelope,
    StrategyView,
    canonical_state_bytes,
    to_agent_view,
    to_operator_view,
    to_strategy_view,
)


HASH = "a" * 64
UTC_NOW = datetime(2026, 7, 23, 8, 30, tzinfo=timezone.utc)


def _canonical_json(value: object) -> bytes:
    return json.dumps(
        value,
        ensure_ascii=False,
        allow_nan=False,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")


def _full_artifact(path: str = "attempts/a1/manifest.json") -> ArtifactRef:
    return ArtifactRef(
        capture_class="full",
        content_hash=HASH,
        media_type="application/json",
        byte_size=12,
        relative_path=path,
    )


def _strategy_envelope(value: object | None = None) -> StrategyStateEnvelope:
    if value is None:
        value = {"queue": ["agent-a"], "round": 1}
    payload = _canonical_json(value)
    return StrategyStateEnvelope(
        strategy_id="router",
        strategy_schema_version=1,
        value=value,
        content_hash=sha256(payload).hexdigest(),
        byte_size=len(payload),
    )


def _budget() -> BudgetState:
    return BudgetState(
        resources=(
            ResourceBudget(resource="actions", limit=10, reserved=2, consumed=3),
            ResourceBudget(resource="tokens", limit=1_000, reserved=100, consumed=250),
        ),
        deadline_at=UTC_NOW + timedelta(hours=1),
        max_call_depth=4,
        max_concurrent_actions=2,
    )


def _action(
    action_id: str = "action-1",
    *,
    status: ActionStatus = ActionStatus.ACCEPTED,
) -> ActionState:
    is_proposed = status is ActionStatus.PROPOSED
    is_succeeded = status is ActionStatus.SUCCEEDED
    return ActionState(
        action_id=action_id,
        action_type="invoke_agent",
        actor_id="engine",
        target_ids=("agent-a",),
        status=status,
        causal_parent_id=None,
        invocation_id="invocation-1",
        payload_ref=_full_artifact("attempts/a1/actions/action-1.json"),
        result_ref=(
            _full_artifact("attempts/a1/actions/action-1-result.json")
            if is_succeeded
            else None
        ),
        idempotency_key=None if is_proposed else "idem-action-1",
        recovery_policy=RecoveryPolicy.REPLAY_SAFE,
        reservation_id=None if is_proposed else "reservation-1",
        retry_of_action_id=None,
        error_code=None,
    )


def _invocation(
    invocation_id: str = "invocation-1", *, status: str = "REQUESTED"
) -> InvocationState:
    return InvocationState(
        invocation_id=invocation_id,
        agent_id="agent-a",
        conversation_id="conversation-1",
        parent_invocation_id=None,
        latest_context_ref=_full_artifact("attempts/a1/context/latest.json"),
        last_action_id="action-1",
        status=status,
    )


def _state(**updates: object) -> AttemptState:
    values: dict[str, object] = {
        "schema_version": 1,
        "experiment_id": "experiment-1",
        "trial_id": "trial-1",
        "attempt_id": "attempt-1",
        "strategy_id": "router",
        "revision": 1,
        "phase": AttemptPhase.RUNNING,
        "manifest_ref": _full_artifact(),
        "strategy": _strategy_envelope(),
        "budget": _budget(),
        "actions": (_action(),),
        "invocations": (_invocation(),),
        "pending_external": (),
        "result_ref": None,
        "terminal_error": None,
        "started_at": UTC_NOW,
        "finished_at": None,
    }
    values.update(updates)
    if (
        values["phase"] is AttemptPhase.WAITING_EXTERNAL
        and "pending_external" not in updates
    ):
        values["pending_external"] = (
            ExternalRequest(
                request_id="request-1",
                request_kind=ExternalRequestKind.ADDITIONAL_INPUT,
                action_id=None,
                payload_ref=None,
            ),
        )
    return AttemptState(**values)


def test_public_enums_use_canonical_string_values() -> None:
    assert AttemptPhase.PLANNED.value == "PLANNED"
    assert ActionStatus.OUTCOME_UNKNOWN.value == "OUTCOME_UNKNOWN"
    assert RecoveryPolicy.NON_REPLAYABLE.value == "NON_REPLAYABLE"
    assert {status.value for status in ActionStatus} == {
        "PROPOSED",
        "REJECTED",
        "ACCEPTED",
        "STARTED",
        "SUCCEEDED",
        "FAILED",
        "TIMED_OUT",
        "CANCELLED",
        "OUTCOME_UNKNOWN",
    }


def test_attempt_state_round_trips_through_canonical_json() -> None:
    state = _state()

    assert state.revision == 1
    assert canonical_state_bytes(state) == canonical_state_bytes(
        AttemptState.model_validate(state.model_dump(mode="json"))
    )
    assert canonical_state_bytes(state) == canonical_state_bytes(state)


@pytest.mark.parametrize(
    "constructor, kwargs",
    [
        (ResourceBudget, {"resource": "calls", "limit": 1, "unknown": True}),
        (
            ArtifactRef,
            {
                "capture_class": "metadata_only",
                "media_type": "text/plain",
                "byte_size": 0,
                "unknown": True,
            },
        ),
    ],
)
def test_persisted_models_forbid_extra_fields(constructor: object, kwargs: dict[str, object]) -> None:
    with pytest.raises(ValidationError, match="extra_forbidden"):
        constructor(**kwargs)  # type: ignore[operator]

    with pytest.raises(ValidationError, match="extra_forbidden"):
        _state(unknown=True)


def test_persisted_models_are_frozen() -> None:
    state = _state()

    with pytest.raises(ValidationError, match="frozen_instance"):
        state.revision = 2

    with pytest.raises(ValidationError, match="frozen_instance"):
        state.budget.resources[0].consumed = 4


@pytest.mark.parametrize(
    "timestamp",
    [
        datetime(2026, 7, 23, 8, 30),
        datetime(2026, 7, 23, 16, 30, tzinfo=timezone(timedelta(hours=8))),
    ],
)
def test_timestamps_must_be_aware_utc(timestamp: datetime) -> None:
    with pytest.raises(ValidationError, match="UTC"):
        _state(started_at=timestamp)

    with pytest.raises(ValidationError, match="UTC"):
        BudgetState(
            resources=(),
            deadline_at=timestamp,
            max_call_depth=1,
            max_concurrent_actions=1,
        )


@pytest.mark.parametrize(
    "path",
    [
        "../secret.json",
        "artifacts/../secret.json",
        "/var/data/secret.json",
        "C:/artifacts/secret.json",
        "C:\\artifacts\\secret.json",
        "\\\\server\\share\\secret.json",
        "artifacts\\secret.json",
        ".",
        "artifacts/./secret.json",
        "artifacts/secret.json:stream",
    ],
)
def test_artifact_paths_reject_traversal_and_absolute_roots(path: str) -> None:
    with pytest.raises(ValidationError, match="relative path"):
        _full_artifact(path)


@pytest.mark.parametrize(
    "capture_class, content_hash, relative_path, valid",
    [
        ("full", HASH, "attempts/a1/data.json", True),
        ("full", None, "attempts/a1/data.json", False),
        ("full", HASH, None, False),
        ("hashed", HASH, None, True),
        ("hashed", None, None, False),
        ("hashed", HASH, "attempts/a1/data.json", False),
        ("metadata_only", None, None, True),
        ("metadata_only", HASH, None, False),
        ("metadata_only", None, "attempts/a1/data.json", False),
    ],
)
def test_artifact_capture_contract(
    capture_class: str,
    content_hash: str | None,
    relative_path: str | None,
    valid: bool,
) -> None:
    kwargs = {
        "capture_class": capture_class,
        "content_hash": content_hash,
        "media_type": "application/json",
        "byte_size": 12,
        "relative_path": relative_path,
    }

    if valid:
        ArtifactRef(**kwargs)
    else:
        with pytest.raises(ValidationError):
            ArtifactRef(**kwargs)


@pytest.mark.parametrize("content_hash", ["A" * 64, "a" * 63, "g" * 64])
def test_artifact_hash_is_lowercase_sha256(content_hash: str) -> None:
    with pytest.raises(ValidationError, match="SHA-256"):
        ArtifactRef(
            capture_class="hashed",
            content_hash=content_hash,
            media_type="application/octet-stream",
            byte_size=1,
        )


def test_strategy_state_accepts_json_and_recursively_freezes_it() -> None:
    source = {"queue": ["a", {"scores": [1, 2.5, None]}]}
    envelope = _strategy_envelope(source)

    source["queue"].append("later")
    assert envelope.value["queue"] == ("a", {"scores": (1, 2.5, None)})
    with pytest.raises(TypeError):
        envelope.value["new"] = True
    with pytest.raises(TypeError):
        envelope.value["queue"][1]["scores"] += (3,)


def test_frozen_json_mapping_rejects_dict_base_mutators() -> None:
    envelope = _strategy_envelope({"nested": {"round": 1}})

    with pytest.raises(TypeError):
        dict.__setitem__(envelope.value, "injected", True)
    with pytest.raises(TypeError):
        dict.__setitem__(envelope.value["nested"], "injected", True)

    assert envelope.model_dump(mode="json")["value"] == {"nested": {"round": 1}}


@pytest.mark.parametrize("value", [{"bad": object()}, {"bad": float("nan")}, {1: "bad-key"}])
def test_strategy_state_rejects_non_json_values(value: object) -> None:
    with pytest.raises((ValidationError, ValueError), match="JSON|finite|string"):
        StrategyStateEnvelope(
            strategy_id="router",
            strategy_schema_version=1,
            value=value,
            content_hash=HASH,
            byte_size=1,
        )


def test_strategy_state_requires_exactly_one_storage_form() -> None:
    artifact = _full_artifact("strategy/router/state.json")

    with pytest.raises(ValidationError, match="exactly one"):
        StrategyStateEnvelope(
            strategy_id="router",
            strategy_schema_version=1,
            content_hash=HASH,
            byte_size=12,
        )

    with pytest.raises(ValidationError, match="exactly one"):
        StrategyStateEnvelope(
            strategy_id="router",
            strategy_schema_version=1,
            value={},
            artifact_ref=artifact,
            content_hash=HASH,
            byte_size=12,
        )


def test_strategy_state_validates_identity_schema_size_and_hash() -> None:
    value = {"round": 1}
    payload = _canonical_json(value)

    with pytest.raises(ValidationError, match="strategy_id"):
        _state(strategy_id="other")
    with pytest.raises(ValidationError):
        StrategyStateEnvelope(
            strategy_id="router",
            strategy_schema_version=0,
            value=value,
            content_hash=sha256(payload).hexdigest(),
            byte_size=len(payload),
        )
    with pytest.raises(ValidationError, match="byte_size"):
        StrategyStateEnvelope(
            strategy_id="router",
            strategy_schema_version=1,
            value=value,
            content_hash=sha256(payload).hexdigest(),
            byte_size=len(payload) + 1,
        )
    with pytest.raises(ValidationError, match="content_hash"):
        StrategyStateEnvelope(
            strategy_id="router",
            strategy_schema_version=1,
            value=value,
            content_hash=HASH,
            byte_size=len(payload),
        )

    artifact = _full_artifact("strategy/router/state.json")
    with pytest.raises(ValidationError, match="content_hash"):
        StrategyStateEnvelope(
            strategy_id="router",
            strategy_schema_version=1,
            artifact_ref=artifact,
            content_hash="b" * 64,
            byte_size=artifact.byte_size,
        )


@pytest.mark.parametrize(
    "limit, reserved, consumed",
    [(-1, 0, 0), (5, -1, 0), (5, 0, -1), (5, 3, 3)],
)
def test_resource_budget_enforces_ledger_invariant(
    limit: int, reserved: int, consumed: int
) -> None:
    with pytest.raises(ValidationError):
        ResourceBudget(
            resource="calls",
            limit=limit,
            reserved=reserved,
            consumed=consumed,
        )


def test_budget_resource_names_and_state_ids_are_unique() -> None:
    duplicate_resource = ResourceBudget(resource="calls", limit=2)
    with pytest.raises(ValidationError, match="resource"):
        BudgetState(
            resources=(duplicate_resource, duplicate_resource),
            deadline_at=None,
            max_call_depth=1,
            max_concurrent_actions=1,
        )

    with pytest.raises(ValidationError, match="action_id"):
        _state(actions=(_action("duplicate"), _action("duplicate")))
    with pytest.raises(ValidationError, match="invocation_id"):
        _state(invocations=(_invocation("duplicate"), _invocation("duplicate")))


def test_attempt_revision_and_schema_version_are_positive() -> None:
    with pytest.raises(ValidationError):
        _state(revision=0)
    with pytest.raises(ValidationError):
        _state(schema_version=0)


def test_external_request_state_requires_exactly_one_valid_pending_boundary() -> None:
    minimal_additional = ExternalRequest(
        request_id="input-minimal",
        request_kind=ExternalRequestKind.ADDITIONAL_INPUT,
    )
    assert minimal_additional.action_id is None
    assert minimal_additional.payload_ref is None
    additional = ExternalRequest(
        request_id="input-1",
        request_kind=ExternalRequestKind.ADDITIONAL_INPUT,
        action_id=None,
        payload_ref=_full_artifact("attempts/a1/input-request.json"),
    )
    assert set(ExternalRequest.model_fields) == {
        "request_id",
        "request_kind",
        "action_id",
        "payload_ref",
    }
    assert _state(
        phase=AttemptPhase.WAITING_EXTERNAL,
        actions=(),
        invocations=(),
        pending_external=(additional,),
    ).pending_external == (additional,)

    with pytest.raises(ValidationError, match="pending external"):
        _state(phase=AttemptPhase.WAITING_EXTERNAL, pending_external=())
    with pytest.raises(ValidationError, match="pending external"):
        _state(pending_external=(additional,))
    with pytest.raises(ValidationError, match="pending external"):
        _state(
            phase=AttemptPhase.WAITING_EXTERNAL,
            actions=(),
            invocations=(),
            pending_external=(
                additional,
                additional.model_copy(update={"request_id": "input-2"}),
            ),
        )


def test_external_request_action_links_match_the_action_lifecycle() -> None:
    approval = ExternalRequest(
        request_id="approval-1",
        request_kind=ExternalRequestKind.ACTION_APPROVAL,
        action_id="action-1",
        payload_ref=None,
    )
    proposed = _action(status=ActionStatus.PROPOSED)
    waiting_approval = _state(
        phase=AttemptPhase.WAITING_EXTERNAL,
        actions=(proposed,),
        invocations=(),
        pending_external=(approval,),
    )
    assert waiting_approval.actions[0].status is ActionStatus.PROPOSED

    with pytest.raises(ValidationError, match="approval"):
        _state(
            phase=AttemptPhase.WAITING_EXTERNAL,
            pending_external=(approval,),
        )
    with pytest.raises(ValidationError, match="Action"):
        _state(
            phase=AttemptPhase.WAITING_EXTERNAL,
            actions=(),
            invocations=(),
            pending_external=(approval,),
        )

    unknown = _terminal_send_message_action(ActionStatus.OUTCOME_UNKNOWN)
    reconciliation = ExternalRequest(
        request_id="reconcile-1",
        request_kind=ExternalRequestKind.OUTCOME_RECONCILIATION,
        action_id="action-1",
        payload_ref=None,
    )
    waiting_reconciliation = _state(
        phase=AttemptPhase.WAITING_EXTERNAL,
        actions=(unknown,),
        invocations=(),
        pending_external=(reconciliation,),
    )
    assert waiting_reconciliation.actions[0].status is ActionStatus.OUTCOME_UNKNOWN

    reconciled = unknown.model_copy(
        update={
            "reconciled_status": ActionStatus.FAILED,
            "reconciled_error": ErrorSummary(
                code="CONFIRMED_FAILED",
                retryable=False,
                safe_message="The Action was confirmed failed.",
            ),
        }
    )
    with pytest.raises(ValidationError, match="reconciliation"):
        _state(
            phase=AttemptPhase.WAITING_EXTERNAL,
            actions=(reconciled,),
            invocations=(),
            pending_external=(reconciliation,),
        )


def _terminal_error() -> ErrorSummary:
    return ErrorSummary(
        code="TERMINAL_FAILURE",
        retryable=False,
        safe_message="The Attempt did not complete.",
    )


def _valid_lifecycle_fields(phase: AttemptPhase) -> dict[str, object]:
    values: dict[str, object] = {
        "started_at": None,
        "finished_at": None,
        "result_ref": None,
        "terminal_error": None,
    }
    if phase is AttemptPhase.PLANNED:
        return values

    values["started_at"] = UTC_NOW
    if phase in {
        AttemptPhase.SUCCEEDED,
        AttemptPhase.FAILED,
        AttemptPhase.CANCELLED,
        AttemptPhase.TIMED_OUT,
        AttemptPhase.INTERRUPTED,
    }:
        values["finished_at"] = UTC_NOW + timedelta(minutes=1)
    if phase is AttemptPhase.SUCCEEDED:
        values["result_ref"] = _full_artifact("attempts/a1/result.json")
    if phase in {
        AttemptPhase.FAILED,
        AttemptPhase.TIMED_OUT,
        AttemptPhase.INTERRUPTED,
    }:
        values["terminal_error"] = _terminal_error()
    return values


def _terminal_action_fields(phase: AttemptPhase) -> dict[str, object]:
    if phase not in {
        AttemptPhase.SUCCEEDED,
        AttemptPhase.FAILED,
        AttemptPhase.CANCELLED,
        AttemptPhase.TIMED_OUT,
        AttemptPhase.INTERRUPTED,
    }:
        return {}
    return {
        "actions": (_action(status=ActionStatus.SUCCEEDED),),
        "invocations": (_invocation(status="COMPLETED"),),
    }


def _terminal_send_message_action(status: ActionStatus) -> ActionState:
    has_acceptance = status is not ActionStatus.REJECTED
    is_succeeded = status is ActionStatus.SUCCEEDED
    error = (
        None
        if is_succeeded
        else ErrorSummary(
            code=f"{status.value}_ACTION",
            retryable=False,
            safe_message="The Action reached a terminal state.",
        )
    )
    action_fields: dict[str, object] = {
        "action_id": "action-1",
        "action_type": "SEND_MESSAGE",
        "actor_id": "engine",
        "target_ids": ("agent-a",),
        "status": status,
        "causal_parent_id": None,
        "invocation_id": None,
        "payload_ref": _full_artifact("attempts/a1/actions/action-1.json"),
        "result_ref": (
            _full_artifact("attempts/a1/actions/action-1-result.json")
            if is_succeeded
            else None
        ),
        "idempotency_key": "idem-action-1" if has_acceptance else None,
        "recovery_policy": RecoveryPolicy.REPLAY_SAFE,
        "reservation_id": "reservation-1" if has_acceptance else None,
        "retry_of_action_id": None,
        "error_code": None if error is None else error.code,
    }
    # Task 4 extends the schema; Task 2's commit boundary must use its earlier fields.
    if "error" in ActionState.model_fields:
        action_fields["error"] = error
    return ActionState(**action_fields)


@pytest.mark.parametrize("phase", list(AttemptPhase))
def test_attempt_lifecycle_accepts_only_complete_phase_shapes(
    phase: AttemptPhase,
) -> None:
    state = _state(
        phase=phase,
        **_terminal_action_fields(phase),
        **_valid_lifecycle_fields(phase),
    )

    assert state.phase is phase


@pytest.mark.parametrize(
    "phase",
    [
        AttemptPhase.SUCCEEDED,
        AttemptPhase.FAILED,
        AttemptPhase.CANCELLED,
        AttemptPhase.TIMED_OUT,
        AttemptPhase.INTERRUPTED,
    ],
)
@pytest.mark.parametrize(
    "action_status",
    [
        ActionStatus.REJECTED,
        ActionStatus.SUCCEEDED,
        ActionStatus.FAILED,
        ActionStatus.TIMED_OUT,
        ActionStatus.CANCELLED,
        ActionStatus.OUTCOME_UNKNOWN,
    ],
)
def test_terminal_attempt_accepts_every_terminal_action_status(
    phase: AttemptPhase,
    action_status: ActionStatus,
) -> None:
    state = _state(
        phase=phase,
        actions=(_terminal_send_message_action(action_status),),
        invocations=(),
        **_valid_lifecycle_fields(phase),
    )

    assert state.phase is phase
    assert state.actions[0].status is action_status


@pytest.mark.parametrize(
    "phase",
    [
        AttemptPhase.SUCCEEDED,
        AttemptPhase.FAILED,
        AttemptPhase.CANCELLED,
        AttemptPhase.TIMED_OUT,
        AttemptPhase.INTERRUPTED,
    ],
)
@pytest.mark.parametrize(
    "action_status",
    [ActionStatus.PROPOSED, ActionStatus.ACCEPTED, ActionStatus.STARTED],
)
def test_persisted_terminal_attempt_rejects_nonterminal_action(
    phase: AttemptPhase,
    action_status: ActionStatus,
) -> None:
    active = _state(
        actions=(_action(status=action_status),),
        invocations=(_invocation(),),
    )
    snapshot = {
        **active.model_dump(mode="python"),
        "phase": phase,
        **_valid_lifecycle_fields(phase),
    }

    with pytest.raises(ValidationError, match="terminal Action"):
        AttemptState.model_validate(snapshot)


@pytest.mark.parametrize(
    ("phase", "invalid_fields"),
    [
        (AttemptPhase.PLANNED, {"started_at": UTC_NOW}),
        (AttemptPhase.PLANNED, {"finished_at": UTC_NOW}),
        (
            AttemptPhase.PLANNED,
            {"result_ref": _full_artifact("attempts/a1/result.json")},
        ),
        (AttemptPhase.PLANNED, {"terminal_error": _terminal_error()}),
        (AttemptPhase.RUNNING, {"started_at": None}),
        (AttemptPhase.RUNNING, {"terminal_error": _terminal_error()}),
        (AttemptPhase.PAUSE_REQUESTED, {"started_at": None}),
        (AttemptPhase.PAUSE_REQUESTED, {"finished_at": UTC_NOW}),
        (AttemptPhase.PAUSED, {"result_ref": _full_artifact("attempts/a1/result.json")}),
        (AttemptPhase.WAITING_EXTERNAL, {"terminal_error": _terminal_error()}),
        (AttemptPhase.CANCEL_REQUESTED, {"started_at": None}),
        (AttemptPhase.SUCCEEDED, {"started_at": None}),
        (AttemptPhase.SUCCEEDED, {"finished_at": None}),
        (AttemptPhase.SUCCEEDED, {"result_ref": None}),
        (AttemptPhase.SUCCEEDED, {"terminal_error": _terminal_error()}),
        (AttemptPhase.FAILED, {"started_at": None}),
        (AttemptPhase.FAILED, {"finished_at": None}),
        (AttemptPhase.FAILED, {"terminal_error": None}),
        (
            AttemptPhase.FAILED,
            {"result_ref": _full_artifact("attempts/a1/result.json")},
        ),
        (AttemptPhase.TIMED_OUT, {"terminal_error": None}),
        (AttemptPhase.INTERRUPTED, {"finished_at": None}),
        (AttemptPhase.CANCELLED, {"started_at": None}),
        (AttemptPhase.CANCELLED, {"finished_at": None}),
        (
            AttemptPhase.CANCELLED,
            {"result_ref": _full_artifact("attempts/a1/result.json")},
        ),
        (AttemptPhase.CANCELLED, {"terminal_error": _terminal_error()}),
    ],
)
def test_attempt_lifecycle_rejects_incomplete_or_contradictory_phase_shapes(
    phase: AttemptPhase,
    invalid_fields: dict[str, object],
) -> None:
    fields = {**_valid_lifecycle_fields(phase), **invalid_fields}

    with pytest.raises(ValidationError, match="lifecycle"):
        _state(phase=phase, **_terminal_action_fields(phase), **fields)


def test_attempt_lifecycle_rejects_finish_before_start() -> None:
    fields = _valid_lifecycle_fields(AttemptPhase.SUCCEEDED)
    fields["finished_at"] = UTC_NOW - timedelta(seconds=1)

    with pytest.raises(ValidationError, match="finished_at"):
        _state(
            phase=AttemptPhase.SUCCEEDED,
            **_terminal_action_fields(AttemptPhase.SUCCEEDED),
            **fields,
        )


def test_strategy_view_exposes_only_authorized_fields_and_freezes_external_data() -> None:
    state = _state()
    topology = {"agent-a": ["agent-b"]}
    event = {"event_type": "ACTION_ACCEPTED", "payload": {"ids": ["action-1"]}}
    view = to_strategy_view(
        state,
        legal_topology=topology,
        visible_artifacts=[state.manifest_ref],
        latest_committed_event=event,
    )

    assert isinstance(view, StrategyView)
    assert set(view.model_dump()) == {
        "attempt_id",
        "strategy_id",
        "revision",
        "remaining_budget",
        "legal_topology",
        "actions",
        "invocations",
        "visible_artifacts",
        "latest_committed_event",
    }
    assert view.attempt_id == state.attempt_id
    assert view.strategy_id == state.strategy_id
    assert view.revision == state.revision
    assert view.remaining_budget == (("actions", 5), ("tokens", 650))
    assert "manifest_ref" not in view.model_dump()
    assert "strategy" not in view.model_dump()

    topology["agent-a"].append("agent-secret")
    event["payload"]["ids"].append("action-secret")
    assert view.legal_topology["agent-a"] == ("agent-b",)
    assert view.latest_committed_event["payload"]["ids"] == ("action-1",)


def test_agent_view_exposes_only_authorized_frozen_inputs() -> None:
    current_task = {"task_id": "task-1", "steps": ["inspect"]}
    agent_definition = {"agent_id": "agent-a", "tools": ["search"]}
    authorized_context = {"facts": [{"value": "public"}]}
    artifact_refs = [_full_artifact("attempts/a1/agent/context.json")]

    view = to_agent_view(
        current_task=current_task,
        agent_definition=agent_definition,
        authorized_context=authorized_context,
        artifact_refs=artifact_refs,
    )

    assert isinstance(view, AgentView)
    assert set(view.model_dump()) == {
        "current_task",
        "agent_definition",
        "authorized_context",
        "artifact_refs",
    }
    current_task["steps"].append("leak")
    agent_definition["tools"].append("secret-tool")
    authorized_context["facts"][0]["value"] = "changed"
    artifact_refs.append(_full_artifact("attempts/a1/agent/other.json"))
    assert view.current_task["steps"] == ("inspect",)
    assert view.agent_definition["tools"] == ("search",)
    assert view.authorized_context["facts"][0]["value"] == "public"
    assert len(view.artifact_refs) == 1


def test_operator_view_exposes_only_control_plane_summary() -> None:
    state = _state()
    view = to_operator_view(state)

    assert isinstance(view, OperatorView)
    assert set(view.model_dump()) == {
        "phase",
        "revision",
        "progress",
        "budget",
        "pending_external",
        "actions",
        "failures",
    }
    assert view.progress == state.invocations
    assert view.failures == ()
    assert "strategy" not in view.model_dump()
    assert "manifest_ref" not in view.model_dump()
    assert "result_ref" not in view.model_dump()
