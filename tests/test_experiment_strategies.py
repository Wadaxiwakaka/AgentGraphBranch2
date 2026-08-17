from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone
from hashlib import sha256
from types import MappingProxyType
from uuid import UUID

import pytest
from pydantic import ValidationError

from experiment_system.actions import (
    ActionCancelledOutcome,
    ActionFailedOutcome,
    ActionStatus,
    ActionSucceededOutcome,
    ActionTimedOutOutcome,
    ActionType,
    ActionUnknownOutcome,
    NormalizedAction,
)
from experiment_system.backends.deterministic import (
    ScriptedBackend,
    ScriptedBackendError,
)
from experiment_system.commands import (
    ApplyStrategyDecision,
    strategy_action_id,
    strategy_invocation_id,
)
from experiment_system.contract import (
    ExecutionContext,
    StrategyDecision,
    StrategyDirective,
)
from experiment_system.events import (
    ActionCancelled,
    ActionFailed,
    ActionOutcomeReconciled,
    ActionOutcomeUnknown,
    ActionRejected,
    ActionStarted,
    ActionSucceeded,
    ActionTimedOut,
    AttemptStarted,
    BudgetSettled,
    StrategyDecisionRecorded,
    is_strategy_trigger,
)
from experiment_system.state import (
    ArtifactRef,
    ErrorSummary,
    RecoveryPolicy,
    StrategyStateEnvelope,
    StrategyView,
)
from experiment_system.strategies.single_agent import SingleAgentStrategy
from experiment_system.strategies.static_workflow import StaticWorkflowStrategy


NOW = datetime(2026, 7, 23, 8, 30, tzinfo=timezone.utc)
ATTEMPT_ID = "attempt-1"
STRATEGY_ID = "single-agent"
EVENT_ID = UUID("00000000-0000-0000-0000-000000000002")
COMMAND_ID = UUID("10000000-0000-0000-0000-000000000002")
PARENT_ID = UUID("00000000-0000-0000-0000-000000000001")


def _artifact(name: str) -> ArtifactRef:
    return ArtifactRef(
        capture_class="full",
        content_hash=sha256(name.encode()).hexdigest(),
        media_type="application/json",
        byte_size=len(name),
        relative_path=f"attempts/{ATTEMPT_ID}/{name}.json",
    )


def _envelope(sequence_no: int, event_type: str) -> dict[str, object]:
    return {
        "schema_version": 1,
        "event_id": EVENT_ID,
        "trial_id": "trial-1",
        "attempt_id": ATTEMPT_ID,
        "sequence_no": sequence_no,
        "event_type": event_type,
        "command_id": COMMAND_ID,
        "causal_parent_id": PARENT_ID,
        "logical_time": sequence_no,
        "wall_time_utc": NOW,
    }


def _started(sequence_no: int = 2) -> AttemptStarted:
    return AttemptStarted(**_envelope(sequence_no, "ATTEMPT_STARTED"))


def _strategy_envelope(
    stage: str = "READY",
    *,
    version: object = 1,
) -> StrategyStateEnvelope:
    value = {"stage": stage, "version": version}
    payload = json.dumps(
        value, sort_keys=True, separators=(",", ":")
    ).encode("utf-8")
    return StrategyStateEnvelope(
        strategy_id=STRATEGY_ID,
        strategy_schema_version=1,
        value=value,
        content_hash=sha256(payload).hexdigest(),
        byte_size=len(payload),
    )


def _view(event: object | None = None, *, revision: int = 2) -> StrategyView:
    latest = event or _started()
    return StrategyView(
        attempt_id=ATTEMPT_ID,
        strategy_id=STRATEGY_ID,
        revision=revision,
        remaining_budget=(("model_calls", 1),),
        legal_topology={"single-agent": ["worker-a"]},
        actions=(),
        invocations=(),
        visible_artifacts=(),
        latest_committed_event=latest.model_dump(mode="json"),
    )


def _error(code: str = "BACKEND_SECRET") -> ErrorSummary:
    return ErrorSummary(
        code=code,
        retryable=False,
        safe_message="Backend detail must not enter private strategy state.",
    )


def _normalized_action(action_id: str = "action-1") -> NormalizedAction:
    return NormalizedAction(
        action_id=action_id,
        action_type=ActionType.INVOKE_AGENT,
        actor=STRATEGY_ID,
        target_ids=("worker-a",),
        invocation_id="invocation-1",
        causal_parent_id=str(PARENT_ID),
        payload_ref=_artifact("payload"),
        recovery_policy=RecoveryPolicy.REPLAY_SAFE,
        requested_timeout=30,
        call_depth=0,
        resource_requests=(),
        reservation_id="reservation-1",
        idempotency_key="idem-1",
    )


def _context(action: NormalizedAction) -> ExecutionContext:
    return ExecutionContext(
        attempt_id=ATTEMPT_ID,
        action_id=action.action_id,
        invocation_id=action.invocation_id,
        idempotency_key=action.idempotency_key,
        deadline_at=NOW + timedelta(minutes=1),
        cancellation_requested=False,
    )


def test_strategy_decision_requires_exact_directive_enum() -> None:
    with pytest.raises(ValidationError):
        StrategyDecision(
            trigger_sequence_no=2,
            strategy=_strategy_envelope(),
            proposals=(),
            directive="CONTINUE",  # type: ignore[arg-type]
        )


def test_execution_context_accepts_utc_datetime_or_no_deadline() -> None:
    action = _normalized_action()
    values = _context(action).model_dump(mode="python")

    assert ExecutionContext(**values).deadline_at == NOW + timedelta(minutes=1)
    assert ExecutionContext(**{**values, "deadline_at": None}).deadline_at is None


@pytest.mark.parametrize(
    "deadline",
    [
        "2026-07-23T08:30:00Z",
        datetime(2026, 7, 23, 8, 30),
        datetime(2026, 7, 23, 16, 30, tzinfo=timezone(timedelta(hours=8))),
    ],
)
def test_execution_context_rejects_coerced_or_non_utc_deadline(
    deadline: object,
) -> None:
    values = _context(_normalized_action()).model_dump(mode="python")

    with pytest.raises(ValidationError):
        ExecutionContext(**{**values, "deadline_at": deadline})


@pytest.mark.parametrize(
    "changes",
    [
        {"directive": StrategyDirective.CONTINUE, "result_ref": _artifact("result")},
        {"directive": StrategyDirective.CONTINUE, "error": _error()},
        {"directive": StrategyDirective.SUCCEED, "result_ref": None},
        {
            "directive": StrategyDirective.SUCCEED,
            "result_ref": _artifact("result"),
            "error": _error(),
        },
        {"directive": StrategyDirective.SUCCEED, "proposals": (_normalized_action(),)},
        {"directive": StrategyDirective.FAIL, "error": None},
        {"directive": StrategyDirective.FAIL, "result_ref": _artifact("result")},
        {
            "directive": StrategyDirective.FAIL,
            "error": _error(),
            "proposals": (_normalized_action(),),
        },
    ],
)
def test_strategy_decision_independently_rejects_invalid_directive_shapes(
    changes: dict[str, object],
) -> None:
    values: dict[str, object] = {
        "trigger_sequence_no": 2,
        "strategy": _strategy_envelope(),
        "proposals": (),
        "directive": StrategyDirective.CONTINUE,
        "result_ref": None,
        "error": None,
    }
    values.update(changes)

    with pytest.raises(ValidationError, match="directive"):
        StrategyDecision(**values)


@pytest.mark.parametrize("model", [ApplyStrategyDecision, StrategyDecisionRecorded])
@pytest.mark.parametrize(
    "changes",
    [
        {"directive": StrategyDirective.CONTINUE, "error": _error()},
        {"directive": StrategyDirective.SUCCEED, "result_ref": None},
        {
            "directive": StrategyDirective.FAIL,
            "result_ref": None,
            "error": None,
        },
    ],
)
def test_persisted_decision_layers_independently_reject_invalid_directive_shapes(
    model: type[object],
    changes: dict[str, object],
) -> None:
    common = {
        **_envelope(3, "STRATEGY_DECISION_RECORDED"),
        "trigger_sequence_no": 2,
        "strategy": _strategy_envelope(),
        "proposals": (),
        "directive": StrategyDirective.CONTINUE,
        "result_ref": None,
        "error": None,
    }
    common.update(changes)
    if model is ApplyStrategyDecision:
        common = {
            "schema_version": 1,
            "command_type": "APPLY_STRATEGY_DECISION",
            "command_id": COMMAND_ID,
            "attempt_id": ATTEMPT_ID,
            "expected_revision": 2,
            **{key: value for key, value in common.items() if key not in _envelope(3, "x")},
        }

    with pytest.raises(ValidationError, match="directive"):
        model(**common)  # type: ignore[call-arg]


def test_strategy_trigger_predicate_is_narrow_and_excludes_bookkeeping() -> None:
    started = _started()
    action = _normalized_action()
    succeeded = ActionSucceeded(
        **_envelope(3, "ACTION_SUCCEEDED"),
        action_id=action.action_id,
        outcome=ActionSucceededOutcome(
            status=ActionStatus.SUCCEEDED,
            action_id=action.action_id,
            result_ref=_artifact("result"),
        ),
    )
    decision = StrategyDecisionRecorded(
        **_envelope(4, "STRATEGY_DECISION_RECORDED"),
        trigger_sequence_no=3,
        strategy=_strategy_envelope("DONE"),
        proposals=(),
        directive=StrategyDirective.SUCCEED,
        result_ref=_artifact("result"),
    )

    assert is_strategy_trigger(started)
    assert is_strategy_trigger(succeeded)
    assert not is_strategy_trigger(decision)
    assert not is_strategy_trigger(
        ActionStarted(
            **_envelope(5, "ACTION_STARTED"),
            action_id=action.action_id,
        )
    )
    assert not is_strategy_trigger(
        BudgetSettled.model_construct(event_type="BUDGET_SETTLED")
    )


def test_single_agent_initialize_is_canonical_and_uses_stable_identities() -> None:
    strategy = SingleAgentStrategy(
        strategy_id=STRATEGY_ID,
        agent_id="worker-a",
        payload_ref=_artifact("payload"),
        requested_timeout=30,
    )

    first = strategy.initialize(_view())
    second = strategy.initialize(_view())

    assert first.model_dump_json() == second.model_dump_json()
    assert first.directive is StrategyDirective.CONTINUE
    assert first.trigger_sequence_no == 2
    assert len(first.proposals) == 1
    proposal = first.proposals[0]
    assert proposal.action_id == strategy_action_id(ATTEMPT_ID, STRATEGY_ID, 0)
    assert proposal.invocation_id == strategy_invocation_id(ATTEMPT_ID, STRATEGY_ID, 0)
    assert proposal.action_type is ActionType.INVOKE_AGENT
    assert proposal.target_ids == ("worker-a",)
    assert first.strategy.value == {"stage": "WAITING_FOR_INVOCATION", "version": 1}


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("strategy_id", None),
        ("strategy_id", 1),
        ("strategy_id", ""),
        ("strategy_id", "invalid strategy"),
        ("strategy_id", "-invalid"),
        ("agent_id", None),
        ("agent_id", 1),
        ("agent_id", ""),
        ("agent_id", "invalid/agent"),
    ],
)
def test_single_agent_eagerly_rejects_non_stable_identity(
    field: str,
    value: object,
) -> None:
    arguments: dict[str, object] = {
        "strategy_id": STRATEGY_ID,
        "agent_id": "worker-a",
        "payload_ref": None,
    }
    arguments[field] = value

    with pytest.raises(ValueError, match=field):
        SingleAgentStrategy(**arguments)  # type: ignore[arg-type]


def test_single_agent_ignores_unrelated_action_and_completes_exact_action() -> None:
    strategy = SingleAgentStrategy(
        strategy_id=STRATEGY_ID,
        agent_id="worker-a",
        payload_ref=_artifact("payload"),
    )
    initialized = strategy.initialize(_view())
    action_id = initialized.proposals[0].action_id
    unrelated = ActionFailed(
        **_envelope(3, "ACTION_FAILED"),
        action_id="other-action",
        outcome=ActionFailedOutcome(
            status=ActionStatus.FAILED,
            action_id="other-action",
            error=_error(),
        ),
    )

    ignored = strategy.on_event(initialized.strategy, unrelated, _view(unrelated, revision=3))

    assert ignored.directive is StrategyDirective.CONTINUE
    assert ignored.proposals == ()
    assert ignored.strategy == initialized.strategy

    result_ref = _artifact("agent-result")
    succeeded = ActionSucceeded(
        **_envelope(4, "ACTION_SUCCEEDED"),
        action_id=action_id,
        outcome=ActionSucceededOutcome(
            status=ActionStatus.SUCCEEDED,
            action_id=action_id,
            result_ref=result_ref,
        ),
    )
    completed = strategy.on_event(
        ignored.strategy,
        succeeded,
        _view(succeeded, revision=4),
    )

    assert completed.directive is StrategyDirective.SUCCEED
    assert completed.result_ref == result_ref
    assert completed.proposals == ()
    assert completed.strategy.value == {"stage": "DONE", "version": 1}


def test_single_agent_normalizes_failure_without_copying_backend_details() -> None:
    strategy = SingleAgentStrategy(
        strategy_id=STRATEGY_ID,
        agent_id="worker-a",
        payload_ref=None,
    )
    initialized = strategy.initialize(_view())
    action_id = initialized.proposals[0].action_id
    failed = ActionFailed(
        **_envelope(3, "ACTION_FAILED"),
        action_id=action_id,
        outcome=ActionFailedOutcome(
            status=ActionStatus.FAILED,
            action_id=action_id,
            error=_error("PROVIDER_CREDENTIAL_LEAK"),
        ),
    )

    decision = strategy.on_event(initialized.strategy, failed, _view(failed, revision=3))

    assert decision.directive is StrategyDirective.FAIL
    assert decision.error is not None
    assert decision.error.code == "SINGLE_AGENT_ACTION_FAILED"
    assert "PROVIDER" not in decision.model_dump_json()
    assert decision.strategy.value == {"stage": "DONE", "version": 1}


def test_single_agent_succeeds_from_reconciled_success_outcome() -> None:
    strategy = SingleAgentStrategy(
        strategy_id=STRATEGY_ID,
        agent_id="worker-a",
        payload_ref=None,
    )
    initialized = strategy.initialize(_view())
    action_id = initialized.proposals[0].action_id
    result_ref = _artifact("reconciled-agent-result")
    reconciled = ActionOutcomeReconciled(
        **_envelope(3, "ACTION_OUTCOME_RECONCILED"),
        action_id=action_id,
        outcome=ActionSucceededOutcome(
            status=ActionStatus.SUCCEEDED,
            action_id=action_id,
            result_ref=result_ref,
        ),
    )

    decision = strategy.on_event(
        initialized.strategy,
        reconciled,
        _view(reconciled, revision=3),
    )

    assert decision.trigger_sequence_no == reconciled.sequence_no
    assert decision.directive is StrategyDirective.SUCCEED
    assert decision.result_ref == result_ref
    assert decision.proposals == ()
    assert decision.strategy.value == {"stage": "DONE", "version": 1}


def test_single_agent_fails_safely_from_reconciled_failed_outcome() -> None:
    strategy = SingleAgentStrategy(
        strategy_id=STRATEGY_ID,
        agent_id="worker-a",
        payload_ref=None,
    )
    initialized = strategy.initialize(_view())
    action_id = initialized.proposals[0].action_id
    reconciled = ActionOutcomeReconciled(
        **_envelope(3, "ACTION_OUTCOME_RECONCILED"),
        action_id=action_id,
        outcome=ActionFailedOutcome(
            status=ActionStatus.FAILED,
            action_id=action_id,
            error=_error("RECONCILED_PROVIDER_DETAIL"),
        ),
    )

    decision = strategy.on_event(
        initialized.strategy,
        reconciled,
        _view(reconciled, revision=3),
    )

    assert decision.trigger_sequence_no == reconciled.sequence_no
    assert decision.directive is StrategyDirective.FAIL
    assert decision.error is not None
    assert decision.error.code == "SINGLE_AGENT_ACTION_FAILED"
    assert "RECONCILED_PROVIDER_DETAIL" not in decision.model_dump_json()
    assert decision.proposals == ()
    assert decision.strategy.value == {"stage": "DONE", "version": 1}


@pytest.mark.parametrize(
    ("event_class", "event_type", "outcome_class", "status"),
    [
        (ActionFailed, "ACTION_FAILED", ActionFailedOutcome, ActionStatus.FAILED),
        (ActionTimedOut, "ACTION_TIMED_OUT", ActionTimedOutOutcome, ActionStatus.TIMED_OUT),
        (ActionCancelled, "ACTION_CANCELLED", ActionCancelledOutcome, ActionStatus.CANCELLED),
        (
            ActionOutcomeUnknown,
            "ACTION_OUTCOME_UNKNOWN",
            ActionUnknownOutcome,
            ActionStatus.OUTCOME_UNKNOWN,
        ),
    ],
)
def test_single_agent_safely_fails_for_every_terminal_non_success(
    event_class: type[object],
    event_type: str,
    outcome_class: type[object],
    status: ActionStatus,
) -> None:
    strategy = SingleAgentStrategy(
        strategy_id=STRATEGY_ID,
        agent_id="worker-a",
        payload_ref=None,
    )
    initialized = strategy.initialize(_view())
    action_id = initialized.proposals[0].action_id
    outcome = outcome_class(status=status, action_id=action_id, error=_error())
    event = event_class(
        **_envelope(3, event_type),
        action_id=action_id,
        outcome=outcome,
    )

    decision = strategy.on_event(initialized.strategy, event, _view(event, revision=3))

    assert decision.directive is StrategyDirective.FAIL
    assert decision.error is not None
    assert decision.error.code == "SINGLE_AGENT_ACTION_FAILED"


def test_single_agent_matching_rejection_fails_and_revalidates_private_envelope() -> None:
    strategy = SingleAgentStrategy(
        strategy_id=STRATEGY_ID,
        agent_id="worker-a",
        payload_ref=None,
    )
    initialized = strategy.initialize(_view())
    action_id = initialized.proposals[0].action_id
    rejected = ActionRejected(
        **_envelope(3, "ACTION_REJECTED"),
        action_id=action_id,
        error=_error(),
    )

    decision = strategy.on_event(
        initialized.strategy,
        rejected,
        _view(rejected, revision=3),
    )

    assert decision.directive is StrategyDirective.FAIL
    with pytest.raises(ValidationError, match="content_hash"):
        strategy.on_event(
            initialized.strategy.model_copy(update={"content_hash": "0" * 64}),
            rejected,
            _view(rejected, revision=3),
        )


@pytest.mark.parametrize("version", [True, 1.0])
def test_single_agent_rejects_non_integer_private_state_version(version: object) -> None:
    strategy = SingleAgentStrategy(
        strategy_id=STRATEGY_ID,
        agent_id="worker-a",
        payload_ref=None,
    )
    unrelated = ActionFailed(
        **_envelope(3, "ACTION_FAILED"),
        action_id="other-action",
        outcome=ActionFailedOutcome(
            status=ActionStatus.FAILED,
            action_id="other-action",
            error=_error(),
        ),
    )

    with pytest.raises(ValueError, match="state envelope"):
        strategy.on_event(
            _strategy_envelope("WAITING_FOR_INVOCATION", version=version),
            unrelated,
            _view(unrelated, revision=3),
        )


def _workflow_view(event: object | None = None, *, revision: int = 2) -> StrategyView:
    latest = event or _started()
    return StrategyView(
        attempt_id=ATTEMPT_ID,
        strategy_id="static-workflow",
        revision=revision,
        remaining_budget=(("model_calls", 2),),
        legal_topology={"static-workflow": ["worker-a", "worker-a"]},
        actions=(),
        invocations=(),
        visible_artifacts=(),
        latest_committed_event=latest.model_dump(mode="json"),
    )


def test_static_workflow_preserves_repeated_roles_and_advances_strictly() -> None:
    strategy = StaticWorkflowStrategy(
        strategy_id="static-workflow",
        agent_ids=("worker-a", "worker-a"),
        payload_ref=_artifact("payload"),
        requested_timeout=30,
    )

    initialized = strategy.initialize(_workflow_view())
    assert initialized.model_dump_json() == strategy.initialize(
        _workflow_view()
    ).model_dump_json()
    assert len(initialized.proposals) == 1
    first = initialized.proposals[0]
    assert first.action_id == strategy_action_id(
        ATTEMPT_ID, "static-workflow", 0
    )
    assert first.invocation_id == strategy_invocation_id(
        ATTEMPT_ID, "static-workflow", 0
    )
    assert first.target_ids == ("worker-a",)
    assert initialized.strategy.value == {
        "current_action_id": first.action_id,
        "current_invocation_id": first.invocation_id,
        "next_index": 1,
        "stage": "WAITING_FOR_INVOCATION",
        "version": 1,
    }

    first_result = _artifact("first-result")
    first_succeeded = ActionSucceeded(
        **_envelope(3, "ACTION_SUCCEEDED"),
        action_id=first.action_id,
        outcome=ActionSucceededOutcome(
            status=ActionStatus.SUCCEEDED,
            action_id=first.action_id,
            result_ref=first_result,
        ),
    )
    advanced = strategy.on_event(
        initialized.strategy,
        first_succeeded,
        _workflow_view(first_succeeded, revision=3),
    )

    assert advanced.directive is StrategyDirective.CONTINUE
    assert len(advanced.proposals) == 1
    second = advanced.proposals[0]
    assert second.action_id == strategy_action_id(
        ATTEMPT_ID, "static-workflow", 1
    )
    assert second.invocation_id == strategy_invocation_id(
        ATTEMPT_ID, "static-workflow", 1
    )
    assert second.target_ids == ("worker-a",)
    assert second.action_id != first.action_id
    assert second.invocation_id != first.invocation_id

    second_result = _artifact("second-result")
    second_succeeded = ActionSucceeded(
        **_envelope(4, "ACTION_SUCCEEDED"),
        action_id=second.action_id,
        outcome=ActionSucceededOutcome(
            status=ActionStatus.SUCCEEDED,
            action_id=second.action_id,
            result_ref=second_result,
        ),
    )
    completed = strategy.on_event(
        advanced.strategy,
        second_succeeded,
        _workflow_view(second_succeeded, revision=4),
    )

    assert completed.directive is StrategyDirective.SUCCEED
    assert completed.result_ref == second_result
    assert completed.proposals == ()


def test_static_workflow_reconciled_success_advances_and_completes() -> None:
    strategy = StaticWorkflowStrategy(
        strategy_id="static-workflow",
        agent_ids=("worker-a", "worker-b"),
        payload_ref=None,
    )
    initialized = strategy.initialize(_workflow_view())
    first = initialized.proposals[0]
    first_reconciled = ActionOutcomeReconciled(
        **_envelope(3, "ACTION_OUTCOME_RECONCILED"),
        action_id=first.action_id,
        outcome=ActionSucceededOutcome(
            status=ActionStatus.SUCCEEDED,
            action_id=first.action_id,
            result_ref=_artifact("reconciled-first-result"),
        ),
    )

    advanced = strategy.on_event(
        initialized.strategy,
        first_reconciled,
        _workflow_view(first_reconciled, revision=3),
    )

    assert advanced.trigger_sequence_no == first_reconciled.sequence_no
    assert advanced.directive is StrategyDirective.CONTINUE
    assert len(advanced.proposals) == 1
    second = advanced.proposals[0]
    assert second.action_id == strategy_action_id(
        ATTEMPT_ID, "static-workflow", 1
    )
    assert second.invocation_id == strategy_invocation_id(
        ATTEMPT_ID, "static-workflow", 1
    )
    assert second.causal_parent_id == str(first_reconciled.event_id)
    assert advanced.strategy.value == {
        "current_action_id": second.action_id,
        "current_invocation_id": second.invocation_id,
        "next_index": 2,
        "stage": "WAITING_FOR_INVOCATION",
        "version": 1,
    }

    final_result = _artifact("reconciled-final-result")
    second_reconciled = ActionOutcomeReconciled(
        **_envelope(4, "ACTION_OUTCOME_RECONCILED"),
        action_id=second.action_id,
        outcome=ActionSucceededOutcome(
            status=ActionStatus.SUCCEEDED,
            action_id=second.action_id,
            result_ref=final_result,
        ),
    )
    completed = strategy.on_event(
        advanced.strategy,
        second_reconciled,
        _workflow_view(second_reconciled, revision=4),
    )

    assert completed.trigger_sequence_no == second_reconciled.sequence_no
    assert completed.directive is StrategyDirective.SUCCEED
    assert completed.result_ref == final_result
    assert completed.proposals == ()
    assert completed.strategy.value == {
        "current_action_id": second.action_id,
        "current_invocation_id": second.invocation_id,
        "next_index": 2,
        "stage": "DONE",
        "version": 1,
    }


def test_static_workflow_reconciled_failure_preserves_cursor_and_fails_safely() -> None:
    strategy = StaticWorkflowStrategy(
        strategy_id="static-workflow",
        agent_ids=("worker-a", "worker-b"),
        payload_ref=None,
    )
    initialized = strategy.initialize(_workflow_view())
    current = initialized.proposals[0]
    reconciled = ActionOutcomeReconciled(
        **_envelope(3, "ACTION_OUTCOME_RECONCILED"),
        action_id=current.action_id,
        outcome=ActionFailedOutcome(
            status=ActionStatus.FAILED,
            action_id=current.action_id,
            error=_error("RECONCILED_WORKFLOW_PROVIDER_DETAIL"),
        ),
    )

    decision = strategy.on_event(
        initialized.strategy,
        reconciled,
        _workflow_view(reconciled, revision=3),
    )

    assert decision.trigger_sequence_no == reconciled.sequence_no
    assert decision.directive is StrategyDirective.FAIL
    assert decision.error is not None
    assert decision.error.code == "STATIC_WORKFLOW_ACTION_FAILED"
    assert "RECONCILED_WORKFLOW_PROVIDER_DETAIL" not in decision.model_dump_json()
    assert decision.proposals == ()
    assert decision.strategy.value == {
        "current_action_id": current.action_id,
        "current_invocation_id": current.invocation_id,
        "next_index": 1,
        "stage": "DONE",
        "version": 1,
    }


@pytest.mark.parametrize(
    "strategy_id",
    [None, 1, "", "invalid strategy", "-invalid", "invalid/strategy"],
)
def test_static_workflow_eagerly_rejects_non_stable_strategy_id(
    strategy_id: object,
) -> None:
    with pytest.raises(ValueError, match="strategy_id"):
        StaticWorkflowStrategy(
            strategy_id=strategy_id,  # type: ignore[arg-type]
            agent_ids=("worker-a",),
            payload_ref=None,
        )


def test_static_workflow_consumes_unrelated_trigger_and_safely_fails_current() -> None:
    strategy = StaticWorkflowStrategy(
        strategy_id="static-workflow",
        agent_ids=("worker-a", "worker-b"),
        payload_ref=None,
    )
    initialized = strategy.initialize(_workflow_view())
    current_action_id = initialized.proposals[0].action_id
    unrelated = ActionFailed(
        **_envelope(3, "ACTION_FAILED"),
        action_id="unrelated-action",
        outcome=ActionFailedOutcome(
            status=ActionStatus.FAILED,
            action_id="unrelated-action",
            error=_error("PRIVATE_PROVIDER_DETAIL"),
        ),
    )

    ignored = strategy.on_event(
        initialized.strategy,
        unrelated,
        _workflow_view(unrelated, revision=3),
    )
    assert ignored.directive is StrategyDirective.CONTINUE
    assert ignored.trigger_sequence_no == unrelated.sequence_no
    assert ignored.strategy == initialized.strategy
    assert ignored.proposals == ()

    failed = ActionFailed(
        **_envelope(4, "ACTION_FAILED"),
        action_id=current_action_id,
        outcome=ActionFailedOutcome(
            status=ActionStatus.FAILED,
            action_id=current_action_id,
            error=_error("PRIVATE_PROVIDER_DETAIL"),
        ),
    )
    decision = strategy.on_event(
        ignored.strategy,
        failed,
        _workflow_view(failed, revision=4),
    )

    assert decision.directive is StrategyDirective.FAIL
    assert decision.error is not None
    assert decision.error.code == "STATIC_WORKFLOW_ACTION_FAILED"
    assert "PRIVATE_PROVIDER_DETAIL" not in decision.model_dump_json()


@pytest.mark.parametrize("version", [True, 1.0])
def test_static_workflow_rejects_malformed_private_state(version: object) -> None:
    strategy = StaticWorkflowStrategy(
        strategy_id="static-workflow",
        agent_ids=("worker-a",),
        payload_ref=None,
    )
    initialized = strategy.initialize(_workflow_view())
    value = dict(initialized.strategy.value)
    value["version"] = version
    payload = json.dumps(value, sort_keys=True, separators=(",", ":")).encode()
    malformed = StrategyStateEnvelope(
        strategy_id="static-workflow",
        strategy_schema_version=1,
        value=value,
        content_hash=sha256(payload).hexdigest(),
        byte_size=len(payload),
    )
    unrelated = ActionFailed(
        **_envelope(3, "ACTION_FAILED"),
        action_id="unrelated-action",
        outcome=ActionFailedOutcome(
            status=ActionStatus.FAILED,
            action_id="unrelated-action",
            error=_error(),
        ),
    )

    with pytest.raises(ValueError, match="state envelope"):
        strategy.on_event(
            malformed,
            unrelated,
            _workflow_view(unrelated, revision=3),
        )


def test_static_workflow_rejects_non_string_stage_with_stable_value_error() -> None:
    strategy = StaticWorkflowStrategy(
        strategy_id="static-workflow",
        agent_ids=("worker-a",),
        payload_ref=None,
    )
    initialized = strategy.initialize(_workflow_view())
    value = dict(initialized.strategy.value)
    value["stage"] = {}
    payload = json.dumps(value, sort_keys=True, separators=(",", ":")).encode()
    malformed = StrategyStateEnvelope(
        strategy_id="static-workflow",
        strategy_schema_version=1,
        value=value,
        content_hash=sha256(payload).hexdigest(),
        byte_size=len(payload),
    )
    unrelated = ActionFailed(
        **_envelope(3, "ACTION_FAILED"),
        action_id="unrelated-action",
        outcome=ActionFailedOutcome(
            status=ActionStatus.FAILED,
            action_id="unrelated-action",
            error=_error(),
        ),
    )

    with pytest.raises(ValueError, match="state envelope"):
        strategy.on_event(
            malformed,
            unrelated,
            _workflow_view(unrelated, revision=3),
        )


@pytest.mark.asyncio
async def test_scripted_backend_is_exact_memoized_and_records_immutable_calls() -> None:
    action = _normalized_action()
    outcome = ActionSucceededOutcome(
        status=ActionStatus.SUCCEEDED,
        action_id=action.action_id,
        result_ref=_artifact("result"),
    )
    scripts = {action.action_id: outcome}
    backend = ScriptedBackend(MappingProxyType(scripts))
    context = _context(action)

    first = await backend.execute(action, context)
    scripts.clear()
    second = await backend.execute(action, context)

    assert first is outcome
    assert second is outcome
    assert backend.calls == (
        (action.action_id, action.idempotency_key),
        (action.action_id, action.idempotency_key),
    )
    with pytest.raises(AttributeError):
        backend.calls.append(("other", "idem"))  # type: ignore[attr-defined]


@pytest.mark.asyncio
async def test_scripted_backend_rejects_unknown_action_as_programming_error() -> None:
    action = _normalized_action()
    backend = ScriptedBackend({})

    with pytest.raises(ScriptedBackendError) as caught:
        await backend.execute(action, _context(action))

    assert caught.value.code == "unknown_scripted_action"


def test_scripted_backend_rejects_mismatched_outcome_configuration() -> None:
    outcome = ActionSucceededOutcome(
        status=ActionStatus.SUCCEEDED,
        action_id="other-action",
        result_ref=_artifact("result"),
    )

    with pytest.raises(ScriptedBackendError) as caught:
        ScriptedBackend({"action-1": outcome})

    assert caught.value.code == "invalid_scripted_outcome"
