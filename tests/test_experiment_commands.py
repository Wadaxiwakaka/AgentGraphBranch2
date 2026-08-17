from __future__ import annotations

from datetime import datetime, timedelta, timezone
from hashlib import sha256
from uuid import UUID

import pytest
from pydantic import TypeAdapter, ValidationError

import experiment_system.actions as action_models
import experiment_system.state as state_models
from experiment_system.actions import (
    ActionProposal,
    ActionSucceededOutcome,
    ActionType,
)
from experiment_system.commands import (
    ApplyStrategyDecision,
    CancelAttempt,
    Command,
    CreateAttempt,
    ExpireAttempt,
    FinishAttempt,
    PauseAttempt,
    RecoverAttempt,
    ReportActionOutcome,
    ReportActionStarted,
    ResumeAttempt,
    StartAttempt,
    SubmitExternalInput,
    action_command_id,
    command_request_hash,
    strategy_action_id,
    strategy_invocation_id,
    transition_command_id,
)
from experiment_system.contract import (
    ArtifactVerifier,
    Clock,
    CommandResult,
    IdFactory,
    StrategyDirective,
    SystemClock,
    Uuid4IdFactory,
    StrategyDecision,
)
from experiment_system.state import (
    ActionStatus,
    ArtifactRef,
    AttemptPhase,
    BudgetState,
    ErrorSummary,
    RecoveryPolicy,
    ResourceBudget,
    ResourceKind,
    ResourceRequest,
    StrategyStateEnvelope,
)


UTC_NOW = datetime(2026, 7, 23, 12, 0, tzinfo=timezone.utc)
COMMAND_ID = UUID("10000000-0000-4000-8000-000000000001")
ATTEMPT_ID = "attempt-1"


def _artifact(path: str) -> ArtifactRef:
    return ArtifactRef(
        capture_class="full",
        content_hash="a" * 64,
        media_type="application/json",
        byte_size=12,
        relative_path=path,
    )


def _strategy(strategy_id: str = "router") -> StrategyStateEnvelope:
    value = {"round": 1}
    payload = b'{"round":1}'
    return StrategyStateEnvelope(
        strategy_id=strategy_id,
        strategy_schema_version=1,
        value=value,
        content_hash=sha256(payload).hexdigest(),
        byte_size=len(payload),
    )


def _budget(*, deadline_at: datetime = UTC_NOW + timedelta(hours=1)) -> BudgetState:
    return BudgetState(
        resources=(ResourceBudget(resource="actions", limit=4),),
        deadline_at=deadline_at,
        max_call_depth=3,
        max_concurrent_actions=2,
    )


def _proposal(action_id: str = "action-1") -> ActionProposal:
    return ActionProposal(
        action_id=action_id,
        action_type=ActionType.INVOKE_AGENT,
        actor="engine",
        target_ids=("worker-a",),
        invocation_id="invocation-1",
        causal_parent_id="10000000-0000-4000-8000-000000000002",
        payload_ref=_artifact("attempts/attempt-1/actions/action-1.json"),
        recovery_policy=RecoveryPolicy.REPLAY_SAFE,
        requested_timeout=30,
        call_depth=0,
        resource_requests=(
            ResourceRequest(
                resource=ResourceKind.COORDINATION_ACTIONS,
                amount=1,
            ),
        ),
    )


def _create(**changes: object) -> CreateAttempt:
    values: dict[str, object] = {
        "schema_version": 1,
        "command_type": "CREATE_ATTEMPT",
        "command_id": COMMAND_ID,
        "attempt_id": ATTEMPT_ID,
        "expected_revision": 0,
        "state_schema_version": 1,
        "experiment_id": "experiment-1",
        "trial_id": "trial-1",
        "strategy_id": "router",
        "manifest_ref": _artifact("attempts/attempt-1/manifest.json"),
        "strategy": _strategy(),
        "budget": _budget(),
    }
    values.update(changes)
    return CreateAttempt(**values)


def _commands() -> tuple[object, ...]:
    outcome = ActionSucceededOutcome(
        status=ActionStatus.SUCCEEDED,
        action_id="action-1",
        result_ref=_artifact("attempts/attempt-1/actions/action-1-result.json"),
    )
    common = {
        "schema_version": 1,
        "command_id": COMMAND_ID,
        "attempt_id": ATTEMPT_ID,
        "expected_revision": 1,
    }
    return (
        _create(),
        StartAttempt(command_type="START_ATTEMPT", **common),
        ApplyStrategyDecision(
            command_type="APPLY_STRATEGY_DECISION",
            trigger_sequence_no=1,
            strategy=_strategy(),
            proposals=(_proposal(),),
            directive=StrategyDirective.CONTINUE,
            **common,
        ),
        ReportActionStarted(
            command_type="REPORT_ACTION_STARTED",
            action_id="action-1",
            **common,
        ),
        ReportActionOutcome(
            command_type="REPORT_ACTION_OUTCOME",
            action_id="action-1",
            outcome=outcome,
            **common,
        ),
        FinishAttempt(
            command_type="FINISH_ATTEMPT",
            result_ref=_artifact("attempts/attempt-1/result.json"),
            **common,
        ),
        PauseAttempt(command_type="PAUSE_ATTEMPT", **common),
        ResumeAttempt(command_type="RESUME_ATTEMPT", **common),
        CancelAttempt(command_type="CANCEL_ATTEMPT", **common),
        SubmitExternalInput(
            command_type="SUBMIT_EXTERNAL_INPUT",
            request_id="request-1",
            response_kind="APPROVE",
            **common,
        ),
        ExpireAttempt(
            command_type="EXPIRE_ATTEMPT",
            deadline_at=UTC_NOW + timedelta(hours=1),
            **common,
        ),
        RecoverAttempt(command_type="RECOVER_ATTEMPT", **common),
    )


def test_create_requires_revision_zero_and_has_a_canonical_request_hash() -> None:
    create = _create()

    assert create.expected_revision == 0
    assert command_request_hash(create) == command_request_hash(
        CreateAttempt.model_validate(create.model_dump(mode="json"))
    )
    assert len(command_request_hash(create)) == 64

    with pytest.raises(ValidationError):
        _create(expected_revision=1)
    with pytest.raises(ValidationError):
        _create(expected_revision=False)


def test_full_command_union_round_trips_json_with_the_declared_discriminator() -> None:
    adapter = TypeAdapter(Command)

    for command in _commands():
        restored = adapter.validate_python(command.model_dump(mode="json"))
        assert type(restored) is type(command)
        assert restored == command


@pytest.mark.parametrize("command", _commands()[1:])
def test_non_create_commands_require_a_positive_strict_revision(
    command: object,
) -> None:
    command_type = type(command)
    payload = command.model_dump(mode="python")

    with pytest.raises(ValidationError):
        command_type.model_validate({**payload, "expected_revision": 0})
    with pytest.raises(ValidationError):
        command_type.model_validate({**payload, "expected_revision": True})


@pytest.mark.parametrize("command", _commands())
def test_commands_forbid_extra_fields_and_are_frozen(command: object) -> None:
    command_type = type(command)
    with pytest.raises(ValidationError, match="extra_forbidden"):
        command_type.model_validate(
            {**command.model_dump(mode="python"), "unexpected": "value"}
        )
    with pytest.raises(ValidationError, match="frozen"):
        command.expected_revision = 99


def test_command_union_rejects_unknown_types_and_schema_versions() -> None:
    adapter = TypeAdapter(Command)
    start = _commands()[1].model_dump(mode="python")

    with pytest.raises(ValidationError):
        adapter.validate_python({**start, "command_type": "UNKNOWN"})
    with pytest.raises(ValidationError):
        adapter.validate_python({**start, "schema_version": 2})
    with pytest.raises(ValidationError):
        adapter.validate_python({**start, "schema_version": True})


def test_commands_validate_only_relationships_present_in_domain_payloads() -> None:
    with pytest.raises(ValidationError, match="strategy_id"):
        _create(strategy=_strategy("other-strategy"))

    outcome = ActionSucceededOutcome(
        status=ActionStatus.SUCCEEDED,
        action_id="other-action",
        result_ref=_artifact("attempts/attempt-1/actions/result.json"),
    )
    with pytest.raises(ValidationError, match="action_id"):
        ReportActionOutcome(
            schema_version=1,
            command_type="REPORT_ACTION_OUTCOME",
            command_id=COMMAND_ID,
            attempt_id=ATTEMPT_ID,
            expected_revision=1,
            action_id="action-1",
            outcome=outcome,
        )


def test_external_input_contracts_are_closed_safe_and_stably_identified() -> None:
    request_kind = getattr(state_models, "ExternalRequestKind", None)
    response_kind = getattr(state_models, "ExternalResponseKind", None)
    requirement_type = getattr(action_models, "ExternalInputRequirement", None)

    assert request_kind is not None
    assert response_kind is not None
    assert requirement_type is not None
    assert {kind.value for kind in request_kind} == {
        "ACTION_APPROVAL",
        "ADDITIONAL_INPUT",
        "OUTCOME_RECONCILIATION",
    }
    assert {kind.value for kind in response_kind} == {
        "APPROVE",
        "REJECT",
        "PROVIDE_INPUT",
        "CONFIRM_SUCCEEDED",
        "CONFIRM_FAILED",
        "ABANDON",
    }

    requirement = requirement_type(
        request_id="approval-1",
        request_kind=request_kind.ACTION_APPROVAL,
        action_id="action-1",
        payload_ref=_artifact("attempts/attempt-1/approval.json"),
    )
    assert set(requirement_type.model_fields) == {
        "request_id",
        "request_kind",
        "action_id",
        "payload_ref",
    }
    assert requirement.action_id == "action-1"
    with pytest.raises(ValidationError):
        requirement_type(
            request_id="approval secret",
            request_kind=request_kind.ACTION_APPROVAL,
            action_id="action-1",
        )
    with pytest.raises(ValidationError, match="action_id"):
        requirement_type(
            request_id="approval-2",
            request_kind=request_kind.ACTION_APPROVAL,
        )
    with pytest.raises(ValidationError, match="action_id"):
        requirement_type(
            request_id="input-1",
            request_kind=request_kind.ADDITIONAL_INPUT,
            action_id="action-1",
        )


def test_empty_external_requirements_preserve_old_command_dump_and_hash() -> None:
    requirement_type = getattr(action_models, "ExternalInputRequirement", None)
    request_kind = getattr(state_models, "ExternalRequestKind", None)
    assert requirement_type is not None
    assert request_kind is not None

    old_command = _commands()[2]
    assert "external_requirements" not in old_command.model_dump(mode="json")
    assert command_request_hash(old_command) == (
        "738e1695ec016ede6d8dcc2c74bfdf6b47d3ce15368d367259dc0cbe8bc967bc"
    )

    requirement = requirement_type(
        request_id="approval-1",
        request_kind=request_kind.ACTION_APPROVAL,
        action_id="action-1",
    )
    values = old_command.model_dump(mode="python")
    gated = ApplyStrategyDecision.model_validate(
        {**values, "external_requirements": (requirement,)}
    )
    assert gated.external_requirements == (requirement,)
    assert command_request_hash(gated) != command_request_hash(old_command)

    strategy_decision = StrategyDecision(
        trigger_sequence_no=1,
        strategy=_strategy(),
        proposals=(_proposal(),),
        directive=StrategyDirective.CONTINUE,
    )
    assert "external_requirements" not in strategy_decision.model_dump(mode="json")


def test_decisions_reject_ambiguous_or_forbidden_external_requirements() -> None:
    requirement_type = getattr(action_models, "ExternalInputRequirement", None)
    request_kind = getattr(state_models, "ExternalRequestKind", None)
    assert requirement_type is not None
    assert request_kind is not None

    common = {
        "trigger_sequence_no": 1,
        "strategy": _strategy(),
        "proposals": (_proposal(),),
        "directive": StrategyDirective.CONTINUE,
    }
    approval = requirement_type(
        request_id="approval-1",
        request_kind=request_kind.ACTION_APPROVAL,
        action_id="action-1",
    )
    assert StrategyDecision(
        **common, external_requirements=(approval,)
    ).external_requirements == (approval,)

    invalid_requirements = (
        (
            requirement_type(
                request_id="approval-2",
                request_kind=request_kind.ACTION_APPROVAL,
                action_id="missing-action",
            ),
        ),
        (approval, approval.model_copy(update={"request_id": "approval-2"})),
        (
            requirement_type(
                request_id="input-1",
                request_kind=request_kind.ADDITIONAL_INPUT,
            ),
        ),
        (
            requirement_type(
                request_id="reconcile-1",
                request_kind=request_kind.OUTCOME_RECONCILIATION,
                action_id="action-1",
            ),
        ),
    )
    for requirements in invalid_requirements:
        with pytest.raises(ValidationError, match="external"):
            StrategyDecision(**common, external_requirements=requirements)

    additional = requirement_type(
        request_id="input-2",
        request_kind=request_kind.ADDITIONAL_INPUT,
    )
    decision = StrategyDecision(
        **{**common, "proposals": ()},
        external_requirements=(additional,),
    )
    assert decision.external_requirements == (additional,)

    for directive_values in (
        {
            "directive": StrategyDirective.SUCCEED,
            "result_ref": _artifact("attempts/attempt-1/result.json"),
        },
        {"directive": StrategyDirective.FAIL, "error": ErrorSummary(
            code="FAILED",
            retryable=False,
            safe_message="The Strategy failed.",
        )},
    ):
        with pytest.raises(ValidationError, match="external"):
            StrategyDecision(
                **{
                    **common,
                    "proposals": (),
                    **directive_values,
                },
                external_requirements=(additional,),
            )


@pytest.mark.parametrize(
    ("response_kind", "response_ref", "error"),
    [
        ("APPROVE", _artifact("attempts/attempt-1/response.json"), None),
        ("REJECT", None, ErrorSummary(
            code="REJECTED",
            retryable=False,
            safe_message="The request was rejected.",
        )),
        ("PROVIDE_INPUT", None, None),
        ("PROVIDE_INPUT", _artifact("attempts/attempt-1/input.json"), ErrorSummary(
            code="INVALID",
            retryable=False,
            safe_message="The input was invalid.",
        )),
        ("CONFIRM_SUCCEEDED", None, None),
        ("CONFIRM_FAILED", None, None),
        ("ABANDON", _artifact("attempts/attempt-1/abandon.json"), None),
        ("UNKNOWN", None, None),
    ],
)
def test_submit_external_input_rejects_unsafe_response_shapes(
    response_kind: str,
    response_ref: ArtifactRef | None,
    error: ErrorSummary | None,
) -> None:
    with pytest.raises(ValidationError):
        SubmitExternalInput(
            schema_version=1,
            command_type="SUBMIT_EXTERNAL_INPUT",
            command_id=COMMAND_ID,
            attempt_id=ATTEMPT_ID,
            expected_revision=1,
            request_id="request-1",
            response_kind=response_kind,
            response_ref=response_ref,
            error=error,
        )


def test_strategy_decision_freezes_the_ordered_proposal_collection() -> None:
    proposals = [_proposal("action-1")]
    command = ApplyStrategyDecision(
        schema_version=1,
        command_type="APPLY_STRATEGY_DECISION",
        command_id=COMMAND_ID,
        attempt_id=ATTEMPT_ID,
        expected_revision=1,
        trigger_sequence_no=1,
        strategy=_strategy(),
        proposals=proposals,
        directive=StrategyDirective.CONTINUE,
    )

    proposals.append(_proposal("action-2"))
    assert isinstance(command.proposals, tuple)
    assert tuple(item.action_id for item in command.proposals) == ("action-1",)


def test_expiry_command_rejects_naive_and_non_utc_timestamps() -> None:
    values = _commands()[-2].model_dump(mode="python")

    with pytest.raises(ValidationError, match="timezone-aware"):
        ExpireAttempt.model_validate(
            {**values, "deadline_at": datetime(2026, 7, 23, 13, 0)}
        )
    with pytest.raises(ValidationError, match="UTC"):
        ExpireAttempt.model_validate(
            {
                **values,
                "deadline_at": datetime(
                    2026,
                    7,
                    23,
                    13,
                    0,
                    tzinfo=timezone(timedelta(hours=8)),
                ),
            }
        )


def test_finish_requires_exactly_one_success_or_failure_value() -> None:
    common = {
        "schema_version": 1,
        "command_type": "FINISH_ATTEMPT",
        "command_id": COMMAND_ID,
        "attempt_id": ATTEMPT_ID,
        "expected_revision": 1,
    }
    error = ErrorSummary(
        code="STRATEGY_FAILED",
        retryable=False,
        safe_message="The strategy could not finish the Attempt.",
    )

    assert FinishAttempt(error=error, **common).error == error
    with pytest.raises(ValidationError, match="exactly one"):
        FinishAttempt(**common)
    with pytest.raises(ValidationError, match="exactly one"):
        FinishAttempt(
            result_ref=_artifact("attempts/attempt-1/result.json"),
            error=error,
            **common,
        )


def test_uuid5_identity_helpers_are_stable_separated_and_domain_typed() -> None:
    started = action_command_id("action-1", "started")

    assert started == action_command_id("action-1", "started")
    assert started != action_command_id("action-1", "outcome")
    assert started.version == 5

    first_action = strategy_action_id(ATTEMPT_ID, "router", 0)
    assert isinstance(first_action, str)
    assert UUID(first_action).version == 5
    assert first_action == strategy_action_id(ATTEMPT_ID, "router", 0)
    assert first_action != strategy_action_id(ATTEMPT_ID, "router", 1)

    first_invocation = strategy_invocation_id(ATTEMPT_ID, "router", 0)
    assert UUID(first_invocation).version == 5
    assert first_invocation == strategy_invocation_id(ATTEMPT_ID, "router", 0)
    assert first_invocation != first_action
    assert first_invocation != strategy_invocation_id(ATTEMPT_ID, "router", 1)

    transition = transition_command_id(ATTEMPT_ID, 7, "apply-strategy")
    assert transition.version == 5
    assert transition == transition_command_id(ATTEMPT_ID, 7, "apply-strategy")
    assert transition != transition_command_id(ATTEMPT_ID, 8, "apply-strategy")
    assert transition != action_command_id(ATTEMPT_ID, "apply-strategy")


@pytest.mark.parametrize(
    "call",
    [
        lambda: action_command_id("", "started"),
        lambda: action_command_id("action-1", ""),
        lambda: action_command_id(1, "started"),  # type: ignore[arg-type]
        lambda: strategy_action_id("", "router", 0),
        lambda: strategy_action_id(ATTEMPT_ID, "", 0),
        lambda: strategy_action_id(ATTEMPT_ID, 1, 0),  # type: ignore[arg-type]
        lambda: strategy_action_id(ATTEMPT_ID, "router", -1),
        lambda: strategy_action_id(ATTEMPT_ID, "router", True),
        lambda: strategy_invocation_id("", "router", 0),
        lambda: strategy_invocation_id(ATTEMPT_ID, "", 0),
        lambda: strategy_invocation_id(ATTEMPT_ID, "router", -1),
        lambda: strategy_invocation_id(ATTEMPT_ID, "router", True),
        lambda: transition_command_id(ATTEMPT_ID, 0, "start"),
        lambda: transition_command_id(ATTEMPT_ID, True, "start"),
        lambda: transition_command_id(ATTEMPT_ID, 1, ""),
        lambda: transition_command_id(ATTEMPT_ID, 1, 1),  # type: ignore[arg-type]
    ],
)
def test_uuid5_helpers_reject_ambiguous_identity_components(call: object) -> None:
    with pytest.raises((TypeError, ValueError)):
        call()


def test_request_hash_changes_when_command_payload_changes() -> None:
    original = StartAttempt(
        schema_version=1,
        command_type="START_ATTEMPT",
        command_id=COMMAND_ID,
        attempt_id=ATTEMPT_ID,
        expected_revision=1,
    )
    changed = original.model_copy(update={"expected_revision": 2})

    assert command_request_hash(original) != command_request_hash(changed)


def test_infrastructure_protocols_accept_injected_test_doubles() -> None:
    class FixedClock:
        def now_utc(self) -> datetime:
            return UTC_NOW

    class FixedIds:
        def new_uuid(self) -> UUID:
            return COMMAND_ID

    class RecordingVerifier:
        def __init__(self) -> None:
            self.seen: list[ArtifactRef] = []

        def verify(self, ref: ArtifactRef) -> None:
            self.seen.append(ref)

    verifier = RecordingVerifier()
    artifact = _artifact("attempts/attempt-1/input.json")
    verifier.verify(artifact)

    assert isinstance(FixedClock(), Clock)
    assert isinstance(FixedIds(), IdFactory)
    assert isinstance(verifier, ArtifactVerifier)
    assert verifier.seen == [artifact]


def test_production_metadata_sources_return_utc_and_uuid4_values() -> None:
    now = SystemClock().now_utc()
    generated = Uuid4IdFactory().new_uuid()

    assert now.tzinfo is timezone.utc
    assert now.utcoffset() == timedelta(0)
    assert generated.version == 4


def test_command_result_is_strict_frozen_and_lifecycle_consistent() -> None:
    accepted = CommandResult(
        command_id=COMMAND_ID,
        attempt_id=ATTEMPT_ID,
        accepted=True,
        revision=1,
        phase=AttemptPhase.PLANNED,
    )
    error = ErrorSummary(
        code="REVISION_CONFLICT",
        retryable=True,
        safe_message="The Attempt revision changed.",
    )
    rejected = CommandResult(
        command_id=COMMAND_ID,
        attempt_id=ATTEMPT_ID,
        accepted=False,
        revision=0,
        phase=None,
        error=error,
    )

    assert accepted.error is None
    assert rejected.error == error
    assert CommandResult.model_validate(
        accepted.model_dump(mode="json")
    ) == accepted

    with pytest.raises(ValidationError, match="accepted"):
        CommandResult(
            command_id=COMMAND_ID,
            attempt_id=ATTEMPT_ID,
            accepted=True,
            revision=1,
            phase=AttemptPhase.PLANNED,
            error=error,
        )
    with pytest.raises(ValidationError, match="rejected"):
        CommandResult(
            command_id=COMMAND_ID,
            attempt_id=ATTEMPT_ID,
            accepted=False,
            revision=0,
            phase=None,
        )
    with pytest.raises(ValidationError, match="extra_forbidden"):
        CommandResult.model_validate(
            {**accepted.model_dump(mode="python"), "unexpected": True}
        )
    with pytest.raises(ValidationError, match="frozen"):
        accepted.revision = 2
