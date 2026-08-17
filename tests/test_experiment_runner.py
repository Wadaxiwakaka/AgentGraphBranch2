from __future__ import annotations

import json
import sqlite3
from datetime import datetime, timedelta, timezone
from hashlib import sha256
from pathlib import Path
from uuid import UUID

import pytest

from experiment_system.actions import (
    ActionFailedOutcome,
    ActionProposal,
    ActionSucceededOutcome,
    ActionType,
    ActionUnknownOutcome,
    ExternalInputRequirement,
)
from experiment_system.backends.deterministic import ScriptedBackend
from experiment_system.commands import (
    ApplyStrategyDecision,
    CreateAttempt,
    ReportActionStarted,
    StartAttempt,
    SubmitExternalInput,
    strategy_action_id,
    strategy_invocation_id,
    transition_command_id,
)
from experiment_system.contract import (
    ArtifactVerificationError,
    CommandResult,
    StrategyDecision,
    StrategyDirective,
)
from experiment_system.engine import AttemptEngine
from experiment_system.events import (
    ActionAccepted,
    ActionFailed,
    ActionRejected,
    ActionSucceeded,
    ActionOutcomeUnknown,
    BudgetSettled,
    InvocationCompleted,
    StrategyDecisionRecorded,
    is_strategy_trigger,
    parse_domain_event,
)
from experiment_system.executor import (
    ActionExecutor,
    ExecutorStepResult,
    ExecutorStepStatus,
)
from experiment_system.reducer import replay_events
from experiment_system.runner import (
    DeterministicAttemptRunner,
    RunnerInvariantError,
    RunnerResult,
    RunnerStatus,
    StepLimitExceeded,
)
from experiment_system.state import (
    ActionStatus,
    ArtifactRef,
    AttemptPhase,
    BudgetState,
    ErrorSummary,
    ExternalRequestKind,
    ExternalResponseKind,
    RecoveryPolicy,
    ResourceBudget,
    ResourceKind,
    ResourceRequest,
    StrategyStateEnvelope,
    canonical_state_bytes,
)
from experiment_system.store import AttemptRepository, CommitRequest
from experiment_system.stores import (
    InMemoryAttemptRepository,
    SQLiteAttemptRepository,
)
from experiment_system.strategies import (
    SingleAgentStrategy,
    StaticWorkflowStrategy,
)


NOW = datetime(2026, 7, 23, 10, 0, tzinfo=timezone.utc)
ATTEMPT_ID = "attempt-runner-1"


class FixedClock:
    def __init__(self, value: datetime = NOW) -> None:
        self.value = value
        self.calls = 0

    def now_utc(self) -> datetime:
        self.calls += 1
        return self.value


class SequentialIdFactory:
    def __init__(self, next_value: int = 1) -> None:
        self.next_value = next_value

    def new_uuid(self) -> UUID:
        value = UUID(int=self.next_value)
        self.next_value += 1
        return value


class AcceptingArtifactVerifier:
    def verify(self, ref: ArtifactRef) -> None:
        if ref.capture_class != "full":
            raise ArtifactVerificationError()


def _artifact(name: str, *, attempt_id: str = ATTEMPT_ID) -> ArtifactRef:
    return ArtifactRef(
        capture_class="full",
        content_hash=sha256(name.encode()).hexdigest(),
        media_type="application/json",
        byte_size=len(name),
        relative_path=f"attempts/{attempt_id}/{name}.json",
    )


def _initial_strategy(strategy_id: str) -> StrategyStateEnvelope:
    value = {"stage": "READY", "version": 1}
    payload = json.dumps(value, sort_keys=True, separators=(",", ":")).encode()
    return StrategyStateEnvelope(
        strategy_id=strategy_id,
        strategy_schema_version=1,
        value=value,
        content_hash=sha256(payload).hexdigest(),
        byte_size=len(payload),
    )


def _budget() -> BudgetState:
    return BudgetState(
        resources=(
            ResourceBudget(
                resource=ResourceKind.MODEL_CALLS.value,
                limit=20,
            ),
        ),
        deadline_at=NOW + timedelta(hours=1),
        max_call_depth=4,
        max_concurrent_actions=4,
    )


async def _create_attempt(
    engine: AttemptEngine,
    *,
    attempt_id: str,
    strategy_id: str,
) -> None:
    result = await engine.handle(
        CreateAttempt(
            schema_version=1,
            command_type="CREATE_ATTEMPT",
            command_id=UUID("10000000-0000-0000-0000-000000000001"),
            attempt_id=attempt_id,
            expected_revision=0,
            state_schema_version=1,
            experiment_id="experiment-runner",
            trial_id="trial-runner",
            strategy_id=strategy_id,
            manifest_ref=_artifact("manifest", attempt_id=attempt_id),
            strategy=_initial_strategy(strategy_id),
            budget=_budget(),
        )
    )
    assert result.accepted


def _build_runtime(
    repository: AttemptRepository,
    *,
    strategy_id: str,
    strategy: object,
    outcomes: dict[str, object],
    allowed_edges: frozenset[tuple[str, str]],
    ids: SequentialIdFactory | None = None,
    legal_topology: object | None = None,
):
    clock = FixedClock()
    engine = AttemptEngine(
        repository=repository,
        clock=clock,
        id_factory=ids or SequentialIdFactory(),
        artifact_verifier=AcceptingArtifactVerifier(),
        allowed_edges=allowed_edges,
    )
    backend = ScriptedBackend(outcomes)  # type: ignore[arg-type]
    executor = ActionExecutor(
        backends={ActionType.INVOKE_AGENT: backend},
        repository=repository,
        engine=engine,
        clock=clock,
        lease_seconds=30,
    )
    source_registry = {strategy_id: strategy}
    source_topology = legal_topology or {
        "allowed_edges": tuple(sorted(allowed_edges))
    }
    runner = DeterministicAttemptRunner(
        repository=repository,
        engine=engine,
        executor=executor,
        strategies=source_registry,
        legal_topology=source_topology,
        worker_id="runner-worker",
    )
    source_registry.clear()
    if isinstance(source_topology, dict):
        source_topology.clear()
    return engine, backend, executor, runner


async def _run_single(
    repository: AttemptRepository,
    *,
    attempt_id: str = ATTEMPT_ID,
    allowed_edges: frozenset[tuple[str, str]] | None = None,
    failed: bool = False,
    unknown: bool = False,
):
    strategy_id = "single-agent"
    action_id = strategy_action_id(attempt_id, strategy_id, 0)
    result_ref = _artifact("single-result", attempt_id=attempt_id)
    outcome = (
        ActionUnknownOutcome(
            status=ActionStatus.OUTCOME_UNKNOWN,
            action_id=action_id,
            error=ErrorSummary(
                code="OUTCOME_UNKNOWN",
                retryable=False,
                safe_message="The scripted outcome is unknown.",
            ),
        )
        if unknown
        else
        ActionFailedOutcome(
            status=ActionStatus.FAILED,
            action_id=action_id,
            error=ErrorSummary(
                code="BACKEND_FAILED",
                retryable=False,
                safe_message="The scripted backend failed.",
            ),
        )
        if failed
        else ActionSucceededOutcome(
            status=ActionStatus.SUCCEEDED,
            action_id=action_id,
            result_ref=result_ref,
        )
    )
    strategy = SingleAgentStrategy(
        strategy_id=strategy_id,
        agent_id="worker-a",
        payload_ref=_artifact("single-payload", attempt_id=attempt_id),
        requested_timeout=30,
        resource_requests=(
            ResourceRequest(resource=ResourceKind.MODEL_CALLS, amount=1),
        ),
    )
    edges = allowed_edges or frozenset({(strategy_id, "worker-a")})
    engine, backend, _, runner = _build_runtime(
        repository,
        strategy_id=strategy_id,
        strategy=strategy,
        outcomes={action_id: outcome},
        allowed_edges=edges,
    )
    await _create_attempt(
        engine,
        attempt_id=attempt_id,
        strategy_id=strategy_id,
    )
    result = await runner.run_until_blocked(attempt_id, max_steps=16)
    loaded = await repository.load(attempt_id)
    assert loaded is not None
    return result, loaded, backend, runner


async def _run_static_workflow(
    repository: AttemptRepository,
    *,
    attempt_id: str = ATTEMPT_ID,
):
    strategy_id = "static-workflow"
    action_ids = tuple(
        strategy_action_id(attempt_id, strategy_id, ordinal)
        for ordinal in range(2)
    )
    strategy = StaticWorkflowStrategy(
        strategy_id=strategy_id,
        agent_ids=("worker-a", "worker-a"),
        payload_ref=_artifact("workflow-payload", attempt_id=attempt_id),
        resource_requests=(
            ResourceRequest(resource=ResourceKind.MODEL_CALLS, amount=1),
        ),
    )
    outcomes = {
        action_id: ActionSucceededOutcome(
            status=ActionStatus.SUCCEEDED,
            action_id=action_id,
            result_ref=_artifact(
                f"workflow-result-{ordinal}", attempt_id=attempt_id
            ),
        )
        for ordinal, action_id in enumerate(action_ids)
    }
    engine, backend, _, runner = _build_runtime(
        repository,
        strategy_id=strategy_id,
        strategy=strategy,
        outcomes=outcomes,
        allowed_edges=frozenset({(strategy_id, "worker-a")}),
    )
    await _create_attempt(
        engine,
        attempt_id=attempt_id,
        strategy_id=strategy_id,
    )
    result = await runner.run_until_blocked(attempt_id, max_steps=24)
    loaded = await repository.load(attempt_id)
    assert loaded is not None
    return result, loaded, backend, action_ids


@pytest.mark.asyncio
async def test_single_agent_runs_planned_to_succeeded_and_is_idempotent() -> None:
    repository = InMemoryAttemptRepository()
    result, loaded, backend, runner = await _run_single(repository)

    assert result == RunnerResult(
        status=RunnerStatus.TERMINAL,
        attempt_id=ATTEMPT_ID,
        phase=AttemptPhase.SUCCEEDED,
        revision=loaded.state.revision,
        steps=5,
    )
    assert loaded.state.result_ref == _artifact("single-result")
    assert all(action.status in {ActionStatus.SUCCEEDED} for action in loaded.state.actions)
    assert len(backend.calls) == 1
    assert repository._outbox == {}
    assert await runner.run_until_blocked(ATTEMPT_ID, max_steps=16) == RunnerResult(
        status=RunnerStatus.TERMINAL,
        attempt_id=ATTEMPT_ID,
        phase=AttemptPhase.SUCCEEDED,
        revision=loaded.state.revision,
        steps=0,
    )
    assert len(backend.calls) == 1

    decisions = tuple(
        event for event in loaded.events if isinstance(event, StrategyDecisionRecorded)
    )
    assert tuple(event.trigger_sequence_no for event in decisions) == tuple(
        event.sequence_no for event in loaded.events if is_strategy_trigger(event)
    )
    assert not any(
        decision.trigger_sequence_no == event.sequence_no
        for decision in decisions
        for event in loaded.events
        if isinstance(event, (BudgetSettled, InvocationCompleted))
    )
    assert {
        transition_command_id(ATTEMPT_ID, 1, "start-attempt"),
        *(
            transition_command_id(
                ATTEMPT_ID,
                decision.trigger_sequence_no,
                "apply-strategy-decision",
            )
            for decision in decisions
        ),
        transition_command_id(
            ATTEMPT_ID,
            decisions[-1].sequence_no,
            "finish-attempt",
        ),
    }.issubset({record.command_id for record in loaded.command_records})


@pytest.mark.asyncio
async def test_static_workflow_runs_two_repeated_roles_in_strict_order() -> None:
    repository = InMemoryAttemptRepository()
    result, loaded, backend, action_ids = await _run_static_workflow(repository)

    assert result.status is RunnerStatus.TERMINAL
    assert loaded.state.phase is AttemptPhase.SUCCEEDED
    assert tuple(call[0] for call in backend.calls) == action_ids
    assert tuple(action.target_ids for action in loaded.state.actions) == (
        ("worker-a",),
        ("worker-a",),
    )
    accepted = tuple(
        event for event in loaded.events if isinstance(event, ActionAccepted)
    )
    succeeded = tuple(
        event for event in loaded.events if isinstance(event, ActionSucceeded)
    )
    assert tuple(event.action.action_id for event in accepted) == action_ids
    assert tuple(event.action_id for event in succeeded) == action_ids
    assert succeeded[0].sequence_no < accepted[1].sequence_no
    assert repository._outbox == {}
    assert all(
        sum(event.action_id == accepted_event.action.action_id for event in succeeded)
        == 1
        for accepted_event in accepted
    )


@pytest.mark.asyncio
async def test_action_rejection_finishes_failed_without_backend_call() -> None:
    repository = InMemoryAttemptRepository()
    result, loaded, backend, _ = await _run_single(
        repository,
        allowed_edges=frozenset({("single-agent", "different-worker")}),
    )

    assert result.status is RunnerStatus.TERMINAL
    assert loaded.state.phase is AttemptPhase.FAILED
    assert len(backend.calls) == 0
    assert any(isinstance(event, ActionRejected) for event in loaded.events)
    assert loaded.state.terminal_error is not None
    assert loaded.state.terminal_error.code == "SINGLE_AGENT_ACTION_FAILED"


@pytest.mark.asyncio
async def test_backend_failure_finishes_failed_with_one_terminal_observation() -> None:
    result, loaded, backend, _ = await _run_single(
        InMemoryAttemptRepository(), failed=True
    )

    assert result.status is RunnerStatus.TERMINAL
    assert loaded.state.phase is AttemptPhase.FAILED
    assert len(backend.calls) == 1
    terminal = tuple(
        event
        for event in loaded.events
        if isinstance(event, (ActionSucceeded, ActionFailed))
    )
    assert len(terminal) == 1
    assert terminal[0].action_id == loaded.state.actions[0].action_id


@pytest.mark.asyncio
async def test_outcome_unknown_blocks_for_reconciliation_and_is_not_finished() -> None:
    result, loaded, backend, _ = await _run_single(
        InMemoryAttemptRepository(), unknown=True
    )

    assert result.status is RunnerStatus.BLOCKED
    assert loaded.state.phase is AttemptPhase.WAITING_EXTERNAL
    assert loaded.state.actions[0].status is ActionStatus.OUTCOME_UNKNOWN
    assert (
        loaded.state.pending_external[0].request_kind
        is ExternalRequestKind.OUTCOME_RECONCILIATION
    )
    assert len(backend.calls) == 1


class EmptyDecisionStrategy:
    def __init__(self, strategy_id: str) -> None:
        self._strategy_id = strategy_id

    def initialize(self, view) -> StrategyDecision:
        return StrategyDecision(
            trigger_sequence_no=view.latest_committed_event["sequence_no"],
            strategy=_initial_strategy(self._strategy_id),
            proposals=(),
            directive=StrategyDirective.CONTINUE,
        )

    def on_event(self, state, event, view) -> StrategyDecision:
        return StrategyDecision(
            trigger_sequence_no=event.sequence_no,
            strategy=state,
            proposals=(),
            directive=StrategyDirective.CONTINUE,
        )


class CountingEmptyDecisionStrategy(EmptyDecisionStrategy):
    def __init__(self, strategy_id: str) -> None:
        super().__init__(strategy_id)
        self.calls = 0

    def initialize(self, view) -> StrategyDecision:
        self.calls += 1
        return super().initialize(view)


class RecordingEmptyDecisionStrategy(EmptyDecisionStrategy):
    def __init__(self, strategy_id: str) -> None:
        super().__init__(strategy_id)
        self.seen_event_ids: list[UUID] = []

    def on_event(self, state, event, view) -> StrategyDecision:
        self.seen_event_ids.append(event.event_id)
        return super().on_event(state, event, view)


class MalformedDecisionStrategy:
    def initialize(self, view):
        return {"not": "a StrategyDecision"}

    def on_event(self, state, event, view):
        return {"not": "a StrategyDecision"}


class ForbiddenAfterTerminalStrategy:
    def initialize(self, view):
        raise AssertionError("terminal Strategy must not initialize")

    def on_event(self, state, event, view):
        raise AssertionError("terminal Strategy must not receive another trigger")


class AdditionalInputStrategy:
    def __init__(self, strategy_id: str) -> None:
        self._strategy_id = strategy_id

    def initialize(self, view) -> StrategyDecision:
        return StrategyDecision(
            trigger_sequence_no=view.latest_committed_event["sequence_no"],
            strategy=_initial_strategy(self._strategy_id),
            proposals=(),
            external_requirements=(
                ExternalInputRequirement(
                    request_id="runner-input-1",
                    request_kind=ExternalRequestKind.ADDITIONAL_INPUT,
                    payload_ref=_artifact("runner-input-request"),
                ),
            ),
            directive=StrategyDirective.CONTINUE,
        )

    def on_event(self, state, event, view):
        raise AssertionError("additional input Strategy is waiting for a response")


class ApprovalSingleAgentStrategy(SingleAgentStrategy):
    def initialize(self, view) -> StrategyDecision:
        decision = super().initialize(view)
        return decision.model_copy(
            update={
                "external_requirements": (
                    ExternalInputRequirement(
                        request_id="runner-approval-reject",
                        request_kind=ExternalRequestKind.ACTION_APPROVAL,
                        action_id=decision.proposals[0].action_id,
                    ),
                )
            }
        )


class CompletingButIdleExecutor(ActionExecutor):
    def __init__(self, delegate: ActionExecutor) -> None:
        self._delegate = delegate

    async def run_once(self, worker_id: str) -> ExecutorStepResult:
        completed = await self._delegate.run_once("other-worker")
        assert completed.status is ExecutorStepStatus.COMPLETED
        return ExecutorStepResult(status=ExecutorStepStatus.IDLE)


async def _prepare_scripted_batch(
    repository: AttemptRepository,
    *,
    strategy_id: str,
    strategy: object,
    outcomes: dict[str, object],
):
    engine, backend, executor, runner = _build_runtime(
        repository,
        strategy_id=strategy_id,
        strategy=strategy,
        outcomes=outcomes,
        allowed_edges=frozenset({(strategy_id, "worker-a")}),
    )
    await _create_attempt(engine, attempt_id=ATTEMPT_ID, strategy_id=strategy_id)
    planned = await repository.load(ATTEMPT_ID)
    assert planned is not None
    started = await engine.handle(
        StartAttempt(
            schema_version=1,
            command_type="START_ATTEMPT",
            command_id=transition_command_id(ATTEMPT_ID, 1, "start-attempt"),
            attempt_id=ATTEMPT_ID,
            expected_revision=planned.state.revision,
        )
    )
    assert started.accepted
    loaded = await repository.load(ATTEMPT_ID)
    assert loaded is not None
    trigger = loaded.events[-1]
    action_ids = tuple(outcomes)
    proposals = tuple(
        ActionProposal(
            action_id=action_id,
            action_type=ActionType.INVOKE_AGENT,
            actor=strategy_id,
            target_ids=("worker-a",),
            invocation_id=strategy_invocation_id(
                ATTEMPT_ID, strategy_id, ordinal
            ),
            causal_parent_id=str(trigger.event_id),
            payload_ref=None,
            recovery_policy=RecoveryPolicy.REPLAY_SAFE,
            requested_timeout=30,
            batch_id="review-batch",
            call_depth=0,
            resource_requests=(
                ResourceRequest(resource=ResourceKind.MODEL_CALLS, amount=1),
            ),
        )
        for ordinal, action_id in enumerate(action_ids)
    )
    accepted = await engine.handle(
        ApplyStrategyDecision(
            schema_version=1,
            command_type="APPLY_STRATEGY_DECISION",
            command_id=transition_command_id(
                ATTEMPT_ID, trigger.sequence_no, "apply-strategy-decision"
            ),
            attempt_id=ATTEMPT_ID,
            expected_revision=loaded.state.revision,
            trigger_sequence_no=trigger.sequence_no,
            strategy=_initial_strategy(strategy_id),
            proposals=proposals,
            directive=StrategyDirective.CONTINUE,
        )
    )
    assert accepted.accepted
    return engine, backend, executor, runner, action_ids


@pytest.mark.asyncio
async def test_runner_still_selects_a_genuine_legacy_bare_unknown_trigger() -> None:
    repository = InMemoryAttemptRepository()
    strategy_id = "legacy-bare-unknown"
    action_id = strategy_action_id(ATTEMPT_ID, strategy_id, 0)
    strategy = RecordingEmptyDecisionStrategy(strategy_id)
    engine, backend, _, runner, _ = await _prepare_scripted_batch(
        repository,
        strategy_id=strategy_id,
        strategy=strategy,
        outcomes={
            action_id: ActionUnknownOutcome(
                status=ActionStatus.OUTCOME_UNKNOWN,
                action_id=action_id,
                error=ErrorSummary(
                    code="LEGACY_UNKNOWN",
                    retryable=False,
                    safe_message="The legacy Action outcome is unknown.",
                ),
            )
        },
    )
    accepted = await repository.load(ATTEMPT_ID)
    assert accepted is not None
    claim = await repository.claim_action(
        worker_id="legacy-worker",
        now_utc=NOW,
        lease_seconds=10,
    )
    assert claim is not None
    started_result = await engine.handle(
        ReportActionStarted(
            schema_version=1,
            command_type="REPORT_ACTION_STARTED",
            command_id=UUID("89000000-0000-0000-0000-000000000001"),
            attempt_id=ATTEMPT_ID,
            expected_revision=accepted.state.revision,
            action_id=action_id,
        ),
        delivery_claim=claim.delivery_claim(),
    )
    assert started_result.accepted
    started = await repository.load(ATTEMPT_ID)
    assert started is not None
    command_id = UUID("89000000-0000-0000-0000-000000000002")
    bare_unknown = ActionOutcomeUnknown(
        schema_version=1,
        event_type="ACTION_OUTCOME_UNKNOWN",
        event_id=UUID("89000000-0000-0000-0000-000000000003"),
        trial_id=started.state.trial_id,
        attempt_id=ATTEMPT_ID,
        sequence_no=started.state.revision + 1,
        command_id=command_id,
        causal_parent_id=started.latest_event_id,
        logical_time=started.events[-1].logical_time + 1,
        wall_time_utc=NOW,
        action_id=action_id,
        outcome=ActionUnknownOutcome(
            status=ActionStatus.OUTCOME_UNKNOWN,
            action_id=action_id,
            error=ErrorSummary(
                code="LEGACY_UNKNOWN",
                retryable=False,
                safe_message="The legacy Action outcome is unknown.",
            ),
        ),
    )
    projected = replay_events((*started.events, bare_unknown))
    await repository.commit(
        CommitRequest(
            command_id=command_id,
            request_hash=sha256(b"legacy-bare-unknown").hexdigest(),
            attempt_id=ATTEMPT_ID,
            expected_revision=started.state.revision,
            events=(bare_unknown,),
            outbox_actions=(),
            artifact_registrations=(),
            completed_delivery_action_ids=(action_id,),
            result=CommandResult(
                command_id=command_id,
                attempt_id=ATTEMPT_ID,
                accepted=True,
                revision=projected.revision,
                phase=projected.phase,
            ),
            checkpoint=False,
        )
    )

    result = await runner.run_until_blocked(ATTEMPT_ID, max_steps=4)
    loaded = await repository.load(ATTEMPT_ID)
    assert loaded is not None

    assert result.status is RunnerStatus.QUIESCENT
    assert loaded.state.phase is AttemptPhase.RUNNING
    assert loaded.state.pending_external == ()
    assert strategy.seen_event_ids == [bare_unknown.event_id]
    assert loaded.events[-1].trigger_sequence_no == bare_unknown.sequence_no
    assert backend.calls == ()
    assert repository._outbox == {}


@pytest.mark.asyncio
async def test_empty_continue_decision_is_quiescent_not_success() -> None:
    repository = InMemoryAttemptRepository()
    strategy_id = "empty-strategy"
    engine, backend, _, runner = _build_runtime(
        repository,
        strategy_id=strategy_id,
        strategy=EmptyDecisionStrategy(strategy_id),
        outcomes={},
        allowed_edges=frozenset(),
    )
    await _create_attempt(engine, attempt_id=ATTEMPT_ID, strategy_id=strategy_id)

    result = await runner.run_until_blocked(ATTEMPT_ID, max_steps=4)
    loaded = await repository.load(ATTEMPT_ID)
    assert loaded is not None

    assert result.status is RunnerStatus.QUIESCENT
    assert result.steps == 2
    assert loaded.state.phase is AttemptPhase.RUNNING
    assert loaded.state.actions == ()
    assert backend.calls == ()


@pytest.mark.asyncio
async def test_runner_forwards_external_requirements_and_blocks() -> None:
    repository = InMemoryAttemptRepository()
    strategy_id = "external-input"
    engine, backend, _, runner = _build_runtime(
        repository,
        strategy_id=strategy_id,
        strategy=AdditionalInputStrategy(strategy_id),
        outcomes={},
        allowed_edges=frozenset(),
    )
    await _create_attempt(engine, attempt_id=ATTEMPT_ID, strategy_id=strategy_id)

    result = await runner.run_until_blocked(ATTEMPT_ID, max_steps=4)

    loaded = await repository.load(ATTEMPT_ID)
    assert loaded is not None
    assert result.status is RunnerStatus.BLOCKED
    assert result.phase is AttemptPhase.WAITING_EXTERNAL
    assert loaded.state.pending_external[0].request_id == "runner-input-1"
    assert loaded.events[-1].event_type == "EXTERNAL_INPUT_REQUESTED"
    assert backend.calls == ()


@pytest.mark.asyncio
async def test_rejected_approval_never_reaches_executor_or_backend() -> None:
    repository = InMemoryAttemptRepository()
    strategy_id = "approval-reject"
    action_id = strategy_action_id(ATTEMPT_ID, strategy_id, 0)
    strategy = ApprovalSingleAgentStrategy(
        strategy_id=strategy_id,
        agent_id="worker-a",
        payload_ref=None,
        resource_requests=(
            ResourceRequest(resource=ResourceKind.MODEL_CALLS, amount=1),
        ),
    )
    engine, backend, executor, runner = _build_runtime(
        repository,
        strategy_id=strategy_id,
        strategy=strategy,
        outcomes={
            action_id: ActionSucceededOutcome(
                status=ActionStatus.SUCCEEDED,
                action_id=action_id,
                result_ref=_artifact("should-not-reach-backend"),
            )
        },
        allowed_edges=frozenset({(strategy_id, "worker-a")}),
    )
    await _create_attempt(engine, attempt_id=ATTEMPT_ID, strategy_id=strategy_id)
    blocked = await runner.run_until_blocked(ATTEMPT_ID, max_steps=4)
    waiting = await repository.load(ATTEMPT_ID)
    assert waiting is not None
    assert blocked.status is RunnerStatus.BLOCKED

    rejected = await engine.handle(
        SubmitExternalInput(
            schema_version=1,
            command_type="SUBMIT_EXTERNAL_INPUT",
            command_id=UUID("8a000000-0000-0000-0000-000000000001"),
            attempt_id=ATTEMPT_ID,
            expected_revision=waiting.state.revision,
            request_id="runner-approval-reject",
            response_kind=ExternalResponseKind.REJECT,
        )
    )
    step = await executor.run_once("approval-reject-worker")
    loaded = await repository.load(ATTEMPT_ID)
    assert loaded is not None

    assert rejected.accepted is True
    assert loaded.state.actions[0].status is ActionStatus.REJECTED
    assert step.status is ExecutorStepStatus.IDLE
    assert backend.calls == ()
    assert repository._outbox == {}


@pytest.mark.asyncio
async def test_runner_finishes_after_confirmed_unknown_reconciliation() -> None:
    repository = InMemoryAttemptRepository()
    strategy_id = "reconciliation-terminal"
    action_id = strategy_action_id(ATTEMPT_ID, strategy_id, 0)
    strategy = SingleAgentStrategy(
        strategy_id=strategy_id,
        agent_id="worker-a",
        payload_ref=None,
        resource_requests=(
            ResourceRequest(resource=ResourceKind.MODEL_CALLS, amount=1),
        ),
    )
    engine, backend, _, runner = _build_runtime(
        repository,
        strategy_id=strategy_id,
        strategy=strategy,
        outcomes={
            action_id: ActionUnknownOutcome(
                status=ActionStatus.OUTCOME_UNKNOWN,
                action_id=action_id,
                error=ErrorSummary(
                    code="OUTCOME_UNKNOWN",
                    retryable=False,
                    safe_message="The backend outcome could not be observed.",
                ),
            )
        },
        allowed_edges=frozenset({(strategy_id, "worker-a")}),
    )
    await _create_attempt(engine, attempt_id=ATTEMPT_ID, strategy_id=strategy_id)

    blocked = await runner.run_until_blocked(ATTEMPT_ID, max_steps=8)
    waiting = await repository.load(ATTEMPT_ID)
    assert waiting is not None
    assert blocked.status is RunnerStatus.BLOCKED
    assert waiting.state.phase is AttemptPhase.WAITING_EXTERNAL
    assert tuple(call[0] for call in backend.calls) == (action_id,)
    request = waiting.state.pending_external[0]
    reconciled_result = _artifact("runner-reconciled-action")
    confirmed = await engine.handle(
        SubmitExternalInput(
            schema_version=1,
            command_type="SUBMIT_EXTERNAL_INPUT",
            command_id=UUID("90000000-0000-0000-0000-000000000001"),
            attempt_id=ATTEMPT_ID,
            expected_revision=waiting.state.revision,
            request_id=request.request_id,
            response_kind=ExternalResponseKind.CONFIRM_SUCCEEDED,
            response_ref=reconciled_result,
        )
    )
    assert confirmed.accepted

    result = await runner.run_until_blocked(ATTEMPT_ID, max_steps=4)

    loaded = await repository.load(ATTEMPT_ID)
    assert loaded is not None
    assert result.status is RunnerStatus.TERMINAL
    assert loaded.state.phase is AttemptPhase.SUCCEEDED
    assert loaded.state.result_ref == reconciled_result
    assert loaded.state.actions[0].status is ActionStatus.OUTCOME_UNKNOWN
    assert loaded.state.actions[0].reconciled_status is ActionStatus.SUCCEEDED


@pytest.mark.asyncio
async def test_malformed_strategy_decision_fails_with_stable_runner_invariant() -> None:
    repository = InMemoryAttemptRepository()
    strategy_id = "malformed-strategy"
    engine, _, _, runner = _build_runtime(
        repository,
        strategy_id=strategy_id,
        strategy=MalformedDecisionStrategy(),
        outcomes={},
        allowed_edges=frozenset(),
    )
    await _create_attempt(engine, attempt_id=ATTEMPT_ID, strategy_id=strategy_id)

    with pytest.raises(RunnerInvariantError):
        await runner.run_until_blocked(ATTEMPT_ID, max_steps=4)


@pytest.mark.asyncio
async def test_terminal_directive_dispatches_remaining_action_without_strategy_call() -> None:
    repository = InMemoryAttemptRepository()
    strategy_id = "parallel-terminal"
    action_ids = tuple(
        strategy_action_id(ATTEMPT_ID, strategy_id, ordinal)
        for ordinal in range(3)
    )
    outcomes = {
        action_ids[0]: ActionFailedOutcome(
            status=ActionStatus.FAILED,
            action_id=action_ids[0],
            error=ErrorSummary(
                code="FIRST_FAILED",
                retryable=False,
                safe_message="The first action failed.",
            ),
        ),
        action_ids[1]: ActionSucceededOutcome(
            status=ActionStatus.SUCCEEDED,
            action_id=action_ids[1],
            result_ref=_artifact("parallel-result-1"),
        ),
        action_ids[2]: ActionSucceededOutcome(
            status=ActionStatus.SUCCEEDED,
            action_id=action_ids[2],
            result_ref=_artifact("parallel-result-2"),
        ),
    }
    engine, backend, executor, runner = _build_runtime(
        repository,
        strategy_id=strategy_id,
        strategy=ForbiddenAfterTerminalStrategy(),
        outcomes=outcomes,
        allowed_edges=frozenset({(strategy_id, "worker-a")}),
    )
    await _create_attempt(engine, attempt_id=ATTEMPT_ID, strategy_id=strategy_id)
    planned = await repository.load(ATTEMPT_ID)
    assert planned is not None
    started = await engine.handle(
        StartAttempt(
            schema_version=1,
            command_type="START_ATTEMPT",
            command_id=transition_command_id(ATTEMPT_ID, 1, "start-attempt"),
            attempt_id=ATTEMPT_ID,
            expected_revision=planned.state.revision,
        )
    )
    assert started.accepted
    loaded = await repository.load(ATTEMPT_ID)
    assert loaded is not None
    trigger = loaded.events[-1]
    proposals = tuple(
        ActionProposal(
            action_id=action_id,
            action_type=ActionType.INVOKE_AGENT,
            actor=strategy_id,
            target_ids=("worker-a",),
            invocation_id=strategy_invocation_id(
                ATTEMPT_ID, strategy_id, ordinal
            ),
            causal_parent_id=str(trigger.event_id),
            payload_ref=None,
            recovery_policy=RecoveryPolicy.REPLAY_SAFE,
            requested_timeout=30,
            batch_id="parallel-batch",
            call_depth=0,
            resource_requests=(
                ResourceRequest(resource=ResourceKind.MODEL_CALLS, amount=1),
            ),
        )
        for ordinal, action_id in enumerate(action_ids)
    )
    accepted = await engine.handle(
        ApplyStrategyDecision(
            schema_version=1,
            command_type="APPLY_STRATEGY_DECISION",
            command_id=transition_command_id(
                ATTEMPT_ID, trigger.sequence_no, "apply-strategy-decision"
            ),
            attempt_id=ATTEMPT_ID,
            expected_revision=loaded.state.revision,
            trigger_sequence_no=trigger.sequence_no,
            strategy=_initial_strategy(strategy_id),
            proposals=proposals,
            directive=StrategyDirective.CONTINUE,
        )
    )
    assert accepted.accepted
    assert (await executor.run_once("manual-worker")).status.value == "COMPLETED"
    assert (await executor.run_once("manual-worker")).status.value == "COMPLETED"

    observed = await repository.load(ATTEMPT_ID)
    assert observed is not None
    failed_trigger = next(
        event for event in observed.events if isinstance(event, ActionFailed)
    )
    terminal = await engine.handle(
        ApplyStrategyDecision(
            schema_version=1,
            command_type="APPLY_STRATEGY_DECISION",
            command_id=transition_command_id(
                ATTEMPT_ID,
                failed_trigger.sequence_no,
                "apply-strategy-decision",
            ),
            attempt_id=ATTEMPT_ID,
            expected_revision=observed.state.revision,
            trigger_sequence_no=failed_trigger.sequence_no,
            strategy=_initial_strategy(strategy_id),
            proposals=(),
            directive=StrategyDirective.FAIL,
            error=ErrorSummary(
                code="PARALLEL_FAILED",
                retryable=False,
                safe_message="The parallel batch failed.",
            ),
        )
    )
    assert terminal.accepted

    result = await runner.run_until_blocked(ATTEMPT_ID, max_steps=8)
    final = await repository.load(ATTEMPT_ID)
    assert final is not None
    assert result.status is RunnerStatus.TERMINAL
    assert final.state.phase is AttemptPhase.FAILED
    assert tuple(call[0] for call in backend.calls) == action_ids


@pytest.mark.asyncio
async def test_unknown_outcome_blocks_an_unstarted_accepted_action() -> None:
    repository = InMemoryAttemptRepository()
    strategy_id = "unknown-continue"
    action_ids = tuple(
        strategy_action_id(ATTEMPT_ID, strategy_id, ordinal)
        for ordinal in range(2)
    )
    outcomes = {
        action_ids[0]: ActionUnknownOutcome(
            status=ActionStatus.OUTCOME_UNKNOWN,
            action_id=action_ids[0],
            error=ErrorSummary(
                code="UNKNOWN",
                retryable=False,
                safe_message="The first outcome is unknown.",
            ),
        ),
        action_ids[1]: ActionSucceededOutcome(
            status=ActionStatus.SUCCEEDED,
            action_id=action_ids[1],
            result_ref=_artifact("unknown-batch-result"),
        ),
    }
    _, backend, executor, runner, _ = await _prepare_scripted_batch(
        repository,
        strategy_id=strategy_id,
        strategy=EmptyDecisionStrategy(strategy_id),
        outcomes=outcomes,
    )
    assert (await executor.run_once("manual-worker")).status is ExecutorStepStatus.COMPLETED

    result = await runner.run_until_blocked(ATTEMPT_ID, max_steps=8)
    loaded = await repository.load(ATTEMPT_ID)
    assert loaded is not None

    assert result.status is RunnerStatus.BLOCKED
    assert loaded.state.phase is AttemptPhase.WAITING_EXTERNAL
    assert tuple(action.status for action in loaded.state.actions) == (
        ActionStatus.OUTCOME_UNKNOWN,
        ActionStatus.ACCEPTED,
    )
    assert tuple(call[0] for call in backend.calls) == (action_ids[0],)
    assert {key[1] for key in repository._outbox} == {action_ids[1]}


@pytest.mark.asyncio
async def test_terminal_directive_is_rejected_while_reconciliation_is_pending() -> None:
    repository = InMemoryAttemptRepository()
    strategy_id = "unknown-terminal"
    action_ids = tuple(
        strategy_action_id(ATTEMPT_ID, strategy_id, ordinal)
        for ordinal in range(2)
    )
    outcomes = {
        action_ids[0]: ActionUnknownOutcome(
            status=ActionStatus.OUTCOME_UNKNOWN,
            action_id=action_ids[0],
            error=ErrorSummary(
                code="UNKNOWN",
                retryable=False,
                safe_message="The first outcome is unknown.",
            ),
        ),
        action_ids[1]: ActionSucceededOutcome(
            status=ActionStatus.SUCCEEDED,
            action_id=action_ids[1],
            result_ref=_artifact("terminal-unknown-result"),
        ),
    }
    engine, backend, executor, runner, _ = await _prepare_scripted_batch(
        repository,
        strategy_id=strategy_id,
        strategy=ForbiddenAfterTerminalStrategy(),
        outcomes=outcomes,
    )
    assert (await executor.run_once("manual-worker")).status is ExecutorStepStatus.COMPLETED
    observed = await repository.load(ATTEMPT_ID)
    assert observed is not None
    unknown = next(
        event for event in observed.events if isinstance(event, ActionOutcomeUnknown)
    )
    terminal = await engine.handle(
        ApplyStrategyDecision(
            schema_version=1,
            command_type="APPLY_STRATEGY_DECISION",
            command_id=transition_command_id(
                ATTEMPT_ID, unknown.sequence_no, "apply-strategy-decision"
            ),
            attempt_id=ATTEMPT_ID,
            expected_revision=observed.state.revision,
            trigger_sequence_no=unknown.sequence_no,
            strategy=_initial_strategy(strategy_id),
            proposals=(),
            directive=StrategyDirective.FAIL,
            error=ErrorSummary(
                code="UNKNOWN_BATCH",
                retryable=False,
                safe_message="The batch has an unresolved outcome.",
            ),
        )
    )
    assert terminal.accepted is False
    assert terminal.error is not None
    assert terminal.error.code == "ILLEGAL_TRANSITION"

    result = await runner.run_until_blocked(ATTEMPT_ID, max_steps=8)
    loaded = await repository.load(ATTEMPT_ID)
    assert loaded is not None

    assert result.status is RunnerStatus.BLOCKED
    assert loaded.state.phase is AttemptPhase.WAITING_EXTERNAL
    assert tuple(action.status for action in loaded.state.actions) == (
        ActionStatus.OUTCOME_UNKNOWN,
        ActionStatus.ACCEPTED,
    )
    assert tuple(call[0] for call in backend.calls) == (action_ids[0],)
    assert {key[1] for key in repository._outbox} == {action_ids[1]}


@pytest.mark.asyncio
async def test_executor_idle_reloads_changed_target_and_continues() -> None:
    repository = InMemoryAttemptRepository()
    strategy_id = "single-agent"
    action_id = strategy_action_id(ATTEMPT_ID, strategy_id, 0)
    strategy = SingleAgentStrategy(
        strategy_id=strategy_id,
        agent_id="worker-a",
        payload_ref=None,
    )
    engine, backend, executor, first_runner = _build_runtime(
        repository,
        strategy_id=strategy_id,
        strategy=strategy,
        outcomes={
            action_id: ActionSucceededOutcome(
                status=ActionStatus.SUCCEEDED,
                action_id=action_id,
                result_ref=_artifact("idle-race-result"),
            )
        },
        allowed_edges=frozenset({(strategy_id, "worker-a")}),
    )
    await _create_attempt(engine, attempt_id=ATTEMPT_ID, strategy_id=strategy_id)
    with pytest.raises(StepLimitExceeded):
        await first_runner.run_until_blocked(ATTEMPT_ID, max_steps=2)
    racing_runner = DeterministicAttemptRunner(
        repository=repository,
        engine=engine,
        executor=CompletingButIdleExecutor(executor),
        strategies={strategy_id: strategy},
        legal_topology={},
        worker_id="runner-worker",
    )

    result = await racing_runner.run_until_blocked(ATTEMPT_ID, max_steps=8)
    loaded = await repository.load(ATTEMPT_ID)
    assert loaded is not None

    assert result.status is RunnerStatus.TERMINAL
    assert loaded.state.phase is AttemptPhase.SUCCEEDED
    assert len(backend.calls) == 1


async def _deterministic_sqlite_run(database_path: Path):
    repository = SQLiteAttemptRepository(database_path)
    result, loaded, backend, _ = await _run_single(repository)
    assert result.status is RunnerStatus.TERMINAL
    with sqlite3.connect(database_path) as connection:
        outbox_count = connection.execute(
            "SELECT COUNT(*) FROM action_outbox"
        ).fetchone()[0]
    assert outbox_count == 0
    accepted = tuple(
        event for event in loaded.events if isinstance(event, ActionAccepted)
    )
    succeeded = tuple(
        event for event in loaded.events if isinstance(event, ActionSucceeded)
    )
    assert len(accepted) == len(succeeded) == 1
    assert accepted[0].action.action_id == succeeded[0].action_id
    replayed = replay_events(loaded.events)
    assert canonical_state_bytes(replayed) == canonical_state_bytes(loaded.state)
    assert sha256(canonical_state_bytes(replayed)).hexdigest() == loaded.state_hash
    return loaded, backend.calls


@pytest.mark.asyncio
async def test_memory_and_sqlite_are_equivalent_and_sqlite_runs_are_byte_identical(
    tmp_path: Path,
) -> None:
    memory_result, memory_loaded, _, _ = await _run_single(
        InMemoryAttemptRepository()
    )
    first, first_calls = await _deterministic_sqlite_run(tmp_path / "first.sqlite3")
    second, second_calls = await _deterministic_sqlite_run(tmp_path / "second.sqlite3")

    assert memory_result.status is RunnerStatus.TERMINAL
    assert canonical_state_bytes(memory_loaded.state) == canonical_state_bytes(first.state)
    assert memory_loaded.state_hash == first.state_hash
    assert tuple(
        (type(event), event.event_type, event.event_id, event.logical_time)
        for event in memory_loaded.events
    ) == tuple(
        (type(event), event.event_type, event.event_id, event.logical_time)
        for event in first.events
    )
    assert tuple(
        (type(event), event.event_type, event.event_id, event.logical_time)
        for event in first.events
    ) == tuple(
        (type(event), event.event_type, event.event_id, event.logical_time)
        for event in second.events
    )
    assert canonical_state_bytes(first.state) == canonical_state_bytes(second.state)
    assert first.state_hash == second.state_hash
    assert first_calls == second_calls


async def _deterministic_static_sqlite_run(database_path: Path):
    repository = SQLiteAttemptRepository(database_path)
    result, loaded, backend, action_ids = await _run_static_workflow(repository)
    assert result.status is RunnerStatus.TERMINAL
    assert loaded.state.phase is AttemptPhase.SUCCEEDED
    assert tuple(call[0] for call in backend.calls) == action_ids
    assert tuple(action.target_ids for action in loaded.state.actions) == (
        ("worker-a",),
        ("worker-a",),
    )
    accepted = tuple(
        event for event in loaded.events if isinstance(event, ActionAccepted)
    )
    succeeded = tuple(
        event for event in loaded.events if isinstance(event, ActionSucceeded)
    )
    assert tuple(event.action.action_id for event in accepted) == action_ids
    assert tuple(event.action_id for event in succeeded) == action_ids
    assert all(
        sum(event.action_id == accepted_event.action.action_id for event in succeeded)
        == 1
        for accepted_event in accepted
    )
    with sqlite3.connect(database_path) as connection:
        assert connection.execute(
            "SELECT COUNT(*) FROM action_outbox"
        ).fetchone()[0] == 0
    return loaded


@pytest.mark.asyncio
async def test_static_workflow_is_identical_across_memory_and_fresh_sqlite_runs(
    tmp_path: Path,
) -> None:
    memory_repository = InMemoryAttemptRepository()
    memory_result, memory, _, action_ids = await _run_static_workflow(
        memory_repository
    )
    first = await _deterministic_static_sqlite_run(
        tmp_path / "static-first.sqlite3"
    )
    second = await _deterministic_static_sqlite_run(
        tmp_path / "static-second.sqlite3"
    )

    assert memory_result.status is RunnerStatus.TERMINAL
    assert memory_repository._outbox == {}
    assert tuple(action.action_id for action in memory.state.actions) == action_ids
    memory_signature = tuple(
        (type(event), event.event_type, event.event_id, event.logical_time)
        for event in memory.events
    )
    first_signature = tuple(
        (type(event), event.event_type, event.event_id, event.logical_time)
        for event in first.events
    )
    second_signature = tuple(
        (type(event), event.event_type, event.event_id, event.logical_time)
        for event in second.events
    )
    assert memory_signature == first_signature == second_signature
    assert (
        canonical_state_bytes(memory.state)
        == canonical_state_bytes(first.state)
        == canonical_state_bytes(second.state)
    )
    assert memory.state_hash == first.state_hash == second.state_hash


@pytest.mark.asyncio
async def test_multi_decision_legacy_events_replay_to_loaded_state_without_mutation(
) -> None:
    result, loaded, _, _ = await _run_static_workflow(
        InMemoryAttemptRepository()
    )
    legacy_fields = {
        "trigger_sequence_no",
        "proposals",
        "directive",
        "result_ref",
        "error",
    }
    legacy_payloads = [event.model_dump(mode="json") for event in loaded.events]
    decision_payloads = [
        payload
        for event, payload in zip(loaded.events, legacy_payloads, strict=True)
        if isinstance(event, StrategyDecisionRecorded)
    ]
    for payload in decision_payloads:
        for field in legacy_fields:
            payload.pop(field)
    raw_payload_bytes = tuple(
        json.dumps(payload, sort_keys=True, separators=(",", ":")).encode("utf-8")
        for payload in legacy_payloads
    )
    raw_payload_hashes = tuple(
        sha256(payload).hexdigest() for payload in raw_payload_bytes
    )

    assert result.status is RunnerStatus.TERMINAL
    assert len(decision_payloads) == 3
    assert all(legacy_fields.isdisjoint(payload) for payload in decision_payloads)

    parsed_events = tuple(parse_domain_event(payload) for payload in legacy_payloads)
    reparsed_events = tuple(parse_domain_event(event) for event in parsed_events)
    replayed = replay_events(reparsed_events)
    parsed_decisions = tuple(
        event for event in parsed_events if isinstance(event, StrategyDecisionRecorded)
    )
    reparsed_decisions = tuple(
        event
        for event in reparsed_events
        if isinstance(event, StrategyDecisionRecorded)
    )
    assert all(event._legacy_cursor_fingerprint for event in parsed_decisions)
    assert tuple(
        event._legacy_cursor_fingerprint for event in parsed_decisions
    ) == tuple(event._legacy_cursor_fingerprint for event in reparsed_decisions)

    current_payload_bytes = tuple(
        json.dumps(payload, sort_keys=True, separators=(",", ":")).encode("utf-8")
        for payload in legacy_payloads
    )
    assert current_payload_bytes == raw_payload_bytes
    assert tuple(
        sha256(payload).hexdigest() for payload in current_payload_bytes
    ) == raw_payload_hashes
    assert all(
        "_legacy_cursor_origin" not in event.model_dump(mode="json")
        and "_legacy_cursor_fingerprint" not in event.model_dump(mode="json")
        and "legacy_cursor_origin" not in event.model_dump_json()
        and "legacy_cursor_fingerprint" not in event.model_dump_json()
        for event in reparsed_events
        if isinstance(event, StrategyDecisionRecorded)
    )
    assert canonical_state_bytes(replayed) == canonical_state_bytes(loaded.state)
    assert sha256(canonical_state_bytes(replayed)).hexdigest() == loaded.state_hash


@pytest.mark.asyncio
async def test_sqlite_reopen_continues_without_restarting_deterministic_ids(
    tmp_path: Path,
) -> None:
    database_path = tmp_path / "reopen.sqlite3"
    first_repository = SQLiteAttemptRepository(database_path)
    strategy_id = "single-agent"
    action_id = strategy_action_id(ATTEMPT_ID, strategy_id, 0)
    strategy = SingleAgentStrategy(
        strategy_id=strategy_id,
        agent_id="worker-a",
        payload_ref=_artifact("payload"),
    )
    outcome = ActionSucceededOutcome(
        status=ActionStatus.SUCCEEDED,
        action_id=action_id,
        result_ref=_artifact("reopen-result"),
    )
    ids = SequentialIdFactory()
    engine, first_backend, _, first_runner = _build_runtime(
        first_repository,
        strategy_id=strategy_id,
        strategy=strategy,
        outcomes={action_id: outcome},
        allowed_edges=frozenset({(strategy_id, "worker-a")}),
        ids=ids,
    )
    await _create_attempt(engine, attempt_id=ATTEMPT_ID, strategy_id=strategy_id)

    with pytest.raises(StepLimitExceeded):
        await first_runner.run_until_blocked(ATTEMPT_ID, max_steps=4)
    partially_loaded = await first_repository.load(ATTEMPT_ID)
    assert partially_loaded is not None
    terminal_decisions = tuple(
        event
        for event in partially_loaded.events
        if isinstance(event, StrategyDecisionRecorded)
        and event.directive.value == "SUCCEED"
    )
    assert len(terminal_decisions) == 1
    assert partially_loaded.state.phase is AttemptPhase.RUNNING
    assert len(first_backend.calls) == 1

    reopened = SQLiteAttemptRepository(database_path)
    second_engine, backend, executor, _ = _build_runtime(
        reopened,
        strategy_id=strategy_id,
        strategy=strategy,
        outcomes={action_id: outcome},
        allowed_edges=frozenset({(strategy_id, "worker-a")}),
        ids=ids,
    )
    second_runner = DeterministicAttemptRunner(
        repository=reopened,
        engine=second_engine,
        executor=executor,
        strategies={},
        legal_topology={},
        worker_id="runner-worker",
    )
    result = await second_runner.run_until_blocked(ATTEMPT_ID, max_steps=16)
    loaded = await reopened.load(ATTEMPT_ID)
    assert loaded is not None

    assert result.status is RunnerStatus.TERMINAL
    assert loaded.state.phase is AttemptPhase.SUCCEEDED
    assert len(backend.calls) == 0
    assert len({event.event_id for event in loaded.events}) == len(loaded.events)


@pytest.mark.asyncio
async def test_runner_step_limit_and_missing_strategy_fail_closed() -> None:
    repository = InMemoryAttemptRepository()
    strategy_id = "single-agent"
    strategy = SingleAgentStrategy(
        strategy_id=strategy_id,
        agent_id="worker-a",
        payload_ref=None,
    )
    action_id = strategy_action_id(ATTEMPT_ID, strategy_id, 0)
    engine, _, executor, runner = _build_runtime(
        repository,
        strategy_id=strategy_id,
        strategy=strategy,
        outcomes={
            action_id: ActionSucceededOutcome(
                status=ActionStatus.SUCCEEDED,
                action_id=action_id,
                result_ref=_artifact("result"),
            )
        },
        allowed_edges=frozenset({(strategy_id, "worker-a")}),
    )
    await _create_attempt(engine, attempt_id=ATTEMPT_ID, strategy_id=strategy_id)

    with pytest.raises(StepLimitExceeded, match="step limit"):
        await runner.run_until_blocked(ATTEMPT_ID, max_steps=0)

    missing_runner = DeterministicAttemptRunner(
        repository=repository,
        engine=engine,
        executor=executor,
        strategies={},
        legal_topology={},
        worker_id="runner-worker",
    )
    with pytest.raises(RunnerInvariantError, match="Strategy"):
        await missing_runner.run_until_blocked(ATTEMPT_ID, max_steps=2)
    still_planned = await repository.load(ATTEMPT_ID)
    assert still_planned is not None
    assert still_planned.state.phase is AttemptPhase.PLANNED


@pytest.mark.asyncio
async def test_step_limit_is_checked_before_strategy_invocation() -> None:
    repository = InMemoryAttemptRepository()
    strategy_id = "counting-strategy"
    strategy = CountingEmptyDecisionStrategy(strategy_id)
    engine, _, _, runner = _build_runtime(
        repository,
        strategy_id=strategy_id,
        strategy=strategy,
        outcomes={},
        allowed_edges=frozenset(),
    )
    await _create_attempt(engine, attempt_id=ATTEMPT_ID, strategy_id=strategy_id)

    with pytest.raises(StepLimitExceeded):
        await runner.run_until_blocked(ATTEMPT_ID, max_steps=1)

    loaded = await repository.load(ATTEMPT_ID)
    assert loaded is not None
    assert loaded.state.phase is AttemptPhase.RUNNING
    assert strategy.calls == 0


def test_runner_contracts_are_frozen_and_registry_inputs_are_defensive() -> None:
    result = RunnerResult(
        status=RunnerStatus.QUIESCENT,
        attempt_id=ATTEMPT_ID,
        phase=AttemptPhase.RUNNING,
        revision=2,
        steps=0,
    )
    with pytest.raises(Exception):
        result.steps = 1
    assert {status.value for status in RunnerStatus} == {
        "BLOCKED",
        "QUIESCENT",
        "TERMINAL",
    }


def test_runner_rejects_non_json_topology_at_construction() -> None:
    repository = InMemoryAttemptRepository()
    strategy_id = "single-agent"
    strategy = SingleAgentStrategy(
        strategy_id=strategy_id,
        agent_id="worker-a",
        payload_ref=None,
    )
    action_id = strategy_action_id(ATTEMPT_ID, strategy_id, 0)

    with pytest.raises(ValueError, match="legal_topology"):
        _build_runtime(
            repository,
            strategy_id=strategy_id,
            strategy=strategy,
            outcomes={
                action_id: ActionSucceededOutcome(
                    status=ActionStatus.SUCCEEDED,
                    action_id=action_id,
                    result_ref=_artifact("result"),
                )
            },
            allowed_edges=frozenset({(strategy_id, "worker-a")}),
            legal_topology={"invalid": {"not-json"}},
        )
