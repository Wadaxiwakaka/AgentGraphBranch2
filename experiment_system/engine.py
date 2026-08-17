from __future__ import annotations

from datetime import datetime
from typing import Any
from uuid import UUID

from .actions import (
    ActionCancelledOutcome,
    ActionFailedOutcome,
    ActionProposal,
    ActionStatus,
    ActionSucceededOutcome,
    ActionType,
    NormalizedAction,
)
from .budget import BudgetGuard, GuardRejection, ReservationPlan
from .commands import (
    ApplyStrategyDecision,
    CancelAttempt,
    CommandBase,
    CreateAttempt,
    ExpireAttempt,
    FinishAttempt,
    PauseAttempt,
    ReportActionOutcome,
    ReportActionStarted,
    RecoverAttempt,
    ResumeAttempt,
    StartAttempt,
    SubmitExternalInput,
    command_request_hash,
    outcome_reconciliation_request_id,
)
from .contract import (
    ArtifactVerificationError,
    ArtifactVerifier,
    Clock,
    CommandResult,
    IdFactory,
    StrategyDirective,
)
from .events import (
    ActionAccepted,
    ActionCancellationRequested,
    ActionCancelled,
    ActionFailed,
    ActionOutcomeReconciled,
    ActionOutcomeUnknown,
    ActionProposed,
    ActionRejected,
    ActionStarted,
    ActionSucceeded,
    ActionTimedOut,
    AttemptCancelled,
    AttemptFailed,
    AttemptInterrupted,
    AttemptPaused,
    AttemptPlanned,
    AttemptRecoveryRequested,
    AttemptResumed,
    AttemptStarted,
    AttemptSucceeded,
    AttemptTimedOut,
    BudgetReservationEntry,
    BudgetReleased,
    BudgetReserved,
    BudgetSettled,
    BudgetUncertainSettled,
    CancelRequested,
    DomainEvent,
    ExternalInputApproved,
    ExternalInputExpired,
    ExternalInputReceived,
    ExternalInputRejected,
    ExternalInputRequested,
    InvocationCompleted,
    InvocationFailed,
    InvocationRequested,
    InvocationStarted,
    PauseRequested,
    StrategyDecisionRecorded,
    effective_strategy_triggers,
    is_strategy_trigger,
)
from .reducer import StateTransitionError, replay_events
from .state import (
    ActionState,
    ArtifactRef,
    AttemptPhase,
    AttemptState,
    BudgetState,
    ErrorSummary,
    ExternalRequest,
    ExternalRequestKind,
    ExternalResponseKind,
    ResourceBudget,
    TERMINAL_ACTION_STATUSES,
)
from .store import (
    ArtifactRegistration,
    AttemptRepository,
    CommandReuseError,
    CommitRequest,
    DeliveryClaim,
    InvalidCommit,
    LoadedAttempt,
    RejectedCommandRequest,
    RevisionConflict,
    StaleDeliveryClaim,
)
from .topology import TopologyGuard


_REJECTION_MESSAGES = {
    "ARTIFACT_VERIFICATION_FAILED": "A referenced artifact could not be verified.",
    "ATTEMPT_ALREADY_EXISTS": "The Attempt already exists.",
    "ATTEMPT_NOT_FOUND": "The Attempt does not exist.",
    "COMMAND_REUSE": "The Command identifier was already used for another request.",
    "DUPLICATE_ACTION_ID": "A Strategy decision contains a duplicate Action identifier.",
    "DUPLICATE_EXTERNAL_REQUEST_ID": (
        "The external request identifier was already used in this Attempt."
    ),
    "DUPLICATE_INVOCATION_ID": "A Strategy decision reuses an Invocation identifier.",
    "DEADLINE_MISMATCH": "The expiry deadline does not match the Attempt deadline.",
    "DEADLINE_NOT_REACHED": "The Attempt deadline has not been reached.",
    "INVALID_ACTION_BATCH": "Parallel Actions must share one explicit batch identifier.",
    "INVALID_STRATEGY_TRIGGER": (
        "The Strategy decision does not name a new eligible committed Event."
    ),
    "ILLEGAL_TRANSITION": "The Command is not legal in the current Attempt state.",
    "INVALID_COMMIT": "The proposed Attempt transition is inconsistent.",
    "NONTERMINAL_ACTIONS": "The Attempt still has Actions without a terminal observation.",
    "REVISION_CONFLICT": "The Attempt revision does not match the Command.",
    "STALE_DELIVERY_CLAIM": "The Action delivery claim is no longer current.",
    "STRATEGY_MISMATCH": "The Strategy state does not belong to this Attempt.",
    "STRATEGY_COMPLETION_MISMATCH": (
        "Attempt completion does not match the committed Strategy decision."
    ),
    "STRATEGY_COMPLETION_REQUIRED": "The Strategy has not committed a terminal decision.",
    "UNSUPPORTED_COMMAND": "This Command is not handled by the current kernel phase.",
    "UNSAFE_IN_FLIGHT_ACTIONS": (
        "The Attempt has in-flight Actions that cannot be expired safely."
    ),
    "UNRESOLVED_ACTION_OUTCOME": "An Action outcome still requires reconciliation.",
}


class _EventBuilder:
    def __init__(
        self,
        *,
        attempt_id: str,
        trial_id: str,
        command_id: UUID,
        initial_revision: int,
        causal_parent_id: UUID | None,
        wall_time_utc: datetime,
        id_factory: IdFactory,
    ) -> None:
        self.attempt_id = attempt_id
        self.trial_id = trial_id
        self.command_id = command_id
        self.sequence_no = initial_revision
        self.causal_parent_id = causal_parent_id
        self.wall_time_utc = wall_time_utc
        self.id_factory = id_factory

    def add(self, event_class: type[Any], *, event_type: str, **payload: object) -> DomainEvent:
        self.sequence_no += 1
        event_id = self.id_factory.new_uuid()
        event = event_class(
            schema_version=1,
            event_id=event_id,
            trial_id=self.trial_id,
            attempt_id=self.attempt_id,
            sequence_no=self.sequence_no,
            event_type=event_type,
            command_id=self.command_id,
            causal_parent_id=self.causal_parent_id,
            logical_time=self.sequence_no,
            wall_time_utc=self.wall_time_utc,
            **payload,
        )
        self.causal_parent_id = event_id
        return event


class AttemptEngine:
    def __init__(
        self,
        *,
        repository: AttemptRepository,
        clock: Clock,
        id_factory: IdFactory,
        artifact_verifier: ArtifactVerifier,
        allowed_edges: frozenset[tuple[str, str]],
    ) -> None:
        if not isinstance(allowed_edges, frozenset):
            raise ValueError("allowed_edges must be a frozenset")
        self._repository = repository
        self._clock = clock
        self._id_factory = id_factory
        self._artifact_verifier = artifact_verifier
        self._allowed_edges = allowed_edges

    async def handle(
        self,
        command: CommandBase,
        *,
        delivery_claim: DeliveryClaim | None = None,
    ) -> CommandResult:
        async with self._repository.command_scope(command.attempt_id):
            return await self._handle_serialized(
                command,
                delivery_claim=delivery_claim,
            )

    async def _handle_serialized(
        self,
        command: CommandBase,
        *,
        delivery_claim: DeliveryClaim | None,
    ) -> CommandResult:
        request_hash = command_request_hash(command)
        duplicate = await self._repository.find_command(
            command.attempt_id,
            command.command_id,
        )
        if duplicate is not None and duplicate.request_hash == request_hash:
            return duplicate.result

        loaded = await self._repository.load(command.attempt_id)
        if duplicate is not None:
            return self._rejected(command, loaded, "COMMAND_REUSE")

        precondition = self._precondition_error(command, loaded)
        if precondition is not None:
            if precondition == "REVISION_CONFLICT":
                return self._rejected(command, loaded, precondition)
            return await self._record_rejection(
                command,
                request_hash,
                loaded,
                precondition,
            )

        references = self._artifact_references(command)
        try:
            for ref in references:
                if ref.capture_class == "full":
                    self._artifact_verifier.verify(ref)
        except ArtifactVerificationError:
            return await self._record_rejection(
                command,
                request_hash,
                loaded,
                "ARTIFACT_VERIFICATION_FAILED",
            )

        wall_time_utc = self._clock.now_utc()
        if (
            isinstance(command, ExpireAttempt)
            and wall_time_utc < command.deadline_at
        ):
            return await self._record_rejection(
                command,
                request_hash,
                loaded,
                "DEADLINE_NOT_REACHED",
            )
        try:
            events, outbox_actions, completed_action_ids = self._build_transition(
                command,
                loaded,
                wall_time_utc=wall_time_utc,
            )
            complete_events = (() if loaded is None else loaded.events) + events
            projected = replay_events(complete_events)
            result = CommandResult(
                command_id=command.command_id,
                attempt_id=command.attempt_id,
                accepted=True,
                revision=projected.revision,
                phase=projected.phase,
            )
            committed = await self._repository.commit(
                CommitRequest(
                    command_id=command.command_id,
                    request_hash=request_hash,
                    attempt_id=command.attempt_id,
                    expected_revision=command.expected_revision,
                    events=events,
                    outbox_actions=outbox_actions,
                    artifact_registrations=tuple(
                        ArtifactRegistration(ref=ref) for ref in references
                    ),
                    completed_delivery_action_ids=completed_action_ids,
                    result=result,
                    checkpoint=(
                        self._should_checkpoint(command, projected)
                        or (
                            isinstance(command, (CancelAttempt, ExpireAttempt))
                            and loaded is not None
                            and loaded.state.phase
                            is AttemptPhase.WAITING_EXTERNAL
                        )
                    ),
                    delivery_claim=delivery_claim,
                )
            )
            return committed.command_result
        except CommandReuseError:
            return self._rejected(command, loaded, "COMMAND_REUSE")
        except RevisionConflict:
            latest = await self._repository.load(command.attempt_id)
            return self._rejected(command, latest, "REVISION_CONFLICT")
        except StaleDeliveryClaim:
            latest = await self._repository.load(command.attempt_id)
            return self._rejected(command, latest, "STALE_DELIVERY_CLAIM")
        except InvalidCommit:
            latest = await self._repository.load(command.attempt_id)
            return await self._record_rejection(
                command,
                request_hash,
                latest,
                "INVALID_COMMIT",
            )
        except StateTransitionError:
            latest = await self._repository.load(command.attempt_id)
            return await self._record_rejection(
                command,
                request_hash,
                latest,
                "ILLEGAL_TRANSITION",
            )

    async def _record_rejection(
        self,
        command: CommandBase,
        request_hash: str,
        loaded: LoadedAttempt | None,
        code: str,
    ) -> CommandResult:
        result = self._rejected(command, loaded, code)
        try:
            return await self._repository.record_rejection(
                RejectedCommandRequest(
                    command_id=command.command_id,
                    request_hash=request_hash,
                    attempt_id=command.attempt_id,
                    result=result,
                )
            )
        except CommandReuseError:
            latest = await self._repository.load(command.attempt_id)
            return self._rejected(command, latest, "COMMAND_REUSE")

    @staticmethod
    def _precondition_error(
        command: CommandBase,
        loaded: LoadedAttempt | None,
    ) -> str | None:
        if isinstance(command, CreateAttempt):
            if loaded is not None:
                return "ATTEMPT_ALREADY_EXISTS"
            return None
        if loaded is None:
            return "ATTEMPT_NOT_FOUND"
        state = loaded.state
        if command.expected_revision != state.revision:
            return "REVISION_CONFLICT"
        if isinstance(command, StartAttempt):
            return None if state.phase is AttemptPhase.PLANNED else "ILLEGAL_TRANSITION"
        if isinstance(command, RecoverAttempt):
            return (
                "ILLEGAL_TRANSITION"
                if state.phase
                in {
                    AttemptPhase.SUCCEEDED,
                    AttemptPhase.FAILED,
                    AttemptPhase.CANCELLED,
                    AttemptPhase.TIMED_OUT,
                    AttemptPhase.INTERRUPTED,
                }
                else None
            )
        if isinstance(command, PauseAttempt):
            return None if state.phase is AttemptPhase.RUNNING else "ILLEGAL_TRANSITION"
        if isinstance(command, ResumeAttempt):
            return None if state.phase is AttemptPhase.PAUSED else "ILLEGAL_TRANSITION"
        if isinstance(command, CancelAttempt):
            return (
                None
                if state.phase
                in {
                    AttemptPhase.RUNNING,
                    AttemptPhase.PAUSE_REQUESTED,
                    AttemptPhase.PAUSED,
                    AttemptPhase.WAITING_EXTERNAL,
                }
                else "ILLEGAL_TRANSITION"
            )
        if isinstance(command, SubmitExternalInput):
            if (
                state.phase is not AttemptPhase.WAITING_EXTERNAL
                or len(state.pending_external) != 1
            ):
                return "ILLEGAL_TRANSITION"
            request = state.pending_external[0]
            allowed_responses = {
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
            if (
                request.request_id != command.request_id
                or command.response_kind not in allowed_responses[request.request_kind]
            ):
                return "ILLEGAL_TRANSITION"
            if (
                command.response_kind is ExternalResponseKind.ABANDON
                and any(
                    action.action_id != request.action_id
                    and (
                        action.status
                        not in {
                            *TERMINAL_ACTION_STATUSES,
                            ActionStatus.ACCEPTED,
                        }
                        or (
                            action.status is ActionStatus.OUTCOME_UNKNOWN
                            and action.reconciled_status is None
                        )
                    )
                    for action in state.actions
                )
            ):
                return "UNSAFE_IN_FLIGHT_ACTIONS"
            return None
        if isinstance(command, ExpireAttempt):
            if state.phase not in {
                AttemptPhase.RUNNING,
                AttemptPhase.WAITING_EXTERNAL,
            }:
                return "ILLEGAL_TRANSITION"
            if command.deadline_at != state.budget.deadline_at:
                return "DEADLINE_MISMATCH"
            reconciliation_action_id = None
            if state.phase is AttemptPhase.WAITING_EXTERNAL:
                request = state.pending_external[0]
                if (
                    request.request_kind
                    is ExternalRequestKind.OUTCOME_RECONCILIATION
                ):
                    reconciliation_action_id = request.action_id
            if any(
                action.status is ActionStatus.STARTED
                or (
                    action.status is ActionStatus.OUTCOME_UNKNOWN
                    and action.action_id != reconciliation_action_id
                    and action.reconciled_status is None
                )
                for action in state.actions
            ):
                return "UNSAFE_IN_FLIGHT_ACTIONS"
            return None
        if isinstance(command, ApplyStrategyDecision):
            if state.phase is not AttemptPhase.RUNNING:
                return "ILLEGAL_TRANSITION"
            if command.strategy.strategy_id != state.strategy_id:
                return "STRATEGY_MISMATCH"
            used_request_ids = {
                event.request.request_id
                for event in loaded.events
                if isinstance(event, ExternalInputRequested)
            }
            reserved_reconciliation_ids = {
                outcome_reconciliation_request_id(
                    state.attempt_id,
                    action.action_id,
                )
                for action in state.actions
            }
            requirement_ids = {
                requirement.request_id
                for requirement in command.external_requirements
            }
            proposal_reconciliation_ids = {
                outcome_reconciliation_request_id(
                    state.attempt_id,
                    proposal.action_id,
                )
                for proposal in command.proposals
            }
            if any(
                request_id in used_request_ids
                or request_id in reserved_reconciliation_ids
                for request_id in requirement_ids
            ) or any(
                request_id in used_request_ids
                or request_id in requirement_ids
                for request_id in proposal_reconciliation_ids
            ):
                return "DUPLICATE_EXTERNAL_REQUEST_ID"
            latest_decision = next(
                (
                    event
                    for event in reversed(loaded.events)
                    if isinstance(event, StrategyDecisionRecorded)
                ),
                None,
            )
            if (
                latest_decision is not None
                and latest_decision.directive is not StrategyDirective.CONTINUE
            ):
                return "ILLEGAL_TRANSITION"
            last_cursor = max(
                (
                    event.trigger_sequence_no
                    for event in loaded.events
                    if isinstance(event, StrategyDecisionRecorded)
                ),
                default=0,
            )
            trigger = next(
                (
                    event
                    for event in effective_strategy_triggers(loaded.events)
                    if event.sequence_no > last_cursor
                ),
                None,
            )
            if (
                trigger is None
                or command.trigger_sequence_no != trigger.sequence_no
            ):
                return "INVALID_STRATEGY_TRIGGER"
            action_ids = [proposal.action_id for proposal in command.proposals]
            existing_action_ids = {action.action_id for action in state.actions}
            if (
                len(action_ids) != len(set(action_ids))
                or any(action_id in existing_action_ids for action_id in action_ids)
            ):
                return "DUPLICATE_ACTION_ID"
            invocation_ids = [
                proposal.invocation_id
                for proposal in command.proposals
                if proposal.invocation_id is not None
            ]
            existing_invocation_ids = {
                invocation.invocation_id for invocation in state.invocations
            } | {
                action.invocation_id
                for action in state.actions
                if action.invocation_id is not None
            }
            if (
                len(invocation_ids) != len(set(invocation_ids))
                or any(
                    invocation_id in existing_invocation_ids
                    for invocation_id in invocation_ids
                )
            ):
                return "DUPLICATE_INVOCATION_ID"
            if len(command.proposals) > 1:
                batch_ids = {proposal.batch_id for proposal in command.proposals}
                if None in batch_ids or len(batch_ids) != 1:
                    return "INVALID_ACTION_BATCH"
            return None
        if isinstance(command, ReportActionStarted):
            action = AttemptEngine._state_action(state, command.action_id)
            if (
                state.phase is not AttemptPhase.RUNNING
                or action is None
                or action.status is not ActionStatus.ACCEPTED
            ):
                return "ILLEGAL_TRANSITION"
            return None
        if isinstance(command, ReportActionOutcome):
            action = AttemptEngine._state_action(state, command.action_id)
            waiting_external_known_outcome = (
                state.phase is AttemptPhase.WAITING_EXTERNAL
                and len(state.pending_external) == 1
                and state.pending_external[0].action_id != command.action_id
                and command.outcome.status is not ActionStatus.OUTCOME_UNKNOWN
            )
            if (
                (
                    state.phase
                    not in {
                        AttemptPhase.RUNNING,
                        AttemptPhase.PAUSE_REQUESTED,
                        AttemptPhase.CANCEL_REQUESTED,
                    }
                    and not waiting_external_known_outcome
                )
                or action is None
                or action.status is not ActionStatus.STARTED
            ):
                return "ILLEGAL_TRANSITION"
            return None
        if isinstance(command, FinishAttempt):
            if state.phase is not AttemptPhase.RUNNING:
                return "ILLEGAL_TRANSITION"
            decision = next(
                (
                    event
                    for event in reversed(loaded.events)
                    if isinstance(event, StrategyDecisionRecorded)
                ),
                None,
            )
            if (
                decision is None
                or decision.directive is StrategyDirective.CONTINUE
            ):
                return "STRATEGY_COMPLETION_REQUIRED"
            if (
                decision.result_ref != command.result_ref
                or decision.error != command.error
            ):
                return "STRATEGY_COMPLETION_MISMATCH"
            if any(
                action.status is ActionStatus.OUTCOME_UNKNOWN
                and action.reconciled_status is None
                for action in state.actions
            ):
                return "UNRESOLVED_ACTION_OUTCOME"
            if any(
                action.status not in TERMINAL_ACTION_STATUSES
                for action in state.actions
            ):
                return "NONTERMINAL_ACTIONS"
            return None
        return "UNSUPPORTED_COMMAND"

    def _build_transition(
        self,
        command: CommandBase,
        loaded: LoadedAttempt | None,
        *,
        wall_time_utc: datetime,
    ) -> tuple[
        tuple[DomainEvent, ...],
        tuple[NormalizedAction, ...],
        tuple[str, ...],
    ]:
        if isinstance(command, CreateAttempt):
            builder = _EventBuilder(
                attempt_id=command.attempt_id,
                trial_id=command.trial_id,
                command_id=command.command_id,
                initial_revision=0,
                causal_parent_id=None,
                wall_time_utc=wall_time_utc,
                id_factory=self._id_factory,
            )
            event = builder.add(
                AttemptPlanned,
                event_type="ATTEMPT_PLANNED",
                state_schema_version=command.state_schema_version,
                experiment_id=command.experiment_id,
                strategy_id=command.strategy_id,
                manifest_ref=command.manifest_ref,
                strategy=command.strategy,
                budget=command.budget,
            )
            return (event,), (), ()

        assert loaded is not None
        builder = _EventBuilder(
            attempt_id=command.attempt_id,
            trial_id=loaded.state.trial_id,
            command_id=command.command_id,
            initial_revision=loaded.state.revision,
            causal_parent_id=loaded.latest_event_id,
            wall_time_utc=wall_time_utc,
            id_factory=self._id_factory,
        )
        if isinstance(command, StartAttempt):
            return (
                builder.add(AttemptStarted, event_type="ATTEMPT_STARTED"),
            ), (), ()
        if isinstance(command, RecoverAttempt):
            return (
                builder.add(
                    AttemptRecoveryRequested,
                    event_type="ATTEMPT_RECOVERY_REQUESTED",
                ),
            ), (), ()
        if isinstance(command, PauseAttempt):
            events = [
                builder.add(PauseRequested, event_type="PAUSE_REQUESTED")
            ]
            if not any(
                action.status is ActionStatus.STARTED
                for action in loaded.state.actions
            ):
                events.append(
                    builder.add(AttemptPaused, event_type="ATTEMPT_PAUSED")
                )
            return tuple(events), (), ()
        if isinstance(command, ResumeAttempt):
            return (
                builder.add(AttemptResumed, event_type="ATTEMPT_RESUMED"),
            ), (), ()
        if isinstance(command, CancelAttempt):
            return self._build_cancel(loaded.state, builder)
        if isinstance(command, ExpireAttempt):
            return self._build_expire(loaded.state, builder)
        if isinstance(command, ApplyStrategyDecision):
            return self._build_strategy_decision(command, loaded.state, builder)
        if isinstance(command, ReportActionStarted):
            return self._build_action_started(command, loaded.state, builder)
        if isinstance(command, ReportActionOutcome):
            return self._build_action_outcome(
                command,
                loaded.state,
                loaded.events,
                builder,
            )
        if isinstance(command, SubmitExternalInput):
            return self._build_external_response(command, loaded.state, builder)
        if isinstance(command, FinishAttempt):
            if command.result_ref is not None:
                event = builder.add(
                    AttemptSucceeded,
                    event_type="ATTEMPT_SUCCEEDED",
                    result_ref=command.result_ref,
                )
            else:
                assert command.error is not None
                event = builder.add(
                    AttemptFailed,
                    event_type="ATTEMPT_FAILED",
                    error=command.error,
                )
            return (event,), (), ()
        raise AssertionError("unsupported Command reached transition construction")

    def _build_strategy_decision(
        self,
        command: ApplyStrategyDecision,
        state: AttemptState,
        builder: _EventBuilder,
    ) -> tuple[
        tuple[DomainEvent, ...],
        tuple[NormalizedAction, ...],
        tuple[str, ...],
    ]:
        if command.external_requirements:
            return self._build_external_strategy_decision(command, builder)
        topology_results = tuple(
            TopologyGuard.evaluate(
                action=proposal,
                allowed_edges=self._allowed_edges,
            )
            for proposal in command.proposals
        )
        batch_topology_rejection = (
            next(
                (
                    rejection
                    for rejection in topology_results
                    if rejection is not None
                ),
                None,
            )
            if len(command.proposals) > 1
            else None
        )
        topology_allowed = tuple(
            proposal
            for proposal, rejection in zip(
                command.proposals,
                topology_results,
                strict=True,
            )
            if rejection is None and batch_topology_rejection is None
        )
        budget_result: ReservationPlan | GuardRejection | None = None
        if topology_allowed:
            budget_result = BudgetGuard.evaluate_batch(
                state,
                topology_allowed,
                now_utc=builder.wall_time_utc,
            )

        decision = builder.add(
            StrategyDecisionRecorded,
            event_type="STRATEGY_DECISION_RECORDED",
            trigger_sequence_no=command.trigger_sequence_no,
            strategy=command.strategy,
            proposals=command.proposals,
            external_requirements=command.external_requirements,
            directive=command.directive,
            result_ref=command.result_ref,
            error=command.error,
        )
        events: list[DomainEvent] = [decision]
        accepted_proposals: list[ActionProposal] = []
        for proposal, topology_rejection in zip(
            command.proposals,
            topology_results,
            strict=True,
        ):
            parent_id = builder.causal_parent_id
            normalized_proposal = type(proposal).model_validate(
                {
                    **proposal.model_dump(mode="python"),
                    "causal_parent_id": str(parent_id),
                }
            )
            events.append(
                builder.add(
                    ActionProposed,
                    event_type="ACTION_PROPOSED",
                    proposal=normalized_proposal,
                )
            )
            rejection = batch_topology_rejection or topology_rejection
            if rejection is None and isinstance(budget_result, GuardRejection):
                rejection = budget_result
            if rejection is not None:
                events.append(
                    builder.add(
                        ActionRejected,
                        event_type="ACTION_REJECTED",
                        action_id=normalized_proposal.action_id,
                        error=self._guard_error(rejection),
                    )
                )
                continue
            if normalized_proposal.action_type is ActionType.INVOKE_AGENT:
                events.append(
                    builder.add(
                        InvocationRequested,
                        event_type="INVOCATION_REQUESTED",
                        action_id=normalized_proposal.action_id,
                        invocation_id=normalized_proposal.invocation_id,
                        agent_id=normalized_proposal.target_ids[0],
                        conversation_id=str(self._id_factory.new_uuid()),
                        parent_invocation_id=None,
                        latest_context_ref=normalized_proposal.payload_ref,
                    )
                )
            accepted_proposals.append(normalized_proposal)

        if not accepted_proposals:
            return tuple(events), (), ()
        assert isinstance(budget_result, ReservationPlan)
        actions = tuple(
            NormalizedAction(
                **proposal.model_dump(mode="python"),
                reservation_id=str(self._id_factory.new_uuid()),
                idempotency_key=str(self._id_factory.new_uuid()),
            )
            for proposal in accepted_proposals
        )
        events.append(
            builder.add(
                BudgetReserved,
                event_type="BUDGET_RESERVED",
                budget=budget_result.next_budget,
                reservations=tuple(
                    self._reservation_entry(action) for action in actions
                ),
            )
        )
        for action in actions:
            events.append(
                builder.add(
                    ActionAccepted,
                    event_type="ACTION_ACCEPTED",
                    action=action,
                )
            )
        return tuple(events), actions, ()

    @staticmethod
    def _proposal_from_state(action: ActionState) -> ActionProposal:
        action_type = (
            ActionType.INVOKE_AGENT
            if action.action_type in {ActionType.INVOKE_AGENT.value, "invoke_agent"}
            else ActionType.SEND_MESSAGE
        )
        assert action.causal_parent_id is not None
        return ActionProposal(
            action_id=action.action_id,
            action_type=action_type,
            actor=action.actor_id,
            target_ids=action.target_ids,
            invocation_id=action.invocation_id,
            causal_parent_id=action.causal_parent_id,
            payload_ref=action.payload_ref,
            recovery_policy=action.recovery_policy,
            requested_timeout=action.requested_timeout,
            batch_id=action.batch_id,
            retry_of_action_id=action.retry_of_action_id,
            call_depth=action.call_depth,
            resource_requests=action.resource_requests,
        )

    def _build_external_strategy_decision(
        self,
        command: ApplyStrategyDecision,
        builder: _EventBuilder,
    ) -> tuple[
        tuple[DomainEvent, ...],
        tuple[NormalizedAction, ...],
        tuple[str, ...],
    ]:
        decision = builder.add(
            StrategyDecisionRecorded,
            event_type="STRATEGY_DECISION_RECORDED",
            trigger_sequence_no=command.trigger_sequence_no,
            strategy=command.strategy,
            proposals=command.proposals,
            external_requirements=command.external_requirements,
            directive=command.directive,
            result_ref=command.result_ref,
            error=command.error,
        )
        events: list[DomainEvent] = [decision]
        requirement = command.external_requirements[0]
        if requirement.request_kind is ExternalRequestKind.ACTION_APPROVAL:
            proposal = command.proposals[0]
            normalized_proposal = type(proposal).model_validate(
                {
                    **proposal.model_dump(mode="python"),
                    "causal_parent_id": str(builder.causal_parent_id),
                }
            )
            events.append(
                builder.add(
                    ActionProposed,
                    event_type="ACTION_PROPOSED",
                    proposal=normalized_proposal,
                )
            )
        events.append(
            builder.add(
                ExternalInputRequested,
                event_type="EXTERNAL_INPUT_REQUESTED",
                request=ExternalRequest(
                    request_id=requirement.request_id,
                    request_kind=requirement.request_kind,
                    action_id=requirement.action_id,
                    payload_ref=requirement.payload_ref,
                ),
            )
        )
        return tuple(events), (), ()

    def _build_external_response(
        self,
        command: SubmitExternalInput,
        state: AttemptState,
        builder: _EventBuilder,
    ) -> tuple[
        tuple[DomainEvent, ...],
        tuple[NormalizedAction, ...],
        tuple[str, ...],
    ]:
        request = state.pending_external[0]
        events: list[DomainEvent] = [
            builder.add(
                ExternalInputReceived,
                event_type="EXTERNAL_INPUT_RECEIVED",
                request_id=request.request_id,
                request_kind=request.request_kind,
                action_id=request.action_id,
                response_kind=command.response_kind,
                response_ref=command.response_ref,
                error=command.error,
            )
        ]

        if command.response_kind is ExternalResponseKind.REJECT:
            events.append(
                builder.add(
                    ExternalInputRejected,
                    event_type="EXTERNAL_INPUT_REJECTED",
                    request_id=request.request_id,
                    request_kind=request.request_kind,
                    action_id=request.action_id,
                )
            )
            assert request.action_id is not None
            events.append(
                builder.add(
                    ActionRejected,
                    event_type="ACTION_REJECTED",
                    action_id=request.action_id,
                    error=ErrorSummary(
                        code="EXTERNAL_INPUT_REJECTED",
                        retryable=False,
                        safe_message="The external approval request was rejected.",
                    ),
                )
            )
            return tuple(events), (), ()

        if command.response_kind is ExternalResponseKind.ABANDON:
            events.append(
                builder.add(
                    ExternalInputRejected,
                    event_type="EXTERNAL_INPUT_REJECTED",
                    request_id=request.request_id,
                    request_kind=request.request_kind,
                    action_id=request.action_id,
                )
            )
            assert request.action_id is not None
            action_state = self._required_action(state, request.action_id)
            settled_budget = self._settled_budget(state.budget, action_state)
            events.append(
                builder.add(
                    BudgetUncertainSettled,
                    event_type="BUDGET_UNCERTAIN_SETTLED",
                    budget=settled_budget,
                    reservation=self._reservation_entry(action_state),
                )
            )
            completed_action_ids = self._append_accepted_cancellations(
                state,
                builder,
                events,
                error=ErrorSummary(
                    code="ACTION_CANCELLED_AFTER_RECONCILIATION_ABANDONED",
                    retryable=False,
                    safe_message=(
                        "The Action was cancelled before dispatch after "
                        "reconciliation was abandoned."
                    ),
                ),
                initial_budget=settled_budget,
            )
            events.append(
                builder.add(
                    AttemptInterrupted,
                    event_type="ATTEMPT_INTERRUPTED",
                    error=ErrorSummary(
                        code="OUTCOME_RECONCILIATION_ABANDONED",
                        retryable=False,
                        safe_message=(
                            "The Attempt was interrupted after outcome "
                            "reconciliation was abandoned."
                        ),
                    ),
                )
            )
            return tuple(events), (), completed_action_ids

        events.append(
            builder.add(
                ExternalInputApproved,
                event_type="EXTERNAL_INPUT_APPROVED",
                request_id=request.request_id,
                request_kind=request.request_kind,
                action_id=request.action_id,
            )
        )
        if request.request_kind is ExternalRequestKind.ADDITIONAL_INPUT:
            return tuple(events), (), ()

        if request.request_kind is ExternalRequestKind.OUTCOME_RECONCILIATION:
            assert request.action_id is not None
            action_state = self._required_action(state, request.action_id)
            if command.response_kind is ExternalResponseKind.CONFIRM_SUCCEEDED:
                assert command.response_ref is not None
                outcome = ActionSucceededOutcome(
                    status=ActionStatus.SUCCEEDED,
                    action_id=action_state.action_id,
                    result_ref=command.response_ref,
                )
            else:
                assert command.response_kind is ExternalResponseKind.CONFIRM_FAILED
                assert command.error is not None
                outcome = ActionFailedOutcome(
                    status=ActionStatus.FAILED,
                    action_id=action_state.action_id,
                    error=command.error,
                )
            if action_state.action_type in {
                ActionType.INVOKE_AGENT.value,
                "invoke_agent",
            }:
                assert action_state.invocation_id is not None
                if isinstance(outcome, ActionSucceededOutcome):
                    events.append(
                        builder.add(
                            InvocationCompleted,
                            event_type="INVOCATION_COMPLETED",
                            action_id=action_state.action_id,
                            invocation_id=action_state.invocation_id,
                            latest_context_ref=outcome.result_ref,
                        )
                    )
                else:
                    events.append(
                        builder.add(
                            InvocationFailed,
                            event_type="INVOCATION_FAILED",
                            action_id=action_state.action_id,
                            invocation_id=action_state.invocation_id,
                            error=outcome.error,
                        )
                    )
            events.append(
                builder.add(
                    ActionOutcomeReconciled,
                    event_type="ACTION_OUTCOME_RECONCILED",
                    action_id=action_state.action_id,
                    outcome=outcome,
                )
            )
            events.append(
                builder.add(
                    BudgetSettled,
                    event_type="BUDGET_SETTLED",
                    budget=self._settled_budget(state.budget, action_state),
                    reservation=self._reservation_entry(action_state),
                )
            )
            return tuple(events), (), ()

        if request.request_kind is not ExternalRequestKind.ACTION_APPROVAL:
            raise StateTransitionError("illegal_transition")
        assert request.action_id is not None
        action_state = self._required_action(state, request.action_id)
        proposal = self._proposal_from_state(action_state)
        topology_rejection = TopologyGuard.evaluate(
            action=proposal,
            allowed_edges=self._allowed_edges,
        )
        budget_result: ReservationPlan | GuardRejection
        if topology_rejection is None:
            budget_result = BudgetGuard.evaluate_batch(
                state,
                (proposal,),
                now_utc=builder.wall_time_utc,
            )
        else:
            budget_result = topology_rejection
        if isinstance(budget_result, GuardRejection):
            events.append(
                builder.add(
                    ActionRejected,
                    event_type="ACTION_REJECTED",
                    action_id=proposal.action_id,
                    error=self._guard_error(budget_result),
                )
            )
            return tuple(events), (), ()

        normalized = NormalizedAction(
            **proposal.model_dump(mode="python"),
            reservation_id=str(self._id_factory.new_uuid()),
            idempotency_key=str(self._id_factory.new_uuid()),
        )
        if normalized.action_type is ActionType.INVOKE_AGENT:
            events.append(
                builder.add(
                    InvocationRequested,
                    event_type="INVOCATION_REQUESTED",
                    action_id=normalized.action_id,
                    invocation_id=normalized.invocation_id,
                    agent_id=normalized.target_ids[0],
                    conversation_id=str(self._id_factory.new_uuid()),
                    parent_invocation_id=None,
                    latest_context_ref=normalized.payload_ref,
                )
            )
        events.append(
            builder.add(
                BudgetReserved,
                event_type="BUDGET_RESERVED",
                budget=budget_result.next_budget,
                reservations=(self._reservation_entry(normalized),),
            )
        )
        events.append(
            builder.add(
                ActionAccepted,
                event_type="ACTION_ACCEPTED",
                action=normalized,
            )
        )
        return tuple(events), (normalized,), ()

    @staticmethod
    def _build_action_started(
        command: ReportActionStarted,
        state: AttemptState,
        builder: _EventBuilder,
    ) -> tuple[
        tuple[DomainEvent, ...],
        tuple[NormalizedAction, ...],
        tuple[str, ...],
    ]:
        action = AttemptEngine._required_action(state, command.action_id)
        events: list[DomainEvent] = [
            builder.add(
                ActionStarted,
                event_type="ACTION_STARTED",
                action_id=action.action_id,
            )
        ]
        if action.action_type in {ActionType.INVOKE_AGENT.value, "invoke_agent"}:
            assert action.invocation_id is not None
            events.append(
                builder.add(
                    InvocationStarted,
                    event_type="INVOCATION_STARTED",
                    action_id=action.action_id,
                    invocation_id=action.invocation_id,
                )
            )
        return tuple(events), (), ()

    @staticmethod
    def _build_action_outcome(
        command: ReportActionOutcome,
        state: AttemptState,
        committed_events: tuple[DomainEvent, ...],
        builder: _EventBuilder,
    ) -> tuple[
        tuple[DomainEvent, ...],
        tuple[NormalizedAction, ...],
        tuple[str, ...],
    ]:
        action = AttemptEngine._required_action(state, command.action_id)
        outcome = command.outcome
        events: list[DomainEvent] = []
        is_invocation = action.action_type in {
            ActionType.INVOKE_AGENT.value,
            "invoke_agent",
        }
        if is_invocation and outcome.status is not ActionStatus.OUTCOME_UNKNOWN:
            assert action.invocation_id is not None
            if isinstance(outcome, ActionSucceededOutcome):
                events.append(
                    builder.add(
                        InvocationCompleted,
                        event_type="INVOCATION_COMPLETED",
                        action_id=action.action_id,
                        invocation_id=action.invocation_id,
                        latest_context_ref=outcome.result_ref,
                    )
                )
            else:
                events.append(
                    builder.add(
                        InvocationFailed,
                        event_type="INVOCATION_FAILED",
                        action_id=action.action_id,
                        invocation_id=action.invocation_id,
                        error=outcome.error,
                    )
                )
        terminal_class, event_type = {
            ActionStatus.SUCCEEDED: (ActionSucceeded, "ACTION_SUCCEEDED"),
            ActionStatus.FAILED: (ActionFailed, "ACTION_FAILED"),
            ActionStatus.TIMED_OUT: (ActionTimedOut, "ACTION_TIMED_OUT"),
            ActionStatus.CANCELLED: (ActionCancelled, "ACTION_CANCELLED"),
            ActionStatus.OUTCOME_UNKNOWN: (
                ActionOutcomeUnknown,
                "ACTION_OUTCOME_UNKNOWN",
            ),
        }[outcome.status]
        events.append(
            builder.add(
                terminal_class,
                event_type=event_type,
                action_id=action.action_id,
                outcome=outcome,
            )
        )
        if outcome.status is ActionStatus.OUTCOME_UNKNOWN:
            events.append(
                builder.add(
                    ExternalInputRequested,
                    event_type="EXTERNAL_INPUT_REQUESTED",
                    request=ExternalRequest(
                        request_id=outcome_reconciliation_request_id(
                            state.attempt_id,
                            action.action_id,
                        ),
                        request_kind=ExternalRequestKind.OUTCOME_RECONCILIATION,
                        action_id=action.action_id,
                        payload_ref=None,
                    ),
                )
            )
        else:
            events.append(
                builder.add(
                    BudgetSettled,
                    event_type="BUDGET_SETTLED",
                    budget=AttemptEngine._settled_budget(state.budget, action),
                    reservation=AttemptEngine._reservation_entry(action),
                )
            )
        settled_unknown_reservations = {
            (
                event.reservation.action_id,
                event.reservation.reservation_id,
            )
            for event in committed_events
            if isinstance(event, (BudgetSettled, BudgetUncertainSettled))
        }
        observed_actions = tuple(
            (
                candidate,
                outcome.status
                if candidate.action_id == action.action_id
                else candidate.status,
            )
            for candidate in state.actions
        )
        unresolved_unknown = any(
            status is ActionStatus.OUTCOME_UNKNOWN
            and candidate.reconciled_status is None
            and (candidate.action_id, candidate.reservation_id)
            not in settled_unknown_reservations
            for candidate, status in observed_actions
        )
        pause_boundary = (
            all(
                status is not ActionStatus.STARTED
                for _, status in observed_actions
            )
            and not unresolved_unknown
        )
        cancel_boundary = (
            all(
                status in TERMINAL_ACTION_STATUSES
                for _, status in observed_actions
            )
            and not unresolved_unknown
        )
        if pause_boundary and state.phase is AttemptPhase.PAUSE_REQUESTED:
            events.append(
                builder.add(AttemptPaused, event_type="ATTEMPT_PAUSED")
            )
        elif cancel_boundary and state.phase is AttemptPhase.CANCEL_REQUESTED:
            events.append(
                builder.add(AttemptCancelled, event_type="ATTEMPT_CANCELLED")
            )
        return tuple(events), (), (action.action_id,)

    @staticmethod
    def _build_cancel(
        state: AttemptState,
        builder: _EventBuilder,
    ) -> tuple[
        tuple[DomainEvent, ...],
        tuple[NormalizedAction, ...],
        tuple[str, ...],
    ]:
        events: list[DomainEvent] = []
        next_budget = AttemptEngine._append_external_expiry(
            state,
            builder,
            events,
        )
        events.append(builder.add(CancelRequested, event_type="CANCEL_REQUESTED"))
        completed_action_ids: list[str] = []
        cancellation_error = ErrorSummary(
            code="ACTION_CANCELLED_BEFORE_START",
            retryable=False,
            safe_message="The Action was cancelled before it started.",
        )
        completed_action_ids.extend(
            AttemptEngine._append_accepted_cancellations(
                state,
                builder,
                events,
                error=cancellation_error,
                initial_budget=next_budget,
            )
        )
        for action in state.actions:
            if action.status is ActionStatus.STARTED:
                events.append(
                    builder.add(
                        ActionCancellationRequested,
                        event_type="ACTION_CANCELLATION_REQUESTED",
                        action_id=action.action_id,
                    )
                )
        if not any(action.status is ActionStatus.STARTED for action in state.actions):
            events.append(
                builder.add(AttemptCancelled, event_type="ATTEMPT_CANCELLED")
            )
        return tuple(events), (), tuple(completed_action_ids)

    @staticmethod
    def _build_expire(
        state: AttemptState,
        builder: _EventBuilder,
    ) -> tuple[
        tuple[DomainEvent, ...],
        tuple[NormalizedAction, ...],
        tuple[str, ...],
    ]:
        events: list[DomainEvent] = []
        next_budget = AttemptEngine._append_external_expiry(
            state,
            builder,
            events,
        )
        completed_action_ids: list[str] = []
        cancellation_error = ErrorSummary(
            code="ACTION_CANCELLED_BY_DEADLINE",
            retryable=False,
            safe_message="The Action was cancelled before dispatch at the deadline.",
        )
        completed_action_ids.extend(
            AttemptEngine._append_accepted_cancellations(
                state,
                builder,
                events,
                error=cancellation_error,
                initial_budget=next_budget,
            )
        )
        events.append(
            builder.add(
                AttemptTimedOut,
                event_type="ATTEMPT_TIMED_OUT",
                error=ErrorSummary(
                    code="ATTEMPT_DEADLINE_EXCEEDED",
                    retryable=False,
                    safe_message="The Attempt deadline was reached.",
                ),
            )
        )
        return tuple(events), (), tuple(completed_action_ids)

    @staticmethod
    def _append_external_expiry(
        state: AttemptState,
        builder: _EventBuilder,
        events: list[DomainEvent],
    ) -> BudgetState:
        if state.phase is not AttemptPhase.WAITING_EXTERNAL:
            return state.budget
        request = state.pending_external[0]
        events.append(
            builder.add(
                ExternalInputExpired,
                event_type="EXTERNAL_INPUT_EXPIRED",
                request_id=request.request_id,
                request_kind=request.request_kind,
                action_id=request.action_id,
            )
        )
        if request.request_kind is ExternalRequestKind.ACTION_APPROVAL:
            assert request.action_id is not None
            events.append(
                builder.add(
                    ActionRejected,
                    event_type="ACTION_REJECTED",
                    action_id=request.action_id,
                    error=ErrorSummary(
                        code="EXTERNAL_INPUT_EXPIRED",
                        retryable=False,
                        safe_message="The external approval request expired.",
                    ),
                )
            )
            return state.budget
        if (
            request.request_kind
            is ExternalRequestKind.OUTCOME_RECONCILIATION
        ):
            assert request.action_id is not None
            action = AttemptEngine._required_action(state, request.action_id)
            budget = AttemptEngine._settled_budget(state.budget, action)
            events.append(
                builder.add(
                    BudgetUncertainSettled,
                    event_type="BUDGET_UNCERTAIN_SETTLED",
                    budget=budget,
                    reservation=AttemptEngine._reservation_entry(action),
                )
            )
            return budget
        return state.budget

    @staticmethod
    def _append_accepted_cancellations(
        state: AttemptState,
        builder: _EventBuilder,
        events: list[DomainEvent],
        *,
        error: ErrorSummary,
        initial_budget: BudgetState | None = None,
    ) -> tuple[str, ...]:
        completed_action_ids: list[str] = []
        next_budget = state.budget if initial_budget is None else initial_budget
        for action in state.actions:
            if action.status is ActionStatus.ACCEPTED:
                events.append(
                    builder.add(
                        ActionCancelled,
                        event_type="ACTION_CANCELLED",
                        action_id=action.action_id,
                        outcome=ActionCancelledOutcome(
                            status=ActionStatus.CANCELLED,
                            action_id=action.action_id,
                            error=error,
                        ),
                    )
                )
                next_budget = AttemptEngine._released_budget(next_budget, action)
                events.append(
                    builder.add(
                        BudgetReleased,
                        event_type="BUDGET_RELEASED",
                        budget=next_budget,
                        reservation=AttemptEngine._reservation_entry(action),
                    )
                )
                completed_action_ids.append(action.action_id)
        return tuple(completed_action_ids)

    @staticmethod
    def _should_checkpoint(
        command: CommandBase,
        state: AttemptState,
    ) -> bool:
        return isinstance(command, SubmitExternalInput) or state.phase in {
            AttemptPhase.PAUSED,
            AttemptPhase.WAITING_EXTERNAL,
            AttemptPhase.CANCELLED,
            AttemptPhase.TIMED_OUT,
            AttemptPhase.INTERRUPTED,
            AttemptPhase.SUCCEEDED,
            AttemptPhase.FAILED,
        }

    @staticmethod
    def _reservation_entry(
        action: ActionState | NormalizedAction,
    ) -> BudgetReservationEntry:
        assert action.reservation_id is not None
        return BudgetReservationEntry(
            action_id=action.action_id,
            reservation_id=action.reservation_id,
            resource_requests=action.resource_requests,
        )

    @staticmethod
    def _settled_budget(budget: BudgetState, action: ActionState) -> BudgetState:
        requested = {
            request.resource.value: request.amount
            for request in action.resource_requests
        }
        resources: list[ResourceBudget] = []
        for counter in budget.resources:
            amount = requested.get(counter.resource, 0)
            if amount > counter.reserved:
                raise StateTransitionError("invalid_budget_transition")
            resources.append(
                ResourceBudget(
                    resource=counter.resource,
                    limit=counter.limit,
                    reserved=counter.reserved - amount,
                    consumed=counter.consumed + amount,
                )
            )
        return BudgetState(
            resources=tuple(resources),
            deadline_at=budget.deadline_at,
            max_call_depth=budget.max_call_depth,
            max_concurrent_actions=budget.max_concurrent_actions,
        )

    @staticmethod
    def _released_budget(budget: BudgetState, action: ActionState) -> BudgetState:
        requested = {
            request.resource.value: request.amount
            for request in action.resource_requests
        }
        resources: list[ResourceBudget] = []
        for counter in budget.resources:
            amount = requested.get(counter.resource, 0)
            if amount > counter.reserved:
                raise StateTransitionError("invalid_budget_transition")
            resources.append(
                ResourceBudget(
                    resource=counter.resource,
                    limit=counter.limit,
                    reserved=counter.reserved - amount,
                    consumed=counter.consumed,
                )
            )
        return BudgetState(
            resources=tuple(resources),
            deadline_at=budget.deadline_at,
            max_call_depth=budget.max_call_depth,
            max_concurrent_actions=budget.max_concurrent_actions,
        )

    @staticmethod
    def _guard_error(rejection: GuardRejection) -> ErrorSummary:
        return ErrorSummary(
            code=rejection.code,
            retryable=False,
            safe_message="The Action was rejected by an orchestration guard.",
        )

    @staticmethod
    def _state_action(state: AttemptState, action_id: str) -> ActionState | None:
        return next(
            (action for action in state.actions if action.action_id == action_id),
            None,
        )

    @staticmethod
    def _required_action(state: AttemptState, action_id: str) -> ActionState:
        action = AttemptEngine._state_action(state, action_id)
        if action is None:
            raise StateTransitionError("illegal_transition")
        return action

    @staticmethod
    def _artifact_references(command: CommandBase) -> tuple[ArtifactRef, ...]:
        refs: list[ArtifactRef] = []

        def add(ref: ArtifactRef | None) -> None:
            if ref is not None and ref not in refs:
                refs.append(ref)

        if isinstance(command, CreateAttempt):
            add(command.manifest_ref)
            add(command.strategy.artifact_ref)
        elif isinstance(command, ApplyStrategyDecision):
            add(command.strategy.artifact_ref)
            add(command.result_ref)
            if command.error is not None:
                add(command.error.detail_ref)
            for proposal in command.proposals:
                add(proposal.payload_ref)
            for requirement in command.external_requirements:
                add(requirement.payload_ref)
        elif isinstance(command, ReportActionOutcome):
            outcome = command.outcome
            if isinstance(outcome, ActionSucceededOutcome):
                add(outcome.result_ref)
            else:
                add(outcome.error.detail_ref)
        elif isinstance(command, FinishAttempt):
            add(command.result_ref)
            if command.error is not None:
                add(command.error.detail_ref)
        elif isinstance(command, SubmitExternalInput):
            add(command.response_ref)
            if command.error is not None:
                add(command.error.detail_ref)
        return tuple(refs)

    @staticmethod
    def _rejected(
        command: CommandBase,
        loaded: LoadedAttempt | None,
        code: str,
    ) -> CommandResult:
        state = None if loaded is None else loaded.state
        return CommandResult(
            command_id=command.command_id,
            attempt_id=command.attempt_id,
            accepted=False,
            revision=0 if state is None else state.revision,
            phase=None if state is None else state.phase,
            error=ErrorSummary(
                code=code,
                retryable=code == "REVISION_CONFLICT",
                safe_message=_REJECTION_MESSAGES[code],
            ),
        )


__all__ = ["ArtifactVerificationError", "AttemptEngine"]
