from __future__ import annotations

import json
from collections.abc import Mapping
from hashlib import sha256

from ..actions import (
    ActionFailedOutcome,
    ActionProposal,
    ActionSucceededOutcome,
    ActionType,
    validate_stable_id,
)
from ..commands import strategy_action_id, strategy_invocation_id
from ..contract import StrategyDecision, StrategyDirective
from ..events import (
    ActionCancelled,
    ActionFailed,
    ActionOutcomeReconciled,
    ActionOutcomeUnknown,
    ActionRejected,
    ActionSucceeded,
    ActionTimedOut,
    AttemptStarted,
    DomainEvent,
    is_strategy_trigger,
)
from ..state import (
    ArtifactRef,
    ErrorSummary,
    RecoveryPolicy,
    ResourceRequest,
    StrategyStateEnvelope,
    StrategyView,
)


_SCHEMA_VERSION = 1
_STATE_KEYS = frozenset(
    {
        "current_action_id",
        "current_invocation_id",
        "next_index",
        "stage",
        "version",
    }
)
_WAITING_STAGE = "WAITING_FOR_INVOCATION"
_DONE_STAGE = "DONE"


def _state_envelope(
    strategy_id: str,
    *,
    stage: str,
    next_index: int,
    current_action_id: str,
    current_invocation_id: str,
) -> StrategyStateEnvelope:
    value = {
        "current_action_id": current_action_id,
        "current_invocation_id": current_invocation_id,
        "next_index": next_index,
        "stage": stage,
        "version": _SCHEMA_VERSION,
    }
    payload = json.dumps(value, sort_keys=True, separators=(",", ":")).encode(
        "utf-8"
    )
    return StrategyStateEnvelope(
        strategy_id=strategy_id,
        strategy_schema_version=_SCHEMA_VERSION,
        value=value,
        content_hash=sha256(payload).hexdigest(),
        byte_size=len(payload),
    )


class StaticWorkflowStrategy:
    def __init__(
        self,
        *,
        strategy_id: str,
        agent_ids: tuple[str, ...],
        payload_ref: ArtifactRef | None,
        requested_timeout: int = 60,
        resource_requests: tuple[ResourceRequest, ...] = (),
    ) -> None:
        validated_strategy_id = validate_stable_id(
            strategy_id, field_name="strategy_id"
        )
        copied_agent_ids = tuple(agent_ids)
        if not copied_agent_ids:
            raise ValueError("agent_ids must contain at least one Agent id")
        for agent_id in copied_agent_ids:
            validate_stable_id(agent_id, field_name="agent_ids")
        if type(requested_timeout) is not int or requested_timeout <= 0:
            raise ValueError("requested_timeout must be a positive integer")
        self._strategy_id = validated_strategy_id
        self._agent_ids = copied_agent_ids
        self._payload_ref = payload_ref
        self._requested_timeout = requested_timeout
        self._resource_requests = tuple(resource_requests)

    def initialize(self, view: StrategyView) -> StrategyDecision:
        self._validate_view(view)
        latest = view.latest_committed_event
        if latest.get("event_type") != "ATTEMPT_STARTED":
            raise ValueError("StaticWorkflowStrategy must initialize from AttemptStarted")
        trigger_sequence_no = self._trigger_sequence(latest)
        proposal = self._proposal(view, ordinal=0, causal_event=latest)
        assert proposal.invocation_id is not None
        return StrategyDecision(
            trigger_sequence_no=trigger_sequence_no,
            strategy=_state_envelope(
                self._strategy_id,
                stage=_WAITING_STAGE,
                next_index=1,
                current_action_id=proposal.action_id,
                current_invocation_id=proposal.invocation_id,
            ),
            proposals=(proposal,),
            directive=StrategyDirective.CONTINUE,
        )

    def on_event(
        self,
        state: StrategyStateEnvelope,
        event: DomainEvent,
        view: StrategyView,
    ) -> StrategyDecision:
        self._validate_view(view)
        private_state = self._validate_state(state, view)
        if private_state["stage"] != _WAITING_STAGE:
            raise ValueError("StaticWorkflowStrategy is not waiting for an invocation")
        if not is_strategy_trigger(event) or isinstance(event, AttemptStarted):
            raise ValueError("event is not an eligible StaticWorkflowStrategy trigger")
        if event.attempt_id != view.attempt_id:
            raise ValueError("event does not belong to the Strategy view")

        trigger_sequence_no = event.sequence_no
        current_action_id = private_state["current_action_id"]
        if getattr(event, "action_id", None) != current_action_id:
            return StrategyDecision(
                trigger_sequence_no=trigger_sequence_no,
                strategy=state,
                proposals=(),
                directive=StrategyDirective.CONTINUE,
            )

        current_invocation_id = private_state["current_invocation_id"]
        next_index = private_state["next_index"]
        if isinstance(event, ActionSucceeded) or (
            isinstance(event, ActionOutcomeReconciled)
            and isinstance(event.outcome, ActionSucceededOutcome)
        ):
            if next_index < len(self._agent_ids):
                proposal = self._proposal(
                    view,
                    ordinal=next_index,
                    causal_event=event.model_dump(mode="json"),
                )
                assert proposal.invocation_id is not None
                return StrategyDecision(
                    trigger_sequence_no=trigger_sequence_no,
                    strategy=_state_envelope(
                        self._strategy_id,
                        stage=_WAITING_STAGE,
                        next_index=next_index + 1,
                        current_action_id=proposal.action_id,
                        current_invocation_id=proposal.invocation_id,
                    ),
                    proposals=(proposal,),
                    directive=StrategyDirective.CONTINUE,
                )
            return StrategyDecision(
                trigger_sequence_no=trigger_sequence_no,
                strategy=_state_envelope(
                    self._strategy_id,
                    stage=_DONE_STAGE,
                    next_index=next_index,
                    current_action_id=current_action_id,
                    current_invocation_id=current_invocation_id,
                ),
                proposals=(),
                directive=StrategyDirective.SUCCEED,
                result_ref=event.outcome.result_ref,
            )

        if isinstance(
            event,
            (
                ActionRejected,
                ActionFailed,
                ActionTimedOut,
                ActionCancelled,
                ActionOutcomeUnknown,
            ),
        ) or (
            isinstance(event, ActionOutcomeReconciled)
            and isinstance(event.outcome, ActionFailedOutcome)
        ):
            return StrategyDecision(
                trigger_sequence_no=trigger_sequence_no,
                strategy=_state_envelope(
                    self._strategy_id,
                    stage=_DONE_STAGE,
                    next_index=next_index,
                    current_action_id=current_action_id,
                    current_invocation_id=current_invocation_id,
                ),
                proposals=(),
                directive=StrategyDirective.FAIL,
                error=ErrorSummary(
                    code="STATIC_WORKFLOW_ACTION_FAILED",
                    retryable=False,
                    safe_message=(
                        "The current static workflow action did not complete "
                        "successfully."
                    ),
                ),
            )
        raise ValueError("unsupported StaticWorkflowStrategy trigger")

    def _proposal(
        self,
        view: StrategyView,
        *,
        ordinal: int,
        causal_event: Mapping[str, object],
    ) -> ActionProposal:
        return ActionProposal(
            action_id=strategy_action_id(
                view.attempt_id, self._strategy_id, ordinal
            ),
            action_type=ActionType.INVOKE_AGENT,
            actor=self._strategy_id,
            target_ids=(self._agent_ids[ordinal],),
            invocation_id=strategy_invocation_id(
                view.attempt_id, self._strategy_id, ordinal
            ),
            causal_parent_id=str(causal_event["event_id"]),
            payload_ref=self._payload_ref,
            recovery_policy=RecoveryPolicy.REPLAY_SAFE,
            requested_timeout=self._requested_timeout,
            call_depth=0,
            resource_requests=self._resource_requests,
        )

    def _validate_view(self, view: StrategyView) -> None:
        if view.strategy_id != self._strategy_id:
            raise ValueError("Strategy view identity does not match")

    def _validate_state(
        self,
        state: StrategyStateEnvelope,
        view: StrategyView,
    ) -> Mapping[str, object]:
        validated = StrategyStateEnvelope.model_validate_json(
            state.model_dump_json()
        )
        value = validated.value
        if (
            validated.strategy_id != self._strategy_id
            or validated.strategy_schema_version != _SCHEMA_VERSION
            or not isinstance(value, Mapping)
            or set(value) != _STATE_KEYS
            or type(value["version"]) is not int
            or value["version"] != _SCHEMA_VERSION
            or type(value["stage"]) is not str
            or value["stage"] not in {_WAITING_STAGE, _DONE_STAGE}
            or type(value["next_index"]) is not int
            or not 1 <= value["next_index"] <= len(self._agent_ids)
            or type(value["current_action_id"]) is not str
            or type(value["current_invocation_id"]) is not str
        ):
            raise ValueError("invalid StaticWorkflowStrategy state envelope")
        ordinal = value["next_index"] - 1
        if (
            value["current_action_id"]
            != strategy_action_id(view.attempt_id, self._strategy_id, ordinal)
            or value["current_invocation_id"]
            != strategy_invocation_id(view.attempt_id, self._strategy_id, ordinal)
        ):
            raise ValueError("invalid StaticWorkflowStrategy state envelope")
        return value

    @staticmethod
    def _trigger_sequence(event: Mapping[str, object]) -> int:
        sequence_no = event.get("sequence_no")
        if type(sequence_no) is not int or sequence_no < 1:
            raise ValueError("latest committed event has no valid sequence number")
        return sequence_no


__all__ = ["StaticWorkflowStrategy"]
