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
_STAGES = frozenset({"READY", "WAITING_FOR_INVOCATION", "DONE"})


def _state_envelope(strategy_id: str, stage: str) -> StrategyStateEnvelope:
    value = {"stage": stage, "version": _SCHEMA_VERSION}
    payload = json.dumps(
        value,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")
    return StrategyStateEnvelope(
        strategy_id=strategy_id,
        strategy_schema_version=_SCHEMA_VERSION,
        value=value,
        content_hash=sha256(payload).hexdigest(),
        byte_size=len(payload),
    )


class SingleAgentStrategy:
    def __init__(
        self,
        *,
        strategy_id: str,
        agent_id: str,
        payload_ref: ArtifactRef | None,
        requested_timeout: int = 60,
        resource_requests: tuple[ResourceRequest, ...] = (),
    ) -> None:
        validated_strategy_id = validate_stable_id(
            strategy_id, field_name="strategy_id"
        )
        validated_agent_id = validate_stable_id(agent_id, field_name="agent_id")
        if type(requested_timeout) is not int or requested_timeout <= 0:
            raise ValueError("requested_timeout must be a positive integer")
        self._strategy_id = validated_strategy_id
        self._agent_id = validated_agent_id
        self._payload_ref = payload_ref
        self._requested_timeout = requested_timeout
        self._resource_requests = tuple(resource_requests)

    def initialize(self, view: StrategyView) -> StrategyDecision:
        self._validate_view(view)
        latest = view.latest_committed_event
        if latest.get("event_type") != "ATTEMPT_STARTED":
            raise ValueError("SingleAgentStrategy must initialize from AttemptStarted")
        trigger_sequence_no = self._trigger_sequence(latest)
        action_id = strategy_action_id(view.attempt_id, self._strategy_id, 0)
        proposal = ActionProposal(
            action_id=action_id,
            action_type=ActionType.INVOKE_AGENT,
            actor=self._strategy_id,
            target_ids=(self._agent_id,),
            invocation_id=strategy_invocation_id(view.attempt_id, self._strategy_id, 0),
            causal_parent_id=str(latest["event_id"]),
            payload_ref=self._payload_ref,
            recovery_policy=RecoveryPolicy.REPLAY_SAFE,
            requested_timeout=self._requested_timeout,
            call_depth=0,
            resource_requests=self._resource_requests,
        )
        return StrategyDecision(
            trigger_sequence_no=trigger_sequence_no,
            strategy=_state_envelope(self._strategy_id, "WAITING_FOR_INVOCATION"),
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
        stage = self._validate_state(state)
        if stage != "WAITING_FOR_INVOCATION":
            raise ValueError("SingleAgentStrategy is not waiting for an invocation")
        if not is_strategy_trigger(event) or isinstance(event, AttemptStarted):
            raise ValueError("event is not an eligible SingleAgentStrategy trigger")
        if event.attempt_id != view.attempt_id:
            raise ValueError("event does not belong to the Strategy view")
        trigger_sequence_no = event.sequence_no
        expected_action_id = strategy_action_id(view.attempt_id, self._strategy_id, 0)
        if getattr(event, "action_id", None) != expected_action_id:
            return StrategyDecision(
                trigger_sequence_no=trigger_sequence_no,
                strategy=state,
                proposals=(),
                directive=StrategyDirective.CONTINUE,
            )
        done = _state_envelope(self._strategy_id, "DONE")
        if isinstance(event, ActionSucceeded) or (
            isinstance(event, ActionOutcomeReconciled)
            and isinstance(event.outcome, ActionSucceededOutcome)
        ):
            return StrategyDecision(
                trigger_sequence_no=trigger_sequence_no,
                strategy=done,
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
                strategy=done,
                proposals=(),
                directive=StrategyDirective.FAIL,
                error=ErrorSummary(
                    code="SINGLE_AGENT_ACTION_FAILED",
                    retryable=False,
                    safe_message="The single Agent action did not complete successfully.",
                ),
            )
        raise ValueError("unsupported SingleAgentStrategy trigger")

    def _validate_view(self, view: StrategyView) -> None:
        if view.strategy_id != self._strategy_id:
            raise ValueError("Strategy view identity does not match")

    def _validate_state(self, state: StrategyStateEnvelope) -> str:
        validated = StrategyStateEnvelope.model_validate_json(state.model_dump_json())
        if (
            validated.strategy_id != self._strategy_id
            or validated.strategy_schema_version != _SCHEMA_VERSION
            or not isinstance(validated.value, Mapping)
            or set(validated.value) != {"stage", "version"}
            or type(validated.value["version"]) is not int
            or validated.value["version"] != _SCHEMA_VERSION
            or validated.value["stage"] not in _STAGES
        ):
            raise ValueError("invalid SingleAgentStrategy state envelope")
        return validated.value["stage"]

    @staticmethod
    def _trigger_sequence(event: Mapping[str, object]) -> int:
        sequence_no = event.get("sequence_no")
        if type(sequence_no) is not int or sequence_no < 1:
            raise ValueError("latest committed event has no valid sequence number")
        return sequence_no
