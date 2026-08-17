from __future__ import annotations

from collections.abc import Mapping
from types import MappingProxyType

from ..actions import ActionOutcome, NormalizedAction
from ..contract import ExecutionContext


class ScriptedBackendError(RuntimeError):
    def __init__(self, code: str, message: str) -> None:
        self.code = code
        super().__init__(message)


class ScriptedBackend:
    def __init__(self, outcomes: Mapping[str, ActionOutcome]) -> None:
        copied = dict(outcomes)
        if any(action_id != outcome.action_id for action_id, outcome in copied.items()):
            raise ScriptedBackendError(
                "invalid_scripted_outcome",
                "A scripted outcome must match its Action identifier.",
            )
        self._outcomes = MappingProxyType(copied)
        self._memoized: dict[str, ActionOutcome] = {}
        self._memoized_action_ids: dict[str, str] = {}
        self._calls: list[tuple[str, str]] = []

    @property
    def calls(self) -> tuple[tuple[str, str], ...]:
        return tuple(self._calls)

    async def execute(
        self,
        action: NormalizedAction,
        context: ExecutionContext,
    ) -> ActionOutcome:
        if (
            context.action_id != action.action_id
            or context.invocation_id != action.invocation_id
            or context.idempotency_key != action.idempotency_key
        ):
            raise ScriptedBackendError(
                "invalid_execution_context",
                "Execution context does not match the Action.",
            )
        self._calls.append((action.action_id, context.idempotency_key))
        memoized = self._memoized.get(context.idempotency_key)
        if memoized is not None:
            if self._memoized_action_ids[context.idempotency_key] != action.action_id:
                raise ScriptedBackendError(
                    "idempotency_key_reused",
                    "An idempotency key cannot identify different Actions.",
                )
            return memoized
        outcome = self._outcomes.get(action.action_id)
        if outcome is None:
            raise ScriptedBackendError(
                "unknown_scripted_action",
                "No outcome is registered for this Action.",
            )
        self._memoized[context.idempotency_key] = outcome
        self._memoized_action_ids[context.idempotency_key] = action.action_id
        return outcome
