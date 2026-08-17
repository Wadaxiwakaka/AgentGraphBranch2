from __future__ import annotations

from .actions import ActionProposal
from .budget import GuardRejection


class TopologyGuard:
    @staticmethod
    def evaluate(
        *,
        action: ActionProposal,
        allowed_edges: frozenset[tuple[str, str]],
    ) -> GuardRejection | None:
        if not isinstance(action, ActionProposal):
            raise ValueError("action must be an ActionProposal")
        if not isinstance(allowed_edges, frozenset):
            raise ValueError("allowed_edges must be a frozenset")
        if any(
            not isinstance(edge, tuple)
            or len(edge) != 2
            or not all(isinstance(node, str) for node in edge)
            for edge in allowed_edges
        ):
            raise ValueError("allowed_edges must contain actor-target string pairs")

        for target_id in action.target_ids:
            if (action.actor, target_id) not in allowed_edges:
                return GuardRejection(
                    code="TOPOLOGY_EDGE_FORBIDDEN",
                    action_id=action.action_id,
                    target_id=target_id,
                )
        return None


__all__ = ["TopologyGuard"]
