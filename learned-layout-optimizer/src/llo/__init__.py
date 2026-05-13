"""learned-layout-optimizer — online layout tuning via bandit / RL policies."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

__version__ = "0.1.0"

if TYPE_CHECKING:
    from llo.agent.loop import LayoutAgent
    from llo.curves.spacefill import hilbert_index, hilbert_index_nd, z_order_index
    from llo.policy.bandit import (
        Action,
        EpsilonGreedyPolicy,
        HeuristicPolicy,
        ThompsonPolicy,
        UCBPolicy,
        heuristic_action,
    )
    from llo.replay.pages import PAGE_ROWS, apply_layout, expected_pages, reward
    from llo.workload.drift import DriftDetector
    from llo.workload.profile import Query, WorkloadProfile

_LAZY: dict[str, tuple[str, str]] = {
    "z_order_index": ("llo.curves.spacefill", "z_order_index"),
    "hilbert_index": ("llo.curves.spacefill", "hilbert_index"),
    "hilbert_index_nd": ("llo.curves.spacefill", "hilbert_index_nd"),
    "Query": ("llo.workload.profile", "Query"),
    "WorkloadProfile": ("llo.workload.profile", "WorkloadProfile"),
    "DriftDetector": ("llo.workload.drift", "DriftDetector"),
    "Action": ("llo.policy.bandit", "Action"),
    "HeuristicPolicy": ("llo.policy.bandit", "HeuristicPolicy"),
    "heuristic_action": ("llo.policy.bandit", "heuristic_action"),
    "UCBPolicy": ("llo.policy.bandit", "UCBPolicy"),
    "EpsilonGreedyPolicy": ("llo.policy.bandit", "EpsilonGreedyPolicy"),
    "ThompsonPolicy": ("llo.policy.bandit", "ThompsonPolicy"),
    "apply_layout": ("llo.replay.pages", "apply_layout"),
    "expected_pages": ("llo.replay.pages", "expected_pages"),
    "reward": ("llo.replay.pages", "reward"),
    "PAGE_ROWS": ("llo.replay.pages", "PAGE_ROWS"),
    "LayoutAgent": ("llo.agent.loop", "LayoutAgent"),
}


def __getattr__(name: str) -> Any:
    if name in _LAZY:
        from importlib import import_module

        module_name, attr = _LAZY[name]
        return getattr(import_module(module_name), attr)
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")


__all__ = [
    "PAGE_ROWS",
    "Action",
    "DriftDetector",
    "EpsilonGreedyPolicy",
    "HeuristicPolicy",
    "LayoutAgent",
    "Query",
    "ThompsonPolicy",
    "UCBPolicy",
    "WorkloadProfile",
    "__version__",
    "apply_layout",
    "expected_pages",
    "heuristic_action",
    "hilbert_index",
    "hilbert_index_nd",
    "reward",
    "z_order_index",
]
