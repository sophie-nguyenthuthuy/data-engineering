"""causal-feature-store — per-entity vector-clock causal-consistency."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

__version__ = "0.1.0"

if TYPE_CHECKING:
    from cfs.clock.vector_clock import (
        VectorClock,
        bump,
        concurrent,
        dominates,
        equal,
        lt,
        pointwise_max,
    )
    from cfs.partition import PartitionScenario
    from cfs.serving.resolver import ResolvedVector, Resolver
    from cfs.store.cold import ColdStore
    from cfs.store.hot import HotStore
    from cfs.store.version import Version
    from cfs.writer import Writer

_LAZY: dict[str, tuple[str, str]] = {
    "VectorClock": ("cfs.clock.vector_clock", "VectorClock"),
    "bump": ("cfs.clock.vector_clock", "bump"),
    "dominates": ("cfs.clock.vector_clock", "dominates"),
    "equal": ("cfs.clock.vector_clock", "equal"),
    "lt": ("cfs.clock.vector_clock", "lt"),
    "concurrent": ("cfs.clock.vector_clock", "concurrent"),
    "pointwise_max": ("cfs.clock.vector_clock", "pointwise_max"),
    "Version": ("cfs.store.version", "Version"),
    "HotStore": ("cfs.store.hot", "HotStore"),
    "ColdStore": ("cfs.store.cold", "ColdStore"),
    "Writer": ("cfs.writer", "Writer"),
    "Resolver": ("cfs.serving.resolver", "Resolver"),
    "ResolvedVector": ("cfs.serving.resolver", "ResolvedVector"),
    "PartitionScenario": ("cfs.partition", "PartitionScenario"),
}


def __getattr__(name: str) -> Any:
    if name in _LAZY:
        from importlib import import_module

        module, attr = _LAZY[name]
        return getattr(import_module(module), attr)
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")


__all__ = [
    "ColdStore",
    "HotStore",
    "PartitionScenario",
    "ResolvedVector",
    "Resolver",
    "VectorClock",
    "Version",
    "Writer",
    "__version__",
    "bump",
    "concurrent",
    "dominates",
    "equal",
    "lt",
    "pointwise_max",
]
