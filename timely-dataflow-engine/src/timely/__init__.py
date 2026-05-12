"""Timely Dataflow Engine (Naiad-style)."""

from __future__ import annotations

__version__ = "0.1.0"

from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:  # pragma: no cover
    from timely.graph.builder import GraphBuilder
    from timely.graph.runtime import Runtime
    from timely.progress.tracker import ProgressTracker
    from timely.timestamp.antichain import Antichain
    from timely.timestamp.ts import Timestamp

_LAZY: dict[str, str] = {
    "Timestamp": "timely.timestamp.ts",
    "Antichain": "timely.timestamp.antichain",
    "ProgressTracker": "timely.progress.tracker",
    "GraphBuilder": "timely.graph.builder",
    "Runtime": "timely.graph.runtime",
}


def __getattr__(name: str) -> Any:
    mod = _LAZY.get(name)
    if mod is None:
        raise AttributeError(f"module 'timely' has no attribute {name!r}")
    import importlib

    return getattr(importlib.import_module(mod), name)


__all__ = ["Antichain", "GraphBuilder", "ProgressTracker", "Runtime", "Timestamp"]
