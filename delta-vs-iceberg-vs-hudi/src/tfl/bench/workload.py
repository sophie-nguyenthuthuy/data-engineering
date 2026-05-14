"""CDC workload — the cross-format comparison fixture."""

from __future__ import annotations

from dataclasses import dataclass
from enum import Enum


class CDCOp(str, Enum):
    """Per-row mutation kind."""

    INSERT = "insert"
    UPDATE = "update"
    DELETE = "delete"


@dataclass(frozen=True, slots=True)
class CDCEvent:
    """One row-mutation event in the workload stream."""

    op: CDCOp
    key: str
    payload_size: int  # bytes the new value would occupy on disk

    def __post_init__(self) -> None:
        if not self.key:
            raise ValueError("key must be non-empty")
        if self.payload_size < 0:
            raise ValueError("payload_size must be ≥ 0")


@dataclass(frozen=True, slots=True)
class Workload:
    """A named CDC event stream."""

    name: str
    events: tuple[CDCEvent, ...]

    def __post_init__(self) -> None:
        if not self.name:
            raise ValueError("name must be non-empty")
        if not self.events:
            raise ValueError("workload must have ≥ 1 event")

    def update_ratio(self) -> float:
        n_upd = sum(1 for e in self.events if e.op is CDCOp.UPDATE)
        return n_upd / len(self.events)


__all__ = ["CDCEvent", "CDCOp", "Workload"]
