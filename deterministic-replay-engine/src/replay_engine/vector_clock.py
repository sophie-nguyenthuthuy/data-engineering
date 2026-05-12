"""Vector clock implementation for tracking causal relationships."""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum, auto
from typing import Mapping


class Relation(Enum):
    BEFORE = auto()    # self happened-before other
    AFTER = auto()     # other happened-before self
    CONCURRENT = auto()
    EQUAL = auto()


@dataclass
class VectorClock:
    """Immutable-ish vector clock.  Mutations return new instances."""

    clocks: dict[str, int] = field(default_factory=dict)

    def __post_init__(self) -> None:
        # Normalize: all values non-negative integers.
        self.clocks = {k: int(v) for k, v in self.clocks.items() if int(v) >= 0}

    # ------------------------------------------------------------------
    # Core operations
    # ------------------------------------------------------------------
    def get(self, producer_id: str) -> int:
        return self.clocks.get(producer_id, 0)

    def increment(self, producer_id: str) -> VectorClock:
        updated = dict(self.clocks)
        updated[producer_id] = updated.get(producer_id, 0) + 1
        return VectorClock(updated)

    def merge(self, other: VectorClock) -> VectorClock:
        """Component-wise maximum (union of clocks)."""
        all_keys = set(self.clocks) | set(other.clocks)
        return VectorClock(
            {k: max(self.clocks.get(k, 0), other.clocks.get(k, 0)) for k in all_keys}
        )

    # ------------------------------------------------------------------
    # Comparison (happens-before partial order)
    # ------------------------------------------------------------------
    def compare(self, other: VectorClock) -> Relation:
        all_keys = set(self.clocks) | set(other.clocks)
        self_leq = all(self.clocks.get(k, 0) <= other.clocks.get(k, 0) for k in all_keys)
        other_leq = all(other.clocks.get(k, 0) <= self.clocks.get(k, 0) for k in all_keys)

        if self_leq and other_leq:
            return Relation.EQUAL
        if self_leq:
            return Relation.BEFORE
        if other_leq:
            return Relation.AFTER
        return Relation.CONCURRENT

    def happens_before(self, other: VectorClock) -> bool:
        return self.compare(other) == Relation.BEFORE

    def concurrent_with(self, other: VectorClock) -> bool:
        return self.compare(other) == Relation.CONCURRENT

    # ------------------------------------------------------------------
    # Dunder helpers
    # ------------------------------------------------------------------
    def __eq__(self, other: object) -> bool:
        if not isinstance(other, VectorClock):
            return NotImplemented
        return self.compare(other) == Relation.EQUAL

    def __le__(self, other: VectorClock) -> bool:
        return self.compare(other) in (Relation.BEFORE, Relation.EQUAL)

    def __lt__(self, other: VectorClock) -> bool:
        return self.compare(other) == Relation.BEFORE

    def __repr__(self) -> str:
        parts = ", ".join(f"{k}:{v}" for k, v in sorted(self.clocks.items()))
        return f"VC({{{parts}}})"

    def __hash__(self) -> int:
        return hash(tuple(sorted(self.clocks.items())))
