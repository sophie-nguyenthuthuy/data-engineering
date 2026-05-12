"""
G-Counter (Grow-only Counter) CRDT.

Each node maintains its own slot in a vector. Increment is local.
Merge takes the element-wise maximum. Value is the sum of all slots.

Invariants:
  - counters[node_id] only ever increases
  - merge(a, b) >= a and merge(a, b) >= b (monotone join)
  - merge is commutative, associative, idempotent
"""
from __future__ import annotations
from dataclasses import dataclass, field
from typing import Dict


@dataclass
class GCounter:
    node_id: str
    counters: Dict[str, int] = field(default_factory=dict)

    def __post_init__(self):
        self.counters.setdefault(self.node_id, 0)

    def increment(self, amount: int = 1) -> None:
        if amount < 0:
            raise ValueError("G-Counter only supports non-negative increments")
        self.counters[self.node_id] = self.counters.get(self.node_id, 0) + amount

    def value(self) -> int:
        return sum(self.counters.values())

    def merge(self, other: "GCounter") -> "GCounter":
        merged = dict(self.counters)
        for node, count in other.counters.items():
            merged[node] = max(merged.get(node, 0), count)
        return GCounter(node_id=self.node_id, counters=merged)

    def merge_into(self, other: "GCounter") -> None:
        for node, count in other.counters.items():
            if count > self.counters.get(node, 0):
                self.counters[node] = count

    def clone(self) -> "GCounter":
        return GCounter(node_id=self.node_id, counters=dict(self.counters))

    def to_dict(self) -> dict:
        return {"node_id": self.node_id, "counters": dict(self.counters)}

    @classmethod
    def from_dict(cls, data: dict) -> "GCounter":
        return cls(node_id=data["node_id"], counters=dict(data["counters"]))

    def __le__(self, other: "GCounter") -> bool:
        """Partial order: self <= other if every slot of self <= other."""
        for node, count in self.counters.items():
            if count > other.counters.get(node, 0):
                return False
        return True

    def __repr__(self) -> str:
        return f"GCounter(node={self.node_id}, value={self.value()}, slots={self.counters})"
