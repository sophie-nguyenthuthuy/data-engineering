"""
PNCounter (Positive-Negative Counter) CRDT.

Composed of two G-Counters: one for increments (P), one for decrements (N).
Value = P.value() - N.value()

Supports both increment and decrement while preserving all CRDT properties.
"""
from __future__ import annotations
from dataclasses import dataclass, field
from .g_counter import GCounter


@dataclass
class PNCounter:
    node_id: str
    p: GCounter = field(init=False)
    n: GCounter = field(init=False)

    def __post_init__(self):
        self.p = GCounter(node_id=self.node_id)
        self.n = GCounter(node_id=self.node_id)

    def increment(self, amount: int = 1) -> None:
        if amount < 0:
            raise ValueError("Use decrement() for negative changes")
        self.p.increment(amount)

    def decrement(self, amount: int = 1) -> None:
        if amount < 0:
            raise ValueError("amount must be positive")
        self.n.increment(amount)

    def value(self) -> int:
        return self.p.value() - self.n.value()

    def merge(self, other: "PNCounter") -> "PNCounter":
        result = PNCounter(node_id=self.node_id)
        result.p = self.p.merge(other.p)
        result.n = self.n.merge(other.n)
        return result

    def merge_into(self, other: "PNCounter") -> None:
        self.p.merge_into(other.p)
        self.n.merge_into(other.n)

    def clone(self) -> "PNCounter":
        result = PNCounter(node_id=self.node_id)
        result.p = self.p.clone()
        result.n = self.n.clone()
        return result

    def to_dict(self) -> dict:
        return {
            "node_id": self.node_id,
            "p": self.p.to_dict(),
            "n": self.n.to_dict(),
        }

    @classmethod
    def from_dict(cls, data: dict) -> "PNCounter":
        obj = cls(node_id=data["node_id"])
        obj.p = GCounter.from_dict(data["p"])
        obj.n = GCounter.from_dict(data["n"])
        return obj

    def __repr__(self) -> str:
        return f"PNCounter(node={self.node_id}, value={self.value()}, +{self.p.value()}/-{self.n.value()})"
