"""
OR-Set (Observed-Remove Set) CRDT.

Each element is tagged with a unique token on add. Remove deletes all
observed tokens for that element. An element is in the set iff it has
at least one live token.

Properties:
  - Add wins over concurrent removes of *different* tokens
  - Concurrent add+remove of the same element: add wins (new token survives)
  - Merge is commutative, associative, idempotent
"""
from __future__ import annotations
import uuid
from dataclasses import dataclass, field
from typing import Any, Dict, FrozenSet, Set


@dataclass
class ORSet:
    node_id: str
    # element -> set of unique tokens
    entries: Dict[Any, Set[str]] = field(default_factory=dict)
    # tombstone of removed tokens
    tombstones: Set[str] = field(default_factory=set)

    def add(self, element: Any) -> None:
        token = f"{self.node_id}:{uuid.uuid4().hex}"
        if element not in self.entries:
            self.entries[element] = set()
        self.entries[element].add(token)

    def remove(self, element: Any) -> None:
        if element in self.entries:
            self.tombstones.update(self.entries[element])
            del self.entries[element]

    def contains(self, element: Any) -> bool:
        tokens = self.entries.get(element, set())
        return bool(tokens - self.tombstones)

    def elements(self) -> FrozenSet[Any]:
        return frozenset(e for e in self.entries if self.contains(e))

    def merge(self, other: "ORSet") -> "ORSet":
        result = ORSet(node_id=self.node_id)
        result.tombstones = self.tombstones | other.tombstones

        all_elements = set(self.entries) | set(other.entries)
        for elem in all_elements:
            tokens = (self.entries.get(elem, set()) | other.entries.get(elem, set()))
            live = tokens - result.tombstones
            if live:
                result.entries[elem] = live

        return result

    def merge_into(self, other: "ORSet") -> None:
        self.tombstones |= other.tombstones

        for elem, tokens in other.entries.items():
            if elem not in self.entries:
                self.entries[elem] = set()
            self.entries[elem] |= tokens

        # prune elements whose tokens are all tombstoned
        dead = [e for e, t in self.entries.items() if not (t - self.tombstones)]
        for e in dead:
            del self.entries[e]

    def __len__(self) -> int:
        return len(self.elements())

    def to_dict(self) -> dict:
        return {
            "node_id": self.node_id,
            "entries": {str(k): list(v) for k, v in self.entries.items()},
            "tombstones": list(self.tombstones),
        }

    @classmethod
    def from_dict(cls, data: dict) -> "ORSet":
        obj = cls(node_id=data["node_id"])
        obj.entries = {k: set(v) for k, v in data["entries"].items()}
        obj.tombstones = set(data["tombstones"])
        return obj

    def __repr__(self) -> str:
        return f"ORSet(node={self.node_id}, elements={self.elements()})"
