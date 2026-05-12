"""Timestamp algebra for Timely Dataflow.

Naiad timestamps are (epoch, iteration) pairs with a partial order:

    (e1, i1) ≤ (e2, i2)  iff  e1 ≤ e2  AND  i1 ≤ i2

This forms a lattice. The least upper bound (join) is component-wise max.
"""
from __future__ import annotations

from dataclasses import dataclass
from functools import total_ordering


@total_ordering
@dataclass(frozen=True)
class Timestamp:
    epoch: int
    iteration: int = 0

    def __le__(self, other: "Timestamp") -> bool:
        """Partial order. NOTE: not total — two timestamps can be incomparable."""
        return self.epoch <= other.epoch and self.iteration <= other.iteration

    def __lt__(self, other: "Timestamp") -> bool:
        return self <= other and self != other

    def join(self, other: "Timestamp") -> "Timestamp":
        """Lattice join (component-wise max). Used for frontier merging."""
        return Timestamp(max(self.epoch, other.epoch),
                         max(self.iteration, other.iteration))

    def meet(self, other: "Timestamp") -> "Timestamp":
        return Timestamp(min(self.epoch, other.epoch),
                         min(self.iteration, other.iteration))

    def next_iter(self) -> "Timestamp":
        return Timestamp(self.epoch, self.iteration + 1)

    def next_epoch(self) -> "Timestamp":
        return Timestamp(self.epoch + 1, 0)

    def __repr__(self) -> str:
        return f"({self.epoch}.{self.iteration})"


def comparable(a: Timestamp, b: Timestamp) -> bool:
    return a <= b or b <= a


def antichain_insert(antichain: set[Timestamp], t: Timestamp) -> set[Timestamp]:
    """Insert into an antichain (set of pairwise-incomparable timestamps).
    Removes any element dominated by t; refuses to insert if t is dominated."""
    for x in antichain:
        if x <= t and x != t:
            # x strictly dominates direction — drop x
            pass
    # Drop dominated elements
    new = {x for x in antichain if not (t < x or t == x)}
    # Refuse if t is dominated by some surviving x
    if any(x < t for x in new):
        return new
    new.add(t)
    return new


__all__ = ["Timestamp", "comparable", "antichain_insert"]
