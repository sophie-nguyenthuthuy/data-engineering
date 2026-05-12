"""(epoch, iteration) timestamp.

The Naiad model: timestamps form a *lattice* under a partial order
(`a ≤ b` iff a.epoch ≤ b.epoch AND a.iter ≤ b.iter). Two timestamps
can be incomparable.

The lattice **join** (least upper bound) is component-wise max.
The lattice **meet** (greatest lower bound) is component-wise min.

This module is hot-path; we keep it tight.
"""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True, slots=True)
class Timestamp:
    epoch: int
    iteration: int = 0

    # ---- Partial order ----------------------------------------------------

    def __le__(self, other: Timestamp) -> bool:
        return self.epoch <= other.epoch and self.iteration <= other.iteration

    def __lt__(self, other: Timestamp) -> bool:
        return self <= other and self != other

    def __ge__(self, other: Timestamp) -> bool:
        return other <= self

    def __gt__(self, other: Timestamp) -> bool:
        return other < self

    # NB: __eq__/__hash__ provided by frozen dataclass; they use both fields.

    # ---- Lattice operations ----------------------------------------------

    def join(self, other: Timestamp) -> Timestamp:
        """Least upper bound (component-wise max)."""
        return Timestamp(
            max(self.epoch, other.epoch),
            max(self.iteration, other.iteration),
        )

    def meet(self, other: Timestamp) -> Timestamp:
        """Greatest lower bound (component-wise min)."""
        return Timestamp(
            min(self.epoch, other.epoch),
            min(self.iteration, other.iteration),
        )

    # ---- Naiad timestamp transformations ---------------------------------

    def next_iter(self) -> Timestamp:
        return Timestamp(self.epoch, self.iteration + 1)

    def next_epoch(self) -> Timestamp:
        return Timestamp(self.epoch + 1, 0)

    def __repr__(self) -> str:
        return f"({self.epoch}.{self.iteration})"


def comparable(a: Timestamp, b: Timestamp) -> bool:
    """True iff a and b are comparable in the partial order."""
    return a <= b or b <= a


__all__ = ["Timestamp", "comparable"]
