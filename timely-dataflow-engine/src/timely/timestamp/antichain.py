"""Antichain — set of pairwise-incomparable timestamps.

The progress frontier is represented as an antichain of minimal active
timestamps. Antichains have well-defined `insert` semantics: insertion
removes any element that the new one dominates.
"""

from __future__ import annotations

import threading
from dataclasses import dataclass, field
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from collections.abc import Iterator

    from timely.timestamp.ts import Timestamp


@dataclass
class Antichain:
    """Set of pairwise-incomparable timestamps."""

    _elements: set[Timestamp] = field(default_factory=set)
    _lock: threading.RLock = field(default_factory=threading.RLock)  # type: ignore[assignment]

    def insert(self, t: Timestamp) -> None:
        """Insert `t` if not already dominated; remove any element t dominates."""
        with self._lock:
            # If any existing element ≤ t, t is dominated; skip.
            for x in self._elements:
                if x <= t:
                    return
            # Remove any element dominated by t (x > t)
            self._elements = {x for x in self._elements if not (t < x)}
            self._elements.add(t)

    def remove(self, t: Timestamp) -> bool:
        with self._lock:
            if t in self._elements:
                self._elements.remove(t)
                return True
            return False

    def dominates(self, t: Timestamp) -> bool:
        """True if some element in the antichain dominates `t` (element ≤ t)."""
        with self._lock:
            return any(x <= t for x in self._elements)

    def less_than(self, t: Timestamp) -> bool:
        """True if some element in the antichain is strictly less than `t`."""
        with self._lock:
            return any(x < t for x in self._elements)

    def __len__(self) -> int:
        with self._lock:
            return len(self._elements)

    def __iter__(self) -> Iterator[Timestamp]:
        with self._lock:
            return iter(sorted(self._elements, key=lambda t: (t.epoch, t.iteration)))

    def __contains__(self, t: Timestamp) -> bool:
        with self._lock:
            return t in self._elements

    def copy(self) -> Antichain:
        with self._lock:
            new = Antichain()
            new._elements = set(self._elements)
            return new

    def elements(self) -> list[Timestamp]:
        with self._lock:
            return sorted(self._elements, key=lambda t: (t.epoch, t.iteration))

    def __repr__(self) -> str:
        with self._lock:
            inner = ", ".join(
                repr(t) for t in sorted(self._elements, key=lambda t: (t.epoch, t.iteration))
            )
            return f"Antichain({{{inner}}})"


__all__ = ["Antichain"]
