"""RANK and DENSE_RANK IVM.

RANK: ties get the same rank; next rank skips by tie-count.
    [10, 20, 20, 30] → ranks [1, 2, 2, 4]
DENSE_RANK: ties get the same rank; next rank is +1.
    [10, 20, 20, 30] → ranks [1, 2, 2, 3]

The ranking depends only on the SORT KEY value, not on row identity.
"""

from __future__ import annotations

import threading
from bisect import insort
from dataclasses import dataclass, field


@dataclass
class RankIVM:
    """RANK() OVER (PARTITION BY p ORDER BY t).

    On insert: ties of equal `t` share the same rank; next unique value
    moves the rank forward by the tie count.
    """

    _partitions: dict[object, list[float]] = field(default_factory=dict)
    _lock: threading.RLock = field(default_factory=threading.RLock)  # type: ignore[assignment]

    def insert(self, partition: object, t: float) -> None:
        with self._lock:
            insort(self._partitions.setdefault(partition, []), t)

    def delete(self, partition: object, t: float) -> bool:
        with self._lock:
            rows = self._partitions.get(partition, [])
            try:
                rows.remove(t)
                return True
            except ValueError:
                return False

    def rank_of(self, partition: object, t: float) -> int | None:
        """Return 1-indexed RANK of `t` in its partition, or None if absent."""
        with self._lock:
            rows = self._partitions.get(partition)
            if rows is None or t not in rows:
                return None
            # First index where rows[i] == t  (RANK semantics).
            # We treat duplicates: rank = position of first occurrence + 1.
            from bisect import bisect_left
            return bisect_left(rows, t) + 1


@dataclass
class DenseRankIVM:
    """DENSE_RANK() OVER (PARTITION BY p ORDER BY t)."""

    _partitions: dict[object, list[float]] = field(default_factory=dict)
    _lock: threading.RLock = field(default_factory=threading.RLock)  # type: ignore[assignment]

    def insert(self, partition: object, t: float) -> None:
        with self._lock:
            insort(self._partitions.setdefault(partition, []), t)

    def delete(self, partition: object, t: float) -> bool:
        with self._lock:
            rows = self._partitions.get(partition, [])
            try:
                rows.remove(t)
                return True
            except ValueError:
                return False

    def rank_of(self, partition: object, t: float) -> int | None:
        with self._lock:
            rows = self._partitions.get(partition)
            if rows is None or t not in rows:
                return None
            # DENSE_RANK: count of distinct values <= t
            distinct: set[float] = set()
            for v in rows:
                if v <= t:
                    distinct.add(v)
                else:
                    break
            return len(distinct)


__all__ = ["DenseRankIVM", "RankIVM"]
