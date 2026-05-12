"""ROW_NUMBER() OVER (PARTITION BY p ORDER BY t).

Inserting a row at sort-position i shifts ranks of all rows at positions
>= i by +1. We emit deltas only for the affected suffix — the
"affected-suffix" optimisation.

Order-statistic structure: a sorted list with bisect. Insertion is
O(log n) for finding the index + O(n) for the actual list insert. A
balanced BST would give true O(log n) but pure Python list insert is
fast in practice for moderate partition sizes.
"""

from __future__ import annotations

import threading
from bisect import bisect_left, insort
from dataclasses import dataclass, field


@dataclass
class RowNumberIVM:
    """Per-partition rankings under incremental insert/delete."""

    _partitions: dict[object, list[tuple[float, object]]] = field(default_factory=dict)
    _lock: threading.RLock = field(default_factory=threading.RLock)  # type: ignore[assignment]

    def insert(
        self, partition: object, t: float, row_id: object
    ) -> list[tuple[object, int]]:
        """Insert `(t, row_id)` into partition. Return deltas for the
        suffix: `(row_id, new_rank)` pairs, 1-indexed."""
        with self._lock:
            rows = self._partitions.setdefault(partition, [])
            entry = (t, row_id)
            insort(rows, entry)
            idx = bisect_left(rows, entry)
            return [(rid, i + 1) for i, (_, rid) in enumerate(rows[idx:], start=idx)]

    def delete(
        self, partition: object, t: float, row_id: object
    ) -> list[tuple[object, int]]:
        """Delete `(t, row_id)` from partition. Return deltas for the suffix."""
        with self._lock:
            rows = self._partitions.get(partition, [])
            entry = (t, row_id)
            try:
                idx = rows.index(entry)
            except ValueError:
                return []
            rows.pop(idx)
            return [(rid, i + 1) for i, (_, rid) in enumerate(rows[idx:], start=idx)]

    def rank(self, partition: object, t: float, row_id: object) -> int | None:
        with self._lock:
            rows = self._partitions.get(partition)
            if rows is None:
                return None
            entry = (t, row_id)
            try:
                return rows.index(entry) + 1
            except ValueError:
                return None

    def partition_size(self, partition: object) -> int:
        with self._lock:
            return len(self._partitions.get(partition, []))

    def partitions(self) -> list[object]:
        with self._lock:
            return list(self._partitions.keys())


__all__ = ["RowNumberIVM"]
