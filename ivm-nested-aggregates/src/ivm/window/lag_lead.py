"""LAG(value, k) and LEAD(value, k) OVER (PARTITION BY p ORDER BY t).

We store per-partition sorted (t, value) tuples; LAG(k) at position i
returns the value at position i-k (or None if out of bounds).

Insertion at position i shifts ALL positions ≥ i by 1, but for LAG/LEAD
the observable effect is local: only positions [i-k .. i+k] see a change.
"""

from __future__ import annotations

import threading
from bisect import bisect_left, insort
from dataclasses import dataclass, field


@dataclass
class LagLeadIVM:
    _partitions: dict[object, list[tuple[float, object]]] = field(default_factory=dict)
    _lock: threading.RLock = field(default_factory=threading.RLock)  # type: ignore[assignment]

    def insert(self, partition: object, t: float, value: object) -> None:
        with self._lock:
            insort(self._partitions.setdefault(partition, []), (t, value))

    def delete(self, partition: object, t: float, value: object) -> bool:
        with self._lock:
            rows = self._partitions.get(partition, [])
            try:
                rows.remove((t, value))
                return True
            except ValueError:
                return False

    def lag(self, partition: object, t: float, k: int = 1) -> object | None:
        """Return the value that is `k` rows before `t` in the partition.

        If `t` matches multiple rows (ties), uses the first occurrence."""
        if k < 1:
            raise ValueError("k must be >= 1")
        with self._lock:
            rows = self._partitions.get(partition)
            if not rows:
                return None
            idx = bisect_left(rows, (t,))
            # Ensure (t, _) exists at idx
            if idx >= len(rows) or rows[idx][0] != t:
                return None
            lag_idx = idx - k
            if lag_idx < 0:
                return None
            return rows[lag_idx][1]

    def lead(self, partition: object, t: float, k: int = 1) -> object | None:
        """Return the value that is `k` rows after `t` in the partition."""
        if k < 1:
            raise ValueError("k must be >= 1")
        with self._lock:
            rows = self._partitions.get(partition)
            if not rows:
                return None
            idx = bisect_left(rows, (t,))
            if idx >= len(rows) or rows[idx][0] != t:
                return None
            lead_idx = idx + k
            if lead_idx >= len(rows):
                return None
            return rows[lead_idx][1]


__all__ = ["LagLeadIVM"]
