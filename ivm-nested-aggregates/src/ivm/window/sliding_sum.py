"""SUM (or AVG) OVER (PARTITION BY p ORDER BY t ROWS BETWEEN n PRECEDING
AND CURRENT ROW).

For a sliding window of size W ending at the current row, the windowed
sum is `sum(values[max(0, i-W+1) .. i])`. Naïve recomputation per query
is O(W); we maintain a per-partition prefix-sum array so each query is
O(log n) (find i by bisect) + O(1) (prefix-sum subtraction).
"""

from __future__ import annotations

import threading
from bisect import bisect_left
from dataclasses import dataclass, field


@dataclass
class SlidingSumIVM:
    """Maintain per-partition prefix sums under incremental insert/delete."""

    window_size: int = 5
    _partitions: dict[object, list[tuple[float, float]]] = field(default_factory=dict)
    _prefix: dict[object, list[float]] = field(default_factory=dict)
    _lock: threading.RLock = field(default_factory=threading.RLock)  # type: ignore[assignment]

    def insert(self, partition: object, t: float, value: float) -> None:
        with self._lock:
            rows = self._partitions.setdefault(partition, [])
            pre = self._prefix.setdefault(partition, [0.0])
            # Find insert position
            idx = bisect_left(rows, (t, value))
            rows.insert(idx, (t, value))
            # Update prefix from idx onward.
            # Old prefix had len(rows) entries (pre length = len(rows)+1)
            # New: append a new prefix slot.
            base = pre[idx]
            pre.insert(idx + 1, base + value)
            # Shift the tail by +value
            for j in range(idx + 2, len(pre)):
                pre[j] += value

    def delete(self, partition: object, t: float, value: float) -> bool:
        with self._lock:
            rows = self._partitions.get(partition, [])
            pre = self._prefix.get(partition, [0.0])
            entry = (t, value)
            try:
                idx = rows.index(entry)
            except ValueError:
                return False
            rows.pop(idx)
            # Remove pre[idx+1]; shift tail by -value
            pre.pop(idx + 1)
            for j in range(idx + 1, len(pre)):
                pre[j] -= value
            return True

    def sliding_sum(self, partition: object, t: float) -> float | None:
        """Return the SUM over (t - W + 1 .. t] for sort-position of t."""
        with self._lock:
            rows = self._partitions.get(partition)
            if not rows:
                return None
            idx = bisect_left(rows, (t,))
            if idx >= len(rows) or rows[idx][0] != t:
                return None
            lo = max(0, idx - self.window_size + 1)
            pre = self._prefix[partition]
            return pre[idx + 1] - pre[lo]

    def sliding_avg(self, partition: object, t: float) -> float | None:
        s = self.sliding_sum(partition, t)
        if s is None:
            return None
        rows = self._partitions[partition]
        idx = bisect_left(rows, (t,))
        lo = max(0, idx - self.window_size + 1)
        n = idx + 1 - lo
        return s / n if n > 0 else None

__all__ = ["SlidingSumIVM"]
