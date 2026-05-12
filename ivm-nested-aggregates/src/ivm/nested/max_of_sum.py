"""MAX(SUM(amount)) GROUP BY date — nested aggregate.

Inner: per-date running sum (PerKeySum).
Outer: MAX over inner table. We track which date currently holds the
MAX so that:
  - inserts that don't beat current MAX → O(1) update (just bump that
    date's sum)
  - inserts that DO beat current MAX → O(1) (the new key takes over)
  - deletes from current MAX-holder may require O(K) re-scan of all
    dates' sums to find the new MAX (rare in practice)
"""

from __future__ import annotations

import threading
from dataclasses import dataclass, field
from typing import Any


@dataclass
class MaxOfSum:
    _sums: dict[Any, float] = field(default_factory=dict)
    _max_value: float = float("-inf")
    _max_key: Any = None
    _lock: threading.RLock = field(default_factory=threading.RLock)  # type: ignore[assignment]

    def insert(self, key: Any, amount: float) -> tuple[float, Any]:
        with self._lock:
            self._sums[key] = self._sums.get(key, 0.0) + amount
            new = self._sums[key]
            if new > self._max_value:
                self._max_value = new
                self._max_key = key
            elif key == self._max_key and new < self._max_value:
                self._recompute_locked()
            return self._max_value, self._max_key

    def delete(self, key: Any, amount: float) -> tuple[float, Any]:
        with self._lock:
            cur = self._sums.get(key, 0.0)
            new = cur - amount
            if new == 0:
                self._sums.pop(key, None)
            else:
                self._sums[key] = new
            if key == self._max_key:
                self._recompute_locked()
            return self._max_value, self._max_key

    def _recompute_locked(self) -> None:
        if not self._sums:
            self._max_value = float("-inf")
            self._max_key = None
            return
        best_k = max(self._sums, key=lambda k: self._sums[k])
        self._max_value = self._sums[best_k]
        self._max_key = best_k

    @property
    def max(self) -> tuple[float, Any]:
        with self._lock:
            return self._max_value, self._max_key

    def sum_of(self, key: Any) -> float:
        with self._lock:
            return self._sums.get(key, 0.0)


__all__ = ["MaxOfSum"]
