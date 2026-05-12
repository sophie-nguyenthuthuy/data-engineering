"""SUM(MAX(amount)) GROUP BY date — nested in the other direction.

Inner: per-date MAX (PerKeyMax).
Outer: SUM over the inner table.

When a key's MAX changes, we update the outer SUM by (new_max - old_max).
"""

from __future__ import annotations

import threading
from dataclasses import dataclass, field
from typing import Any

from ivm.correlated.per_key_agg import PerKeyMax


@dataclass
class SumOfMax:
    _max: PerKeyMax = field(default_factory=PerKeyMax)
    _outer_sum: float = 0.0
    _per_key_max_known: dict[Any, float] = field(default_factory=dict)
    _lock: threading.RLock = field(default_factory=threading.RLock)  # type: ignore[assignment]

    def insert(self, key: Any, value: float) -> float:
        with self._lock:
            old_max = self._per_key_max_known.get(key, float("-inf"))
            self._max.insert(key, value)
            new_max = self._max.get(key)
            if old_max == float("-inf"):
                self._outer_sum += new_max
            elif new_max != old_max:
                self._outer_sum += (new_max - old_max)
            self._per_key_max_known[key] = new_max
            return self._outer_sum

    def delete(self, key: Any, value: float) -> float:
        with self._lock:
            old_max = self._per_key_max_known.get(key, float("-inf"))
            self._max.delete(key, value)
            new_max = self._max.get(key)
            if new_max == float("-inf"):
                # Key dropped entirely
                self._outer_sum -= old_max
                self._per_key_max_known.pop(key, None)
            elif new_max != old_max:
                self._outer_sum += (new_max - old_max)
                self._per_key_max_known[key] = new_max
            return self._outer_sum

    @property
    def total(self) -> float:
        with self._lock:
            return self._outer_sum

    def max_of(self, key: Any) -> float:
        return self._max.get(key)


__all__ = ["SumOfMax"]
