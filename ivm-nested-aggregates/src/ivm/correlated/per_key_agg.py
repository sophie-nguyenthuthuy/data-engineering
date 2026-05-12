"""Per-key incremental aggregations: SUM, COUNT, AVG, MIN, MAX.

These maintain a value-per-key map under insert/delete with O(1)
amortised cost — except MIN/MAX which need a sorted-multiset for
delete (so we use sortedcontainers.SortedList).
"""

from __future__ import annotations

import threading
from collections import defaultdict
from dataclasses import dataclass, field
from typing import Any

from sortedcontainers import SortedList


@dataclass
class PerKeySum:
    _sum: dict[Any, float] = field(default_factory=lambda: defaultdict(float))
    _lock: threading.RLock = field(default_factory=threading.RLock)  # type: ignore[assignment]

    def insert(self, key: Any, value: float) -> float:
        with self._lock:
            self._sum[key] += value
            return self._sum[key]

    def delete(self, key: Any, value: float) -> float:
        with self._lock:
            self._sum[key] -= value
            if self._sum[key] == 0:
                self._sum.pop(key, None)
                return 0.0
            return self._sum[key]

    def get(self, key: Any) -> float:
        with self._lock:
            return self._sum.get(key, 0.0)


@dataclass
class PerKeyCount:
    _cnt: dict[Any, int] = field(default_factory=lambda: defaultdict(int))
    _lock: threading.RLock = field(default_factory=threading.RLock)  # type: ignore[assignment]

    def insert(self, key: Any) -> int:
        with self._lock:
            self._cnt[key] += 1
            return self._cnt[key]

    def delete(self, key: Any) -> int:
        with self._lock:
            if self._cnt[key] <= 0:
                return 0
            self._cnt[key] -= 1
            if self._cnt[key] == 0:
                self._cnt.pop(key, None)
                return 0
            return self._cnt[key]

    def get(self, key: Any) -> int:
        with self._lock:
            return self._cnt.get(key, 0)


@dataclass
class PerKeyAvg:
    _sum: PerKeySum = field(default_factory=PerKeySum)
    _cnt: PerKeyCount = field(default_factory=PerKeyCount)

    def insert(self, key: Any, value: float) -> float:
        self._sum.insert(key, value)
        self._cnt.insert(key)
        return self.get(key)

    def delete(self, key: Any, value: float) -> float:
        self._sum.delete(key, value)
        self._cnt.delete(key)
        return self.get(key)

    def get(self, key: Any) -> float:
        n = self._cnt.get(key)
        return self._sum.get(key) / n if n > 0 else 0.0


@dataclass
class PerKeyMax:
    _values: dict[Any, SortedList] = field(default_factory=dict)
    _lock: threading.RLock = field(default_factory=threading.RLock)  # type: ignore[assignment]

    def insert(self, key: Any, value: float) -> float:
        with self._lock:
            sl = self._values.setdefault(key, SortedList())
            sl.add(value)
            return float(sl[-1])

    def delete(self, key: Any, value: float) -> float:
        with self._lock:
            sl = self._values.get(key)
            if sl is None or value not in sl:
                return self.get(key)
            sl.remove(value)
            if not sl:
                self._values.pop(key, None)
                return float("-inf")
            return float(sl[-1])

    def get(self, key: Any) -> float:
        with self._lock:
            sl = self._values.get(key)
            if sl is None or not sl:
                return float("-inf")
            return float(sl[-1])


@dataclass
class PerKeyMin:
    _values: dict[Any, SortedList] = field(default_factory=dict)
    _lock: threading.RLock = field(default_factory=threading.RLock)  # type: ignore[assignment]

    def insert(self, key: Any, value: float) -> float:
        with self._lock:
            sl = self._values.setdefault(key, SortedList())
            sl.add(value)
            return float(sl[0])

    def delete(self, key: Any, value: float) -> float:
        with self._lock:
            sl = self._values.get(key)
            if sl is None or value not in sl:
                return self.get(key)
            sl.remove(value)
            if not sl:
                self._values.pop(key, None)
                return float("inf")
            return float(sl[0])

    def get(self, key: Any) -> float:
        with self._lock:
            sl = self._values.get(key)
            if sl is None or not sl:
                return float("inf")
            return float(sl[0])


__all__ = ["PerKeyAvg", "PerKeyCount", "PerKeyMax", "PerKeyMin", "PerKeySum"]
