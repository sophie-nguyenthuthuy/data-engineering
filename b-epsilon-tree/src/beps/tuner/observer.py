"""Workload observer.

Sliding-window counter over recent operations. Exposes:
    read_fraction
    write_fraction
    rate
"""

from __future__ import annotations

import threading
import time
from collections import deque
from dataclasses import dataclass, field
from enum import IntEnum


class Op(IntEnum):
    READ = 1
    WRITE = 2


@dataclass
class WorkloadObserver:
    window: int = 1000
    _events: deque[tuple[Op, float]] = field(default_factory=lambda: deque(maxlen=1000))
    _lock: threading.Lock = field(default_factory=threading.Lock)
    _reads: int = 0
    _writes: int = 0

    def __post_init__(self) -> None:
        self._events = deque(maxlen=self.window)

    def observe(self, op: Op) -> None:
        now = time.perf_counter()
        with self._lock:
            # If event being evicted, decrement counters
            if len(self._events) == self.window:
                old_op, _ = self._events[0]
                if old_op == Op.READ:
                    self._reads -= 1
                else:
                    self._writes -= 1
            self._events.append((op, now))
            if op == Op.READ:
                self._reads += 1
            else:
                self._writes += 1

    @property
    def reads(self) -> int:
        with self._lock:
            return self._reads

    @property
    def writes(self) -> int:
        with self._lock:
            return self._writes

    @property
    def total(self) -> int:
        return self.reads + self.writes

    @property
    def read_fraction(self) -> float:
        t = self.total
        return self.reads / t if t else 0.5

    @property
    def write_fraction(self) -> float:
        t = self.total
        return self.writes / t if t else 0.5
