"""Online epsilon tuner.

Given an observed read/write mix, recommend an epsilon ∈ (ε_min, ε_max).

Heuristic:
    target_ε = ε_min + (ε_max - ε_min) * write_fraction
where ε_min ≈ 0.1 (lots of pivots, B+-tree-like for reads)
and   ε_max ≈ 0.9 (lots of buffer, sort-merge-like for writes).

Hysteresis: don't switch unless the target diverges by `hysteresis` from
the current value. Prevents flapping under noisy mixes.
"""

from __future__ import annotations

import threading
from dataclasses import dataclass, field

from beps.tuner.observer import Op, WorkloadObserver


@dataclass
class EpsilonTuner:
    initial_epsilon: float = 0.5
    eps_min: float = 0.1
    eps_max: float = 0.9
    hysteresis: float = 0.05
    observer: WorkloadObserver = field(default_factory=WorkloadObserver)
    _lock: threading.Lock = field(default_factory=threading.Lock)
    _current: float = 0.5
    _resets: int = 0

    def __post_init__(self) -> None:
        self._current = self.initial_epsilon

    def observe(self, op: Op) -> None:
        self.observer.observe(op)

    def recommend(self) -> float:
        with self._lock:
            target = self.eps_min + (self.eps_max - self.eps_min) * self.observer.write_fraction
            target = max(self.eps_min, min(self.eps_max, target))
            if abs(target - self._current) >= self.hysteresis:
                self._current = target
                self._resets += 1
            return self._current

    @property
    def current(self) -> float:
        with self._lock:
            return self._current

    @property
    def n_switches(self) -> int:
        with self._lock:
            return self._resets
