"""Runtime invariant checker for monotonicity.

Wraps a WatermarkAdvancer; verifies that consecutive watermark values
satisfy `W(t_{n+1}) >= W(t_n)`. Raises MonotonicityViolation on breach.

Used in tests + production canaries. Per-key safe_delay monotonicity is
the precondition for the global watermark monotonicity; we check both.
"""

from __future__ import annotations

import threading
from dataclasses import dataclass, field
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from pwm.watermark.advancer import WatermarkAdvancer


class MonotonicityViolation(Exception):
    pass


@dataclass
class MonotonicityChecker:
    advancer: WatermarkAdvancer
    _last_w: float = -float("inf")
    _last_safe_delay: dict[object, float] = field(default_factory=dict)
    _lock: threading.RLock = field(default_factory=threading.RLock)  # type: ignore[assignment]
    violations: list[str] = field(default_factory=list)
    strict: bool = True              # if True, raise on violation

    def check(self, key: object, event_time: float, arrival_time: float) -> tuple[str, float]:
        status, w = self.advancer.on_record(key, event_time, arrival_time)
        with self._lock:
            if w < self._last_w:
                msg = (f"watermark went backwards: {self._last_w} -> {w} "
                       f"at key={key!r} event={event_time} arr={arrival_time}")
                self.violations.append(msg)
                if self.strict:
                    raise MonotonicityViolation(msg)
            self._last_w = w
            # Per-key safe_delay monotonicity
            sd = self.advancer.delay_estimator.safe_delay(key)
            prev = self._last_safe_delay.get(key, -float("inf"))
            if sd < prev:
                msg = (f"safe_delay({key!r}) went backwards: {prev} -> {sd}")
                self.violations.append(msg)
                if self.strict:
                    raise MonotonicityViolation(msg)
            self._last_safe_delay[key] = sd
        return status, w
