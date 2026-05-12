"""Per-key arrival-delay estimator.

For each key we keep:
  - a t-digest of (arrival - event-time) → gives any quantile
  - a lognormal fitter (for parametric (1-δ)-quantile)
  - an EVT tail fitter (for very-heavy tails)
  - a running rate estimator (events/second)

`safe_delay(key)` is the watermark-input: the (1-δ)-quantile of delay,
**clamped to be monotone non-decreasing across calls**. This monotonicity
is essential for the watermark to be monotone.
"""

from __future__ import annotations

import threading
from dataclasses import dataclass, field
from typing import Literal

from pwm.fit.evt import POTFitter
from pwm.fit.lognormal import LognormalFitter
from pwm.sketch.tdigest import TDigest

QuantileSource = Literal["tdigest", "lognormal", "evt"]


@dataclass
class _KeyState:
    sketch: TDigest = field(default_factory=lambda: TDigest(delta=100.0))
    lognormal: LognormalFitter = field(default_factory=LognormalFitter)
    evt: POTFitter = field(default_factory=POTFitter)
    last_arrival_ts: float = 0.0
    rate_ema: float = 0.0
    peak_safe_delay: float = 0.0


@dataclass
class PerKeyDelayEstimator:
    """Online per-key (1-δ)-quantile of arrival delay, monotone non-decreasing."""

    delta: float = 1e-3                                     # target lateness rate
    source: QuantileSource = "tdigest"                      # which quantile estimator
    _keys: dict[object, _KeyState] = field(default_factory=dict)
    _lock: threading.RLock = field(default_factory=threading.RLock)  # type: ignore[assignment]

    def observe(self, key: object, event_time: float, arrival_time: float) -> None:
        if arrival_time < event_time:
            arrival_time = event_time
        delay = arrival_time - event_time
        with self._lock:
            st = self._keys.get(key)
            if st is None:
                st = _KeyState()
                self._keys[key] = st
            st.sketch.add(delay)
            st.lognormal.observe(delay)
            st.evt.observe(delay)
            # EMA rate (events / second)
            if st.last_arrival_ts > 0.0:
                gap = max(arrival_time - st.last_arrival_ts, 1e-6)
                inst_rate = 1.0 / gap
                # α = 0.1
                st.rate_ema = 0.9 * st.rate_ema + 0.1 * inst_rate
            st.last_arrival_ts = arrival_time

    def safe_delay(self, key: object) -> float:
        """(1-δ)-quantile of delay for `key`, clamped monotone non-decreasing."""
        with self._lock:
            st = self._keys.get(key)
            if st is None:
                return 0.0
            q = 1.0 - self.delta
            if self.source == "lognormal":
                raw = 0.0 if st.lognormal.n < 2 else st.lognormal.quantile(q)
            elif self.source == "evt":
                raw = st.evt.quantile(q)
            else:
                raw = st.sketch.quantile(q)
            new = max(st.peak_safe_delay, raw)
            st.peak_safe_delay = new
            return new

    def rate(self, key: object) -> float:
        with self._lock:
            st = self._keys.get(key)
            return st.rate_ema if st else 0.0

    def keys(self) -> list[object]:
        with self._lock:
            return list(self._keys.keys())

    def n_observations(self, key: object) -> int:
        with self._lock:
            st = self._keys.get(key)
            return st.sketch.count() if st else 0
