"""Per-key delay-distribution estimator.

For each key, maintain an online sketch of arrival_time - event_time and answer
the question: "what is the (1-δ)-quantile of arrival delay for this key?"

We use a t-digest-style merging sketch — simple, bounded memory, monotone in
quantile updates (necessary for the watermark monotonicity proof).
"""
from __future__ import annotations

from dataclasses import dataclass, field


@dataclass
class _Bin:
    mean: float
    count: int


class TDigestLite:
    """Compact t-digest-style quantile sketch.

    Each bin holds (mean, count). On insert we either merge into nearest bin
    (if doing so wouldn't violate the compression invariant) or insert a new
    one. Memory is bounded by `max_bins`.
    """

    def __init__(self, compression: int = 100):
        self.compression = compression
        self.max_bins = compression * 4
        self._bins: list[_Bin] = []
        self._n: int = 0

    def add(self, x: float) -> None:
        self._n += 1
        if not self._bins:
            self._bins.append(_Bin(x, 1))
            return
        # Find nearest bin by mean
        nearest = min(range(len(self._bins)),
                      key=lambda i: abs(self._bins[i].mean - x))
        b = self._bins[nearest]
        # Bin can absorb if count + 1 ≤ allowed (proportional to quantile width)
        q = self._cumulative_count(nearest) / self._n
        max_count = 4 * self._n * q * (1 - q) / self.compression + 1
        if b.count + 1 <= max_count:
            b.mean = (b.mean * b.count + x) / (b.count + 1)
            b.count += 1
        else:
            self._bins.append(_Bin(x, 1))
            self._bins.sort(key=lambda b: b.mean)
        if len(self._bins) > self.max_bins:
            self._compress()

    def _cumulative_count(self, idx: int) -> float:
        return sum(b.count for b in self._bins[:idx]) + self._bins[idx].count / 2.0

    def _compress(self) -> None:
        """Greedy merge of adjacent bins."""
        merged: list[_Bin] = []
        i = 0
        while i < len(self._bins):
            if i + 1 < len(self._bins) and self._bins[i].count + self._bins[i+1].count < self.compression:
                a, b = self._bins[i], self._bins[i+1]
                m = _Bin((a.mean * a.count + b.mean * b.count) / (a.count + b.count),
                         a.count + b.count)
                merged.append(m)
                i += 2
            else:
                merged.append(self._bins[i])
                i += 1
        self._bins = merged

    def quantile(self, q: float) -> float:
        """Approximate q-quantile, 0 ≤ q ≤ 1."""
        if not self._bins:
            return 0.0
        if q <= 0:
            return self._bins[0].mean
        if q >= 1:
            return self._bins[-1].mean
        target = q * self._n
        cum = 0
        for b in self._bins:
            if cum + b.count >= target:
                return b.mean
            cum += b.count
        return self._bins[-1].mean

    def count(self) -> int:
        return self._n


@dataclass
class PerKeyDelayEstimator:
    """One sketch per key + a high-water mark for delay quantile monotonicity."""
    delta: float = 1e-3
    compression: int = 100
    _sketches: dict = field(default_factory=dict)
    _peak_quantile: dict = field(default_factory=dict)
    _rate: dict = field(default_factory=lambda: {})        # observations per arrival
    _last_arrival: dict = field(default_factory=dict)

    def observe(self, key, event_time: float, arrival_time: float) -> None:
        """Record one event for `key`."""
        if key not in self._sketches:
            self._sketches[key] = TDigestLite(self.compression)
        delay = max(0.0, arrival_time - event_time)
        self._sketches[key].add(delay)
        # Tracking rate as moving average
        if key in self._last_arrival:
            inter = arrival_time - self._last_arrival[key]
            if inter > 0:
                prev = self._rate.get(key, 1.0 / max(inter, 1e-9))
                # EMA over rate
                self._rate[key] = 0.9 * prev + 0.1 * (1.0 / inter)
        self._last_arrival[key] = arrival_time

    def safe_delay(self, key) -> float:
        """The (1-δ)-quantile of delay for `key`, made monotone non-decreasing
        across calls so the watermark function is monotone."""
        if key not in self._sketches:
            return 0.0
        q = self._sketches[key].quantile(1.0 - self.delta)
        prev = self._peak_quantile.get(key, 0.0)
        # Monotone non-decrease (conservative)
        new = max(prev, q)
        self._peak_quantile[key] = new
        return new

    def rate(self, key) -> float:
        return self._rate.get(key, 0.0)

    def keys(self):
        return self._sketches.keys()


__all__ = ["TDigestLite", "PerKeyDelayEstimator"]
