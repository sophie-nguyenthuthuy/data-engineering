"""Quantile sketch — adaptive nearest-bin algorithm.

A bin represents (mean, count). On insert we either merge into the nearest
bin (if doing so wouldn't violate the per-quantile size cap) or insert a
new one. The per-quantile cap follows t-digest's intuition: bins near the
tails (q close to 0 or 1) must be small for accuracy; bins near the median
can be large.

This is the "TDigestLite" variant from our MVP — simpler than full
Ben-Haim/Dunning but correctness is good enough for percentile estimation
within a few percent.
"""

from __future__ import annotations

import bisect
import threading
from dataclasses import dataclass, field


@dataclass
class _Bin:
    mean: float
    count: int


@dataclass
class TDigest:
    """Quantile sketch with bounded memory.

    Parameters
    ----------
    delta: compression knob. Higher → more bins → more memory + more accuracy.
    """

    delta: float = 100.0
    _bins: list[_Bin] = field(default_factory=list)
    _n: int = 0
    _lock: threading.RLock = field(default_factory=threading.RLock)  # type: ignore[assignment]

    @property
    def max_bins(self) -> int:
        return int(self.delta) * 4

    def add(self, value: float, weight: float = 1.0) -> None:
        with self._lock:
            self._n += int(weight)
            if not self._bins:
                self._bins.append(_Bin(value, int(weight)))
                return
            # Find nearest bin by mean (binary search)
            keys = [b.mean for b in self._bins]
            idx = bisect.bisect_left(keys, value)
            candidates: list[int] = []
            if idx > 0:
                candidates.append(idx - 1)
            if idx < len(self._bins):
                candidates.append(idx)
            nearest_idx = min(candidates, key=lambda i: abs(self._bins[i].mean - value))
            b = self._bins[nearest_idx]
            # Capacity-by-quantile cap
            q = self._cumulative_q(nearest_idx)
            max_count = max(1, int(4 * self._n * q * (1 - q) / self.delta) + 1)
            if b.count + int(weight) <= max_count:
                b.mean = (b.mean * b.count + value * int(weight)) / (b.count + int(weight))
                b.count += int(weight)
            else:
                self._bins.insert(idx, _Bin(value, int(weight)))
            if len(self._bins) > self.max_bins:
                self._compress()

    def _cumulative_q(self, idx: int) -> float:
        """Centred cumulative-q for the bin at index `idx`."""
        cum = 0
        for i, b in enumerate(self._bins):
            if i == idx:
                return (cum + b.count / 2.0) / max(self._n, 1)
            cum += b.count
        return 1.0

    def _compress(self) -> None:
        """Merge adjacent small bins until count drops below max_bins."""
        merged: list[_Bin] = []
        i = 0
        target = self.max_bins // 2
        while i < len(self._bins):
            if (
                i + 1 < len(self._bins)
                and len(merged) + (len(self._bins) - i) > target
                and self._bins[i].count + self._bins[i + 1].count
                <= max(1, int(4 * self._n / self.delta))
            ):
                a, b = self._bins[i], self._bins[i + 1]
                merged.append(_Bin(
                    mean=(a.mean * a.count + b.mean * b.count) / (a.count + b.count),
                    count=a.count + b.count,
                ))
                i += 2
            else:
                merged.append(self._bins[i])
                i += 1
        self._bins = merged

    def quantile(self, q: float) -> float:
        """Linear-interpolated q-quantile, 0 ≤ q ≤ 1."""
        with self._lock:
            if not self._bins:
                return float("nan")
            if q <= 0:
                return self._bins[0].mean
            if q >= 1:
                return self._bins[-1].mean
            target = q * self._n
            cum = 0.0
            for i, b in enumerate(self._bins):
                next_cum = cum + b.count
                # b "covers" the interval [cum, next_cum] in rank-space; its
                # mean sits at cum + count/2.
                centroid_rank = cum + b.count / 2.0
                if target <= centroid_rank and i > 0:
                    prev = self._bins[i - 1]
                    prev_centroid = (cum - prev.count) + prev.count / 2.0
                    if prev_centroid >= target:
                        return prev.mean
                    frac = (target - prev_centroid) / (centroid_rank - prev_centroid)
                    return prev.mean + frac * (b.mean - prev.mean)
                if target <= next_cum:
                    if i + 1 < len(self._bins):
                        nxt = self._bins[i + 1]
                        next_centroid = next_cum + nxt.count / 2.0
                        frac = (target - centroid_rank) / max(
                            next_centroid - centroid_rank, 1e-12
                        )
                        return b.mean + frac * (nxt.mean - b.mean)
                    return b.mean
                cum = next_cum
            return self._bins[-1].mean

    def count(self) -> int:
        with self._lock:
            return self._n

    def memory_centroids(self) -> int:
        with self._lock:
            return len(self._bins)

    def merge(self, other: TDigest) -> None:
        with self._lock:
            for b in other._bins:
                # Re-insert each centroid as its own bin
                self.add(b.mean, weight=b.count)


__all__ = ["TDigest"]
