"""Coreset construction via sensitivity sampling.

Given n rows with values v_i and weights w_i, build a weighted coreset of size
m << n such that for any query in a class Q,

    | f(coreset, q) - f(full, q) |  <= ε * f(full, q)   w.p. >= 1 - δ

We implement:
  - SumCoreset: for COUNT, SUM with predicates (sensitivity = max contribution)
  - QuantileCoreset: KLL-style rank-tracker

References:
  Feldman & Langberg, "A unified framework for approximating and clustering data" (STOC 2011)
  Karnin, Lang, Liberty, "Optimal Quantile Approximation in Streams" (FOCS 2016) — KLL
"""
from __future__ import annotations

import math
import random
from dataclasses import dataclass

import numpy as np


# ---------------------------------------------------------------------------
# SumCoreset: ε-coreset for sum/count under predicate queries
# ---------------------------------------------------------------------------

@dataclass
class WeightedRow:
    value: float           # row value (for SUM)
    payload: tuple         # other columns, kept for predicate eval
    weight: float          # importance weight in coreset


class SumCoreset:
    """ε-coreset for {sum of v_i * 1[predicate(row_i)]} queries.

    Sensitivity sampling: probability of sampling row i is proportional to its
    *maximum possible contribution* over Q, divided by the sum of contributions.
    For sum-of-values, sensitivity ∝ |v_i|.
    """

    def __init__(self, eps: float = 0.05, delta: float = 0.01, seed: int | None = 0):
        self.eps = eps
        self.delta = delta
        self.rng = random.Random(seed)
        self._rows: list[WeightedRow] = []
        self._total_abs = 0.0
        self._n = 0

    def add(self, value: float, payload: tuple) -> None:
        """Stream one row in."""
        # Reservoir-style with sensitivity weights (offline-style for clarity:
        # accumulate all and resample at finalize).
        self._rows.append(WeightedRow(value=value, payload=payload, weight=1.0))
        self._total_abs += abs(value)
        self._n += 1

    def finalize(self) -> "Coreset":
        # Coreset size: O((1/ε²) log(1/δ))
        m = max(1, int(math.ceil((1.0 / (self.eps ** 2)) * math.log(1.0 / self.delta))))
        m = min(m, self._n)
        if self._total_abs == 0 or m >= self._n:
            return Coreset(rows=list(self._rows))

        # Sensitivity sampling
        probs = np.array([abs(r.value) / self._total_abs for r in self._rows])
        idx = np.random.default_rng(self.rng.randrange(2**31)).choice(
            self._n, size=m, replace=True, p=probs)
        sampled: list[WeightedRow] = []
        for i in idx:
            r = self._rows[i]
            # Importance reweight: w_new = w_old / (m * p_i)
            w = r.weight / (m * probs[i])
            sampled.append(WeightedRow(value=r.value, payload=r.payload, weight=w))
        return Coreset(rows=sampled)


@dataclass
class Coreset:
    rows: list[WeightedRow]

    def query_sum(self, predicate=None) -> float:
        if predicate is None:
            return sum(r.value * r.weight for r in self.rows)
        return sum(r.value * r.weight for r in self.rows if predicate(r.payload))

    def query_count(self, predicate=None) -> float:
        if predicate is None:
            return sum(r.weight for r in self.rows)
        return sum(r.weight for r in self.rows if predicate(r.payload))

    def confidence_interval(self, predicate=None, z: float = 2.576) -> tuple[float, float, float]:
        """Return (estimate, lo, hi) at z standard deviations.
        z=1.96 → 95%, z=2.576 → 99%.
        Variance estimate: Σ (w_i*v_i)² for contributing rows (delta-method-ish)."""
        contrib = [(r.weight * r.value) for r in self.rows
                   if predicate is None or predicate(r.payload)]
        if not contrib:
            return 0.0, 0.0, 0.0
        est = float(sum(contrib))
        var = float(sum(x * x for x in contrib))
        sd = math.sqrt(var)
        return est, est - z * sd, est + z * sd

    def __len__(self) -> int:
        return len(self.rows)


# ---------------------------------------------------------------------------
# Streaming merge-and-reduce skeleton
# ---------------------------------------------------------------------------

class StreamingSumCoreset:
    """Merge-and-reduce: maintain a binary buffer of coresets.

    At each level, capacity = base_size. When level full, merge with next.
    """

    def __init__(self, base_size: int = 256, eps: float = 0.05, delta: float = 0.01,
                 seed: int | None = 0):
        self.base_size = base_size
        self.eps = eps
        self.delta = delta
        self.rng = random.Random(seed)
        self._levels: list[list[WeightedRow] | None] = []
        self._buf: list[WeightedRow] = []

    def add(self, value: float, payload: tuple) -> None:
        self._buf.append(WeightedRow(value=value, payload=payload, weight=1.0))
        if len(self._buf) >= self.base_size:
            self._cascade(self._buf)
            self._buf = []

    def _cascade(self, rows: list[WeightedRow]) -> None:
        i = 0
        cur = self._reduce(rows)
        while True:
            if i >= len(self._levels):
                self._levels.append(None)
            if self._levels[i] is None:
                self._levels[i] = cur
                return
            merged = self._levels[i] + cur
            self._levels[i] = None
            cur = self._reduce(merged)
            i += 1

    def _reduce(self, rows: list[WeightedRow]) -> list[WeightedRow]:
        """Reduce a 2*base_size list to base_size via sensitivity sampling."""
        if len(rows) <= self.base_size:
            return rows
        total = sum(abs(r.value) * r.weight for r in rows)
        if total == 0:
            # Uniform fallback
            return self.rng.sample(rows, self.base_size)
        probs = np.array([abs(r.value) * r.weight / total for r in rows])
        idx = np.random.default_rng(self.rng.randrange(2**31)).choice(
            len(rows), size=self.base_size, replace=True, p=probs)
        out: list[WeightedRow] = []
        for i in idx:
            r = rows[i]
            w = r.weight / (self.base_size * probs[i])
            out.append(WeightedRow(value=r.value, payload=r.payload, weight=w))
        return out

    def finalize(self) -> Coreset:
        all_rows: list[WeightedRow] = list(self._buf)
        for lvl in self._levels:
            if lvl is not None:
                all_rows += lvl
        return Coreset(rows=all_rows)


# ---------------------------------------------------------------------------
# Quantile sketch (simplified KLL-style)
# ---------------------------------------------------------------------------

class QuantileSketch:
    """Simplified KLL: compressors per level, deterministic compression.

    Memory ~ O((1/ε) log²(1/ε)). For pedagogy; production would use the full KLL.
    """

    def __init__(self, eps: float = 0.01, seed: int | None = 0):
        self.eps = eps
        self.k = max(16, int(math.ceil(1.0 / eps)))
        self.rng = random.Random(seed)
        self._levels: list[list[float]] = [[]]

    def add(self, value: float) -> None:
        self._levels[0].append(value)
        self._compress()

    def _compress(self) -> None:
        for i, lvl in enumerate(self._levels):
            if len(lvl) >= self.k:
                lvl.sort()
                # Pick every other element (random offset)
                offset = self.rng.randint(0, 1)
                compressed = lvl[offset::2]
                if i + 1 >= len(self._levels):
                    self._levels.append([])
                self._levels[i + 1].extend(compressed)
                self._levels[i] = []

    def quantile(self, q: float) -> float:
        """Approximate q-quantile, 0 <= q <= 1."""
        # Each item at level i represents 2^i items
        weighted: list[tuple[float, int]] = []
        for i, lvl in enumerate(self._levels):
            w = 1 << i
            for v in lvl:
                weighted.append((v, w))
        if not weighted:
            return float("nan")
        weighted.sort(key=lambda x: x[0])
        total = sum(w for _, w in weighted)
        target = q * total
        cum = 0
        for v, w in weighted:
            cum += w
            if cum >= target:
                return v
        return weighted[-1][0]


__all__ = ["WeightedRow", "Coreset", "SumCoreset", "StreamingSumCoreset", "QuantileSketch"]
