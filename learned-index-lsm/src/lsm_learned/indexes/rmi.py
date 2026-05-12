"""
Recursive Model Index (Kraska et al., 2018).

Two-level learned index that approximates the empirical CDF of sorted keys.
Stage-1 routes a query key to one of M stage-2 models; stage-2 predicts the
array position within a tracked error bound so final resolution is a short
binary search rather than a full O(log n) traversal.
"""

from __future__ import annotations

import bisect
from dataclasses import dataclass, field
from typing import Optional

import numpy as np


@dataclass
class _LinearModel:
    """Closed-form ordinary least-squares linear regression."""

    slope: float = 0.0
    intercept: float = 0.0

    def fit(self, keys: np.ndarray, targets: np.ndarray) -> None:
        n = len(keys)
        if n == 0:
            return
        if n == 1:
            self.slope = 0.0
            self.intercept = float(targets[0])
            return
        # Normal equations (numerically stable for moderate n)
        mean_k = keys.mean()
        mean_t = targets.mean()
        denom = float(((keys - mean_k) ** 2).sum())
        if abs(denom) < 1e-12:
            self.slope = 0.0
            self.intercept = mean_t
        else:
            self.slope = float(((keys - mean_k) * (targets - mean_t)).sum() / denom)
            self.intercept = mean_t - self.slope * mean_k

    def predict(self, key: float) -> float:
        return self.slope * key + self.intercept


@dataclass
class RMIStats:
    mean_search_range: float
    max_search_range: float
    p99_search_range: float
    num_stage2_models: int
    coverage: float  # fraction of stage-2 models that received at least one key


class RMI:
    """
    Two-level Recursive Model Index over a static sorted key array.

    Usage::

        rmi = RMI(num_stage2=200)
        rmi.train(sorted_keys)
        pos = rmi.lookup(query_key)   # int index or None
        lo, hi = rmi.search_range(query_key)
    """

    def __init__(self, num_stage2: int = 100) -> None:
        if num_stage2 < 1:
            raise ValueError("num_stage2 must be >= 1")
        self.M = num_stage2
        self._stage1: _LinearModel = _LinearModel()
        self._stage2: list[_LinearModel] = []
        self._min_err: np.ndarray = np.array([], dtype=np.int64)
        self._max_err: np.ndarray = np.array([], dtype=np.int64)
        self._keys: np.ndarray = np.array([], dtype=np.float64)
        self._n: int = 0
        self._trained: bool = False

    # ------------------------------------------------------------------
    # Training
    # ------------------------------------------------------------------

    def train(self, sorted_keys: np.ndarray) -> None:
        """Train both RMI stages on a sorted key array."""
        keys = np.asarray(sorted_keys, dtype=np.float64)
        n = len(keys)
        if n == 0:
            return
        self._keys = keys
        self._n = n
        positions = np.arange(n, dtype=np.float64)

        # Stage 1: key → model index in [0, M)
        stage1_targets = positions / n * self.M
        self._stage1.fit(keys, stage1_targets)

        # Assign each key to a stage-2 model using stage-1 prediction
        raw_preds = self._stage1.slope * keys + self._stage1.intercept
        model_ids = np.clip(raw_preds.astype(np.int64), 0, self.M - 1)

        # Stage 2: per-model linear regression + error bounds
        self._stage2 = [_LinearModel() for _ in range(self.M)]
        self._min_err = np.zeros(self.M, dtype=np.int64)
        self._max_err = np.zeros(self.M, dtype=np.int64)

        for m in range(self.M):
            mask = model_ids == m
            if not mask.any():
                continue
            k_m = keys[mask]
            p_m = positions[mask]
            self._stage2[m].fit(k_m, p_m)
            preds_m = self._stage2[m].slope * k_m + self._stage2[m].intercept
            residuals = p_m - preds_m
            self._min_err[m] = int(np.floor(residuals.min())) - 1
            self._max_err[m] = int(np.ceil(residuals.max())) + 1

        self._trained = True

    # ------------------------------------------------------------------
    # Query
    # ------------------------------------------------------------------

    def search_range(self, key: float) -> tuple[int, int]:
        """Return (lo, hi) index bounds guaranteed to contain *key* if present."""
        if not self._trained:
            raise RuntimeError("RMI has not been trained yet")
        m = int(np.clip(self._stage1.predict(key), 0, self.M - 1))
        pred = self._stage2[m].predict(key)
        lo = int(np.clip(pred + self._min_err[m], 0, self._n - 1))
        hi = int(np.clip(pred + self._max_err[m], 0, self._n - 1))
        if lo > hi:
            lo, hi = hi, lo
        return lo, hi

    def lookup(self, key: float) -> Optional[int]:
        """Return the array index of *key*, or ``None`` if absent."""
        lo, hi = self.search_range(key)
        idx = bisect.bisect_left(self._keys, key, lo, hi + 1)
        if idx <= hi and self._keys[idx] == key:
            return idx
        return None

    def contains(self, key: float) -> bool:
        return self.lookup(key) is not None

    # ------------------------------------------------------------------
    # Diagnostics
    # ------------------------------------------------------------------

    def stats(self) -> RMIStats:
        if not self._trained:
            raise RuntimeError("RMI has not been trained yet")
        ranges = (self._max_err - self._min_err).astype(float)
        covered = int((ranges > 0).sum())
        return RMIStats(
            mean_search_range=float(ranges.mean()),
            max_search_range=float(ranges.max()),
            p99_search_range=float(np.percentile(ranges, 99)),
            num_stage2_models=self.M,
            coverage=covered / self.M,
        )

    @property
    def trained(self) -> bool:
        return self._trained

    @property
    def size(self) -> int:
        return self._n
