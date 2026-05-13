"""KLL quantile sketch (Karnin-Lang-Liberty, FOCS 2016).

Maintains a stack of *compactors*. Level ``i`` holds at most ``k``
items; each item carries weight ``2^i``. When a level fills, a random
half of its items is promoted to the next level (the other half is
dropped). Rank queries use the weighted multiset.

Worst-case relative error is ``O(1 / sqrt(k))``; choose
``k = ⌈1 / ε⌉`` for ε-accurate quantiles.
"""

from __future__ import annotations

import math

import numpy as np


class KLLSketch:
    """Simplified KLL sketch with deterministic compactor capacities."""

    __slots__ = ("_levels", "_n", "_rng", "k")

    def __init__(self, k: int = 200, seed: int | None = 0) -> None:
        if k < 8:
            raise ValueError("k must be ≥ 8 for a meaningful sketch")
        self.k = k
        self._rng = np.random.default_rng(seed)
        self._levels: list[list[float]] = [[]]
        self._n = 0

    @classmethod
    def for_epsilon(cls, eps: float, seed: int | None = 0) -> KLLSketch:
        """Pick ``k`` to target relative quantile error ``eps``."""
        if not 0.0 < eps < 1.0:
            raise ValueError("eps must be in (0, 1)")
        k = max(8, math.ceil(1.0 / eps))
        return cls(k=k, seed=seed)

    @property
    def n(self) -> int:
        return self._n

    def add(self, value: float) -> None:
        self._levels[0].append(float(value))
        self._n += 1
        self._compress()

    def _compress(self) -> None:
        for i in range(len(self._levels)):
            if len(self._levels[i]) < self.k:
                continue
            self._levels[i].sort()
            # Random parity: pick every-other item starting at 0 or 1.
            offset = int(self._rng.integers(0, 2))
            promoted = self._levels[i][offset::2]
            self._levels[i] = []
            if i + 1 >= len(self._levels):
                self._levels.append([])
            self._levels[i + 1].extend(promoted)

    # -------------------------------------------------------------- queries

    def quantile(self, q: float) -> float:
        """Return the value at rank ``q ∈ [0, 1]``."""
        if not 0.0 <= q <= 1.0:
            raise ValueError("q must be in [0, 1]")
        if self._n == 0:
            raise ValueError("sketch is empty")
        weighted: list[tuple[float, int]] = []
        for i, lvl in enumerate(self._levels):
            w = 1 << i
            for v in lvl:
                weighted.append((v, w))
        weighted.sort(key=lambda vw: vw[0])
        total = sum(w for _, w in weighted)
        target = q * total
        cum = 0
        for v, w in weighted:
            cum += w
            if cum >= target:
                return v
        return weighted[-1][0]

    def rank(self, value: float) -> float:
        """Approximate ``Pr[X ≤ value]`` (CDF) under the sketch."""
        if self._n == 0:
            raise ValueError("sketch is empty")
        below = 0
        total = 0
        for i, lvl in enumerate(self._levels):
            w = 1 << i
            for v in lvl:
                total += w
                if v <= value:
                    below += w
        return below / total

    def merge(self, other: KLLSketch) -> KLLSketch:
        """Merge two sketches into a new one (associative for the same ``k``).

        Both inputs are left unchanged.
        """
        if self.k != other.k:
            raise ValueError("cannot merge sketches with different k")
        out = KLLSketch(k=self.k, seed=int(self._rng.integers(0, 2**31)))
        out._levels = [list(lvl) for lvl in self._levels]
        # Pad with empty levels if other is taller.
        while len(out._levels) < len(other._levels):
            out._levels.append([])
        for i, lvl in enumerate(other._levels):
            out._levels[i].extend(lvl)
        out._n = self._n + other._n
        # Trigger compression where merged levels overflowed.
        out._compress()
        return out


__all__ = ["KLLSketch"]
