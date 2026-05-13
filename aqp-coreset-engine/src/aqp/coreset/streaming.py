"""Streaming SUM coreset via merge-and-reduce.

Maintains a logarithmic stack of fixed-size coresets. When a fresh
batch of ``base_size`` rows arrives we run sensitivity sampling on it;
when two coresets at the same "level" collide we sample again from
their concatenation. Both ε and δ compound by ``log₂(n / base_size)``
in the worst case — controlled by choosing ``base_size`` ≈ √n and
treating the bound as informative rather than tight.

Equivalent in spirit to Bagchi-Chaudhuri-Indyk-Mitzenmacher (PODS 2006)
applied to sums; the implementation is intentionally simple to keep the
correctness story auditable.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

import numpy as np

from aqp.coreset.core import Coreset, WeightedRow

if TYPE_CHECKING:
    from aqp.queries.predicates import Payload


class StreamingSumCoreset:
    """Merge-and-reduce streaming coreset of fixed per-level size."""

    __slots__ = ("_buf", "_levels", "_n", "_rng", "base_size")

    def __init__(self, base_size: int = 256, seed: int | None = 0) -> None:
        if base_size < 2:
            raise ValueError("base_size must be ≥ 2")
        self.base_size = base_size
        self._rng = np.random.default_rng(seed)
        self._levels: list[list[WeightedRow] | None] = []
        self._buf: list[WeightedRow] = []
        self._n = 0

    @property
    def n_rows(self) -> int:
        return self._n

    @property
    def n_levels(self) -> int:
        return len(self._levels)

    def add(self, value: float, payload: Payload) -> None:
        self._buf.append(WeightedRow(float(value), tuple(payload), 1.0))
        self._n += 1
        if len(self._buf) >= self.base_size:
            self._cascade(self._buf)
            self._buf = []

    # ------------------------------------------------------------------ guts

    def _cascade(self, rows: list[WeightedRow]) -> None:
        cur = self._reduce(rows)
        i = 0
        while True:
            if i >= len(self._levels):
                self._levels.append(None)
            if self._levels[i] is None:
                self._levels[i] = cur
                return
            level_i = self._levels[i]
            assert level_i is not None  # narrow for mypy
            merged = level_i + cur
            self._levels[i] = None
            cur = self._reduce(merged)
            i += 1

    def _reduce(self, rows: list[WeightedRow]) -> list[WeightedRow]:
        """Importance-sample down to ``base_size`` rows."""
        if len(rows) <= self.base_size:
            return rows
        contrib = np.array([abs(r.value) * r.weight for r in rows], dtype=np.float64)
        total = float(contrib.sum())
        if total == 0.0:
            # All-zero contribution → uniform downsample.
            idx_uni = self._rng.choice(len(rows), size=self.base_size, replace=False)
            return [rows[int(i)] for i in idx_uni]
        probs = contrib / total
        idx = self._rng.choice(len(rows), size=self.base_size, replace=True, p=probs)
        out: list[WeightedRow] = []
        for j in idx:
            j_int = int(j)
            r = rows[j_int]
            w = r.weight / (self.base_size * probs[j_int])
            out.append(WeightedRow(r.value, r.payload, w))
        return out

    def finalize(self) -> Coreset:
        """Concatenate all levels (and the tail buffer) into one coreset."""
        rows: list[WeightedRow] = list(self._buf)
        for lvl in self._levels:
            if lvl is not None:
                rows.extend(lvl)
        return Coreset.from_list(rows)


__all__ = ["StreamingSumCoreset"]
