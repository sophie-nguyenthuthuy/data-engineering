"""Uniform-sampling baseline (Bernoulli + reservoir).

Used as the "negative control" against :class:`SensitivityCoreset`. For
queries with bounded per-row contribution this baseline is unbiased but
its variance scales with the heaviest contributor, which is exactly the
failure mode sensitivity sampling fixes for predicate queries over rare
strata.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

import numpy as np

from aqp.coreset.core import Coreset, WeightedRow

if TYPE_CHECKING:
    from aqp.queries.predicates import Payload


class UniformCoreset:
    """Reservoir-style uniform sample of fixed size ``m``."""

    __slots__ = ("_n", "_reservoir", "_rng", "m")

    def __init__(self, m: int, seed: int | None = 0) -> None:
        if m < 1:
            raise ValueError("m must be ≥ 1")
        self.m = m
        self._rng = np.random.default_rng(seed)
        self._reservoir: list[tuple[float, Payload]] = []
        self._n = 0

    def add(self, value: float, payload: Payload) -> None:
        """Algorithm R reservoir sampling."""
        v = (float(value), tuple(payload))
        self._n += 1
        if len(self._reservoir) < self.m:
            self._reservoir.append(v)
            return
        j = int(self._rng.integers(0, self._n))
        if j < self.m:
            self._reservoir[j] = v

    @property
    def n_rows(self) -> int:
        return self._n

    def finalize(self) -> Coreset:
        if self._n == 0:
            return Coreset.from_list([])
        # Each retained row represents n/m population rows, so weight = n/m.
        m_kept = len(self._reservoir)
        weight = self._n / m_kept
        return Coreset.from_list([WeightedRow(v, p, weight) for v, p in self._reservoir])


__all__ = ["UniformCoreset"]
