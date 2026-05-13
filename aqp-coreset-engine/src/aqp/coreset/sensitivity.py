"""Sensitivity-sampling coreset for SUM/COUNT queries.

Feldman-Langberg "unified framework" (STOC 2011) gives a recipe for
ε-coresets: sample each row with probability proportional to its
*sensitivity* — the maximum normalised contribution of the row to any
query in the class. For sums over predicates with sensitivity bounded
by ``|vᵢ|``, this collapses to importance sampling with probabilities
``|vᵢ| / Σⱼ|vⱼ|``.

Each sampled row is reweighted by its inverse inclusion probability
(Horvitz-Thompson), which gives an unbiased estimator of the
population sum on every predicate.
"""

from __future__ import annotations

import math
from typing import TYPE_CHECKING

import numpy as np

from aqp.coreset.core import Coreset, WeightedRow

if TYPE_CHECKING:
    from aqp.queries.predicates import Payload


class SensitivityCoreset:
    """Offline coreset builder using sensitivity (importance) sampling.

    ``add(value, payload)`` streams rows in; ``finalize()`` produces an
    immutable :class:`Coreset` of size ``m = ⌈(1/ε²)·(vc + log(1/δ))⌉``
    (or the population size when smaller).
    """

    __slots__ = ("_n", "_payloads", "_rng", "_values", "delta", "eps", "vc")

    def __init__(
        self,
        eps: float = 0.05,
        delta: float = 0.01,
        vc: int = 1,
        seed: int | None = 0,
    ) -> None:
        if not 0.0 < eps < 1.0:
            raise ValueError("eps must be in (0, 1)")
        if not 0.0 < delta < 1.0:
            raise ValueError("delta must be in (0, 1)")
        if vc < 1:
            raise ValueError("vc must be ≥ 1")
        self.eps = eps
        self.delta = delta
        self.vc = vc
        self._rng = np.random.default_rng(seed)
        self._values: list[float] = []
        self._payloads: list[Payload] = []
        self._n = 0

    def add(self, value: float, payload: Payload) -> None:
        """Stream one row in."""
        self._values.append(float(value))
        self._payloads.append(tuple(payload))
        self._n += 1

    def add_many(self, rows: list[tuple[float, Payload]]) -> None:
        for v, p in rows:
            self.add(v, p)

    @property
    def n_rows(self) -> int:
        return self._n

    def target_size(self) -> int:
        from aqp.bounds.size import coreset_size_sum

        return coreset_size_sum(self.eps, self.delta, self.vc)

    def finalize(self) -> Coreset:
        if self._n == 0:
            return Coreset.from_list([])
        m = min(self.target_size(), self._n)
        abs_vals = np.abs(np.asarray(self._values, dtype=np.float64))
        total = float(abs_vals.sum())
        if total == 0.0 or m >= self._n:
            # Degenerate (all-zero) or no compression possible → keep all.
            return Coreset.from_list(
                [WeightedRow(v, p, 1.0) for v, p in zip(self._values, self._payloads, strict=True)]
            )
        probs = abs_vals / total
        idx = self._rng.choice(self._n, size=m, replace=True, p=probs)
        rows: list[WeightedRow] = []
        for i in idx:
            i_int = int(i)
            w = 1.0 / (m * probs[i_int])
            rows.append(WeightedRow(self._values[i_int], self._payloads[i_int], w))
        return Coreset.from_list(rows)


def _theoretical_relative_error(m: int, delta: float) -> float:
    """Reverse-solve the Feldman-Langberg bound for current ``m``.

    Not used internally — exported for users who want to print the
    achievable ε after seeing the empirical sample size.
    """
    if m < 1 or not 0.0 < delta < 1.0:
        raise ValueError("invalid arguments")
    return math.sqrt(math.log(1.0 / delta) / m)


__all__ = ["SensitivityCoreset"]
