"""Coreset data structures and query API.

A :class:`Coreset` is an immutable, weighted multiset of rows produced
by one of the builders in :mod:`aqp.coreset`. Queries against a coreset
return unbiased estimates of the corresponding query over the full
data, together with a Gaussian-tail confidence interval whose half-width
shrinks with the coreset's effective sample size.

Estimator
---------

For predicate `p` and weighted rows `(vᵢ, wᵢ)`:

    SUM   estimate = Σ_{i : p(payloadᵢ)} wᵢ · vᵢ
    COUNT estimate = Σ_{i : p(payloadᵢ)} wᵢ

These are unbiased when the row's weight is its inverse inclusion
probability (Horvitz-Thompson). See :class:`SensitivityCoreset` for the
construction.

Confidence interval
-------------------

We compute an empirical variance of the per-row contributions and use a
Gaussian quantile to produce a two-sided interval. This is conservative
for fixed-size sensitivity samples; for asymptotic-regime AQP it is the
standard interval reported by analytics engines.
"""

from __future__ import annotations

import math
from dataclasses import dataclass
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from aqp.queries.predicates import Payload, Predicate


# Common Gaussian z-scores for two-sided CIs.
_Z_SCORES: dict[float, float] = {
    0.90: 1.6449,
    0.95: 1.96,
    0.99: 2.5758,
}


@dataclass(frozen=True, slots=True)
class WeightedRow:
    """Single coreset entry: ``value`` for SUM, ``payload`` for predicate eval."""

    value: float
    payload: Payload
    weight: float

    def __post_init__(self) -> None:
        if self.weight < 0:
            raise ValueError("weight must be non-negative")


@dataclass(frozen=True, slots=True)
class ConfidenceInterval:
    """Two-sided CI ``estimate ± z·σ̂`` at coverage ``level``."""

    estimate: float
    lo: float
    hi: float
    level: float

    def contains(self, value: float) -> bool:
        return self.lo <= value <= self.hi

    def half_width(self) -> float:
        return 0.5 * (self.hi - self.lo)


@dataclass(frozen=True, slots=True)
class Coreset:
    """Immutable weighted-sample coreset over a stream of rows."""

    rows: tuple[WeightedRow, ...]

    @classmethod
    def from_list(cls, rows: list[WeightedRow]) -> Coreset:
        return cls(rows=tuple(rows))

    def __len__(self) -> int:
        return len(self.rows)

    def total_weight(self) -> float:
        return sum(r.weight for r in self.rows)

    # ------------------------------------------------------------------ queries

    def query_count(self, predicate: Predicate | None = None) -> float:
        """Unbiased estimate of ``|{i : p(rowᵢ)}|``."""
        if predicate is None:
            return self.total_weight()
        return sum(r.weight for r in self.rows if predicate(r.payload))

    def query_sum(self, predicate: Predicate | None = None) -> float:
        """Unbiased estimate of ``Σᵢ vᵢ · 1[p(rowᵢ)]``."""
        if predicate is None:
            return sum(r.weight * r.value for r in self.rows)
        return sum(r.weight * r.value for r in self.rows if predicate(r.payload))

    def query_avg(self, predicate: Predicate | None = None) -> float:
        """Ratio estimator: ``SUM(predicate) / COUNT(predicate)``.

        Returns 0.0 when the selected count is zero.
        """
        c = self.query_count(predicate)
        if c == 0.0:
            return 0.0
        return self.query_sum(predicate) / c

    # ------------------------------------------------------------- intervals

    def sum_confidence_interval(
        self, predicate: Predicate | None = None, level: float = 0.95
    ) -> ConfidenceInterval:
        """Confidence interval for the SUM estimator."""
        z = _zscore(level)
        if predicate is None:
            contribs = [r.weight * r.value for r in self.rows]
        else:
            contribs = [r.weight * r.value for r in self.rows if predicate(r.payload)]
        if not contribs:
            return ConfidenceInterval(0.0, 0.0, 0.0, level)
        est = sum(contribs)
        # Variance of a Horvitz-Thompson estimator across the sample: ΣᵢCᵢ² is
        # a tight upper bound that requires no second-order inclusion probs.
        var = sum(c * c for c in contribs)
        sd = math.sqrt(var)
        return ConfidenceInterval(est, est - z * sd, est + z * sd, level)

    def count_confidence_interval(
        self, predicate: Predicate | None = None, level: float = 0.95
    ) -> ConfidenceInterval:
        """Confidence interval for the COUNT estimator."""
        z = _zscore(level)
        if predicate is None:
            weights = [r.weight for r in self.rows]
        else:
            weights = [r.weight for r in self.rows if predicate(r.payload)]
        if not weights:
            return ConfidenceInterval(0.0, 0.0, 0.0, level)
        est = sum(weights)
        var = sum(w * w for w in weights)
        sd = math.sqrt(var)
        return ConfidenceInterval(est, est - z * sd, est + z * sd, level)


def _zscore(level: float) -> float:
    """Look up a two-sided Gaussian z-score for the given coverage."""
    if not 0.0 < level < 1.0:
        raise ValueError("level must be in (0, 1)")
    # Closest standard score; safe for {0.90, 0.95, 0.99} and reasonable elsewhere.
    if level in _Z_SCORES:
        return _Z_SCORES[level]
    # Fall back to an analytic approximation of the inverse normal CDF for the
    # upper (1+level)/2 quantile (Beasley-Springer-Moro style coarse fit).
    p = 0.5 * (1 + level)
    return _inverse_phi(p)


def _inverse_phi(p: float) -> float:
    """Inverse standard-normal CDF, Beasley-Springer-Moro approximation."""
    if not 0.0 < p < 1.0:
        raise ValueError("p must be in (0, 1)")
    # Coefficients from Abramowitz & Stegun 26.2.23 (rational approximation).
    a = (
        -3.969683028665376e01,
        2.209460984245205e02,
        -2.759285104469687e02,
        1.383577518672690e02,
        -3.066479806614716e01,
        2.506628277459239e00,
    )
    b = (
        -5.447609879822406e01,
        1.615858368580409e02,
        -1.556989798598866e02,
        6.680131188771972e01,
        -1.328068155288572e01,
    )
    c = (
        -7.784894002430293e-03,
        -3.223964580411365e-01,
        -2.400758277161838e00,
        -2.549732539343734e00,
        4.374664141464968e00,
        2.938163982698783e00,
    )
    d = (
        7.784695709041462e-03,
        3.224671290700398e-01,
        2.445134137142996e00,
        3.754408661907416e00,
    )
    plow = 0.02425
    phigh = 1.0 - plow
    if p < plow:
        q = math.sqrt(-2 * math.log(p))
        return (((((c[0] * q + c[1]) * q + c[2]) * q + c[3]) * q + c[4]) * q + c[5]) / (
            (((d[0] * q + d[1]) * q + d[2]) * q + d[3]) * q + 1
        )
    if p <= phigh:
        q = p - 0.5
        r = q * q
        return ((((((a[0] * r + a[1]) * r + a[2]) * r + a[3]) * r + a[4]) * r + a[5]) * q) / (
            ((((b[0] * r + b[1]) * r + b[2]) * r + b[3]) * r + b[4]) * r + 1
        )
    q = math.sqrt(-2 * math.log(1 - p))
    return -(((((c[0] * q + c[1]) * q + c[2]) * q + c[3]) * q + c[4]) * q + c[5]) / (
        (((d[0] * q + d[1]) * q + d[2]) * q + d[3]) * q + 1
    )


__all__ = ["ConfidenceInterval", "Coreset", "WeightedRow"]
