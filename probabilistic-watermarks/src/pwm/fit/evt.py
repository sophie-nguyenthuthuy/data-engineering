"""Extreme-value theory: Peaks-Over-Threshold + Generalised Pareto Distribution.

For the heavy tail of arrival delays, classical parametric fits (lognormal,
Weibull) often under-estimate. EVT fits a GPD only to exceedances above a
high threshold u; the GPD parameters (ξ, σ) describe the tail.

We use method-of-moments for fast online estimation. For a true online fit,
production systems use stochastic-gradient MLE.
"""

from __future__ import annotations

import math
import threading
from dataclasses import dataclass, field


@dataclass
class POTFitter:
    """Peaks-Over-Threshold fitter using method of moments."""

    threshold: float = 0.0           # set automatically once n > burn_in
    burn_in: int = 200
    _exceedances: list[float] = field(default_factory=list)
    _seen: list[float] = field(default_factory=list)
    _lock: threading.RLock = field(default_factory=threading.RLock)  # type: ignore[assignment]
    _xi: float = 0.0                 # shape
    _sigma: float = 1.0              # scale

    def observe(self, value: float) -> None:
        with self._lock:
            self._seen.append(value)
            if len(self._seen) == self.burn_in and self.threshold == 0.0:
                # Pick threshold as 90th percentile of observed values
                self.threshold = float(_percentile(sorted(self._seen), 0.9))
            if value > self.threshold and self.threshold > 0.0:
                self._exceedances.append(value - self.threshold)
                self._refit()

    def _refit(self) -> None:
        if len(self._exceedances) < 3:
            return
        # Method of moments for GPD(ξ, σ):
        # E[X-u | X > u] = σ / (1 - ξ)         (mean of excess)
        # Var[X-u | X > u] = σ² / ((1-ξ)² (1-2ξ))
        n = len(self._exceedances)
        mean = sum(self._exceedances) / n
        var = sum((x - mean) ** 2 for x in self._exceedances) / max(n - 1, 1)
        if var <= 0:
            return
        ratio = mean * mean / var
        xi = 0.5 * (1.0 - ratio)
        sigma = mean * (1.0 - xi)
        if sigma > 0:
            self._xi = max(-0.5, min(xi, 0.5))
            self._sigma = sigma

    @property
    def xi(self) -> float:
        with self._lock:
            return self._xi

    @property
    def sigma(self) -> float:
        with self._lock:
            return self._sigma

    def quantile(self, q: float) -> float:
        """Return the q-quantile of the FULL distribution (not just the tail).

        For q below the threshold quantile, return interpolated empirical.
        For q above, use the GPD inverse:
            X(q) = u + (σ/ξ) * [((1-q')^(-ξ)) - 1]
        where q' = (q - q_u) / (1 - q_u),  q_u = threshold's empirical rank.
        """
        with self._lock:
            if not self._seen:
                return float("nan")
            sorted_seen = sorted(self._seen)
            n = len(sorted_seen)
            q_threshold = (
                _empirical_rank(sorted_seen, self.threshold)
                if self.threshold > 0.0 else 0.0
            )
            if q < q_threshold:
                # Empirical
                idx = max(0, min(int(q * n), n - 1))
                return float(sorted_seen[idx])
            # Tail via GPD
            if q_threshold >= 1.0:
                return float(sorted_seen[-1])
            qp = (q - q_threshold) / (1.0 - q_threshold)
            if abs(self._xi) < 1e-9:
                return float(self.threshold + self._sigma * (-math.log(1.0 - qp)))
            return float(self.threshold + (self._sigma / self._xi) *
                         (((1.0 - qp) ** (-self._xi)) - 1.0))


def _percentile(sorted_values: list[float], p: float) -> float:
    n = len(sorted_values)
    if n == 0:
        return 0.0
    idx = max(0, min(int(p * n), n - 1))
    return sorted_values[idx]


def _empirical_rank(sorted_values: list[float], target: float) -> float:
    """Fraction of values ≤ target."""
    n = len(sorted_values)
    if n == 0:
        return 0.0
    from bisect import bisect_right
    return bisect_right(sorted_values, target) / n
