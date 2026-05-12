"""Per-task duration forecaster.

Maintains a running estimate of each task's runtime distribution. We use a
lognormal model fit via online MLE on log-durations.
"""
from __future__ import annotations

import math
from collections import defaultdict
from dataclasses import dataclass, field


@dataclass
class TaskStats:
    n: int = 0
    sum_log: float = 0.0
    sum_log_sq: float = 0.0

    def add(self, duration: float) -> None:
        if duration <= 0:
            return
        l = math.log(duration)
        self.n += 1
        self.sum_log += l
        self.sum_log_sq += l * l

    @property
    def mu(self) -> float:
        return self.sum_log / self.n if self.n else 0.0

    @property
    def sigma(self) -> float:
        if self.n < 2:
            return 0.0
        m = self.mu
        var = self.sum_log_sq / self.n - m * m
        return math.sqrt(max(var, 0.0))

    def quantile(self, q: float) -> float:
        """q-quantile of duration (e.g., 0.95 → p95)."""
        if self.n == 0:
            return 0.0
        # Standard normal inverse CDF approximation (Acklam)
        z = _ndtri(q)
        return math.exp(self.mu + self.sigma * z)


def _ndtri(p: float) -> float:
    """Inverse of standard normal CDF (Acklam approximation)."""
    if p <= 0 or p >= 1:
        raise ValueError("p must be in (0,1)")
    # Coefficients
    a = [-3.969683028665376e+01, 2.209460984245205e+02, -2.759285104469687e+02,
         1.383577518672690e+02, -3.066479806614716e+01, 2.506628277459239e+00]
    b = [-5.447609879822406e+01, 1.615858368580409e+02, -1.556989798598866e+02,
         6.680131188771972e+01, -1.328068155288572e+01]
    c = [-7.784894002430293e-03, -3.223964580411365e-01, -2.400758277161838e+00,
         -2.549732539343734e+00, 4.374664141464968e+00, 2.938163982698783e+00]
    d = [7.784695709041462e-03, 3.224671290700398e-01, 2.445134137142996e+00,
         3.754408661907416e+00]
    pl = 0.02425
    if p < pl:
        q = math.sqrt(-2 * math.log(p))
        return (((((c[0]*q + c[1])*q + c[2])*q + c[3])*q + c[4])*q + c[5]) / \
               ((((d[0]*q + d[1])*q + d[2])*q + d[3])*q + 1)
    if p > 1 - pl:
        q = math.sqrt(-2 * math.log(1 - p))
        return -(((((c[0]*q + c[1])*q + c[2])*q + c[3])*q + c[4])*q + c[5]) / \
                ((((d[0]*q + d[1])*q + d[2])*q + d[3])*q + 1)
    q = p - 0.5
    r = q*q
    return (((((a[0]*r + a[1])*r + a[2])*r + a[3])*r + a[4])*r + a[5]) * q / \
           (((((b[0]*r + b[1])*r + b[2])*r + b[3])*r + b[4])*r + 1)


@dataclass
class Forecaster:
    _stats: dict = field(default_factory=lambda: defaultdict(TaskStats))

    def observe(self, task: str, duration: float) -> None:
        self._stats[task].add(duration)

    def mean(self, task: str) -> float:
        s = self._stats[task]
        if s.n == 0:
            return 1.0
        return math.exp(s.mu + 0.5 * s.sigma * s.sigma)

    def p95(self, task: str) -> float:
        return self._stats[task].quantile(0.95)


__all__ = ["TaskStats", "Forecaster"]
