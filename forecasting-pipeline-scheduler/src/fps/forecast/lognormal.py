"""Per-task lognormal duration forecaster.

For each task id we maintain running sums sufficient to recover the MLE
of a ``LogNormal(μ, σ²)`` model on observed durations:

    μ̂ = (1/n) Σ ln dᵢ
    σ̂² = (1/n) Σ (ln dᵢ − μ̂)²        (biased MLE, fine for n > 50)

From these we expose:

  * :meth:`mean` — ``exp(μ̂ + σ̂² / 2)`` (the lognormal mean).
  * :meth:`quantile` — uses an Acklam-style inverse-Φ approximation.
  * :meth:`p95` — convenience shortcut for ``quantile(0.95)``.

Non-positive observations are ignored (they would diverge under ``log``).
"""

from __future__ import annotations

import math
from collections import defaultdict
from dataclasses import dataclass, field


@dataclass
class TaskStats:
    """Running sufficient statistics for a single task's log-durations."""

    n: int = 0
    sum_log: float = 0.0
    sum_log_sq: float = 0.0

    def add(self, duration: float) -> None:
        if duration <= 0:
            return
        ln = math.log(duration)
        self.n += 1
        self.sum_log += ln
        self.sum_log_sq += ln * ln

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
        if not 0.0 < q < 1.0:
            raise ValueError("q must be in (0, 1)")
        if self.n == 0:
            return 0.0
        return math.exp(self.mu + self.sigma * _ndtri(q))

    def reset(self) -> None:
        self.n = 0
        self.sum_log = 0.0
        self.sum_log_sq = 0.0


def _ndtri(p: float) -> float:
    """Acklam's inverse-Φ approximation (relative error ≤ 1.15e-9)."""
    if not 0.0 < p < 1.0:
        raise ValueError("p must be in (0, 1)")
    a = (
        -3.969683028665376e1,
        2.209460984245205e2,
        -2.759285104469687e2,
        1.383577518672690e2,
        -3.066479806614716e1,
        2.506628277459239e0,
    )
    b = (
        -5.447609879822406e1,
        1.615858368580409e2,
        -1.556989798598866e2,
        6.680131188771972e1,
        -1.328068155288572e1,
    )
    c = (
        -7.784894002430293e-3,
        -3.223964580411365e-1,
        -2.400758277161838e0,
        -2.549732539343734e0,
        4.374664141464968e0,
        2.938163982698783e0,
    )
    d = (
        7.784695709041462e-3,
        3.224671290700398e-1,
        2.445134137142996e0,
        3.754408661907416e0,
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


@dataclass
class LognormalForecaster:
    """Per-task lognormal forecaster."""

    _stats: dict[str, TaskStats] = field(default_factory=lambda: defaultdict(TaskStats))

    def observe(self, task: str, duration: float) -> None:
        if not task:
            raise ValueError("task must be non-empty")
        self._stats[task].add(duration)

    def stats(self, task: str) -> TaskStats:
        return self._stats[task]

    def mean(self, task: str, default: float = 1.0) -> float:
        s = self._stats[task]
        if s.n == 0:
            return default
        return math.exp(s.mu + 0.5 * s.sigma * s.sigma)

    def quantile(self, task: str, q: float, default: float = 1.0) -> float:
        s = self._stats[task]
        if s.n == 0:
            return default
        return s.quantile(q)

    def p95(self, task: str, default: float = 1.0) -> float:
        return self.quantile(task, 0.95, default=default)

    def reset(self, task: str) -> None:
        if task in self._stats:
            self._stats[task].reset()


__all__ = ["LognormalForecaster", "TaskStats"]
