"""Online lognormal fitter via Welford's algorithm on log-values.

If X ~ Lognormal(μ, σ), then log(X) ~ Normal(μ, σ²). We maintain a
streaming (μ, σ²) over log(values) — Welford guarantees numerical stability.
"""

from __future__ import annotations

import math
import threading
from dataclasses import dataclass, field


@dataclass
class LognormalFitter:
    _n: int = 0
    _mu: float = 0.0
    _m2: float = 0.0  # sum of squared deviations from mean
    _lock: threading.RLock = field(default_factory=threading.RLock)  # type: ignore[assignment]

    def observe(self, value: float) -> None:
        if value <= 0:
            return
        with self._lock:
            x = math.log(value)
            self._n += 1
            delta = x - self._mu
            self._mu += delta / self._n
            delta2 = x - self._mu
            self._m2 += delta * delta2

    @property
    def n(self) -> int:
        with self._lock:
            return self._n

    @property
    def mu(self) -> float:
        with self._lock:
            return self._mu

    @property
    def sigma(self) -> float:
        with self._lock:
            if self._n < 2:
                return 0.0
            return math.sqrt(self._m2 / (self._n - 1))

    def quantile(self, q: float) -> float:
        """q-quantile via inverse normal."""
        if not 0 < q < 1:
            raise ValueError("q must be in (0, 1)")
        with self._lock:
            mu, sigma = self._mu, self.sigma
        z = _inv_norm_cdf(q)
        return math.exp(mu + sigma * z)


def _inv_norm_cdf(p: float) -> float:
    """Beasley-Springer-Moro approximation of the inverse normal CDF."""
    if not 0 < p < 1:
        raise ValueError("p must be in (0,1)")
    a = [-3.969683028665376e+01,  2.209460984245205e+02,
         -2.759285104469687e+02,  1.383577518672690e+02,
         -3.066479806614716e+01,  2.506628277459239e+00]
    b = [-5.447609879822406e+01,  1.615858368580409e+02,
         -1.556989798598866e+02,  6.680131188771972e+01,
         -1.328068155288572e+01]
    c = [-7.784894002430293e-03, -3.223964580411365e-01,
         -2.400758277161838e+00, -2.549732539343734e+00,
          4.374664141464968e+00,  2.938163982698783e+00]
    d = [ 7.784695709041462e-03,  3.224671290700398e-01,
          2.445134137142996e+00,  3.754408661907416e+00]
    p_low = 0.02425
    if p < p_low:
        q = math.sqrt(-2 * math.log(p))
        return (((((c[0]*q+c[1])*q+c[2])*q+c[3])*q+c[4])*q+c[5]) / \
               ((((d[0]*q+d[1])*q+d[2])*q+d[3])*q+1)
    if p > 1 - p_low:
        q = math.sqrt(-2 * math.log(1 - p))
        return -(((((c[0]*q+c[1])*q+c[2])*q+c[3])*q+c[4])*q+c[5]) / \
                ((((d[0]*q+d[1])*q+d[2])*q+d[3])*q+1)
    q = p - 0.5
    r = q*q
    return (((((a[0]*r+a[1])*r+a[2])*r+a[3])*r+a[4])*r+a[5]) * q / \
           (((((b[0]*r+b[1])*r+b[2])*r+b[3])*r+b[4])*r+1)
