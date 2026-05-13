"""Balle et al. (CRYPTO 2019) shuffle-amplification analyzer.

Given n users each ε₀-LDP, derive (ε, δ)-central-DP for the shuffled
output. Uses the Erlingsson et al. (SODA 2019) bound:

    ε ≈ 8 * ε₀ * sqrt( e^{ε₀} * log(4/δ) / n )

Valid in the small-ε₀ regime; outside it we conservatively cap at ε₀.
"""

from __future__ import annotations

import math
from dataclasses import dataclass


@dataclass(frozen=True, slots=True)
class ShuffleBound:
    eps0: float
    n: int
    delta: float
    eps_central: float
    amplification: float


def shuffle_amplification(eps0: float, n: int, delta: float = 1e-6) -> ShuffleBound:
    """Compute the central (ε, δ)-DP guarantee given n ε₀-LDP users + shuffler."""
    if eps0 <= 0:
        raise ValueError("eps0 must be > 0")
    if n < 1:
        raise ValueError("n must be ≥ 1")
    if not 0 < delta < 1:
        raise ValueError("delta must be in (0, 1)")
    if n < 4:
        return ShuffleBound(eps0=eps0, n=n, delta=delta, eps_central=eps0, amplification=1.0)
    bound = 8.0 * eps0 * math.sqrt(math.exp(eps0) * math.log(4.0 / delta) / n)
    eps_central = min(eps0, bound)
    return ShuffleBound(
        eps0=eps0,
        n=n,
        delta=delta,
        eps_central=eps_central,
        amplification=eps0 / max(eps_central, 1e-12),
    )


def required_eps0_for_target(eps_target: float, n: int, delta: float = 1e-6) -> float:
    """Inverse: given target central ε, find the max ε₀ that achieves it."""
    if eps_target <= 0:
        raise ValueError("eps_target must be > 0")
    lo, hi = 1e-6, 20.0
    for _ in range(100):
        mid = (lo + hi) / 2
        b = shuffle_amplification(mid, n, delta)
        if b.eps_central > eps_target:
            hi = mid
        else:
            lo = mid
    return lo


__all__ = ["ShuffleBound", "required_eps0_for_target", "shuffle_amplification"]
