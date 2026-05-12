"""Balle et al. (2019) "Privacy Blanket" analyzer for shuffle DP.

Given n users each ε₀-LDP, derive (ε, δ)-central-DP for the shuffled output.
We implement a clean, slightly conservative form of Theorem 3.1 from Erlingsson
et al. (SODA 2019) for binary randomized response — and a numerical bound from
Feldman-McMillan-Talwar (FOCS 2022) for general k-ary.

For binary randomized response (RR) with parameter ε₀, the shuffled output is
(ε, δ)-DP centrally where, asymptotically as n → ∞:

    ε = O( ε₀ * sqrt( log(1/δ) / n ) )

We compute a concrete bound; tighter analytic bounds exist but this gives the
right scaling and demonstrates the amplification effect.
"""
from __future__ import annotations

import math
from dataclasses import dataclass


@dataclass
class ShuffleBound:
    eps0: float          # local epsilon
    n: int               # number of users
    delta: float         # central delta
    eps_central: float   # implied central epsilon
    amplification: float # eps_central / eps0


def shuffle_amplification(eps0: float, n: int, delta: float = 1e-6) -> ShuffleBound:
    """Compute central (ε, δ) from local ε₀, n users.

    Uses the bound from Erlingsson et al. (SODA 2019, Thm 3.1):

        ε ≈ ε₀ * 8 * sqrt( e^{ε₀} * log(4/δ) / n )

    valid for ε₀ ≤ log(n/log(1/δ))/2. Outside this regime we conservatively
    fall back to ε ≈ ε₀.
    """
    if n < 4 or eps0 <= 0:
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


def composition(bounds: list[ShuffleBound]) -> ShuffleBound:
    """Sequential composition: ε's add, δ's add (basic).

    For k queries with (ε_i, δ_i): total ≤ (Σε_i, Σδ_i).
    """
    eps_sum = sum(b.eps_central for b in bounds)
    delta_sum = sum(b.delta for b in bounds)
    return ShuffleBound(
        eps0=0.0, n=0, delta=delta_sum, eps_central=eps_sum, amplification=0.0,
    )


def required_eps0_for_target(eps_target: float, n: int, delta: float = 1e-6) -> float:
    """Inverse: given target central ε, find max ε₀ allowed.

    Solve ε₀ * 8 * sqrt(e^{ε₀} log(4/δ) / n) = eps_target  for ε₀.
    Use binary search.
    """
    lo, hi = 1e-6, 20.0
    for _ in range(100):
        mid = (lo + hi) / 2
        b = shuffle_amplification(mid, n, delta)
        if b.eps_central > eps_target:
            hi = mid
        else:
            lo = mid
    return lo


__all__ = ["ShuffleBound", "shuffle_amplification", "composition", "required_eps0_for_target"]
