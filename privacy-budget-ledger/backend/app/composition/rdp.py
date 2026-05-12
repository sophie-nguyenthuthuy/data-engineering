"""
Rényi Differential Privacy (RDP) accounting.

Reference: Mironov (2017) "Rényi Differential Privacy of the Gaussian Mechanism"
           https://arxiv.org/abs/1702.07476

Key facts:
  - A mechanism M satisfies (α, ε)-RDP if  D_α(M(x) ‖ M(x')) ≤ ε  for all adj. x, x'
  - Composition: (α, ε₁)-RDP + (α, ε₂)-RDP  →  (α, ε₁+ε₂)-RDP
  - To (ε,δ)-DP conversion: (α, ε_rdp) → (ε_rdp + log(1/δ)/(α-1), δ)-DP  ∀δ∈(0,1)
  - Tighter conversion (Balle et al. 2020):
      ε_dp = ε_rdp + log((α-1)/α) - (log(δ) + log(α))/(α-1)

We track budgets over a fine grid of α orders so the best conversion is always found.
"""
from __future__ import annotations

import math
from typing import List, Tuple

# Standard α grid used throughout the library.
ALPHA_ORDERS: List[float] = [
    1.01, 1.1, 1.25, 1.5, 1.75,
    2.0, 2.5, 3.0, 4.0, 5.0,
    6.0, 8.0, 16.0, 32.0, 64.0, 1e6,
]


# ---------------------------------------------------------------------------
# Per-mechanism RDP ε at a given α
# ---------------------------------------------------------------------------

def rdp_gaussian(sensitivity: float, sigma: float, alpha: float) -> float:
    """
    (α, ε)-RDP for the Gaussian mechanism with noise std σ and l₂-sensitivity Δ.
    ε(α) = α · Δ² / (2σ²)   [Proposition 3, Mironov 2017]
    """
    if alpha == 1.0:
        return sensitivity ** 2 / (2 * sigma ** 2)  # limit as α→1
    return alpha * sensitivity ** 2 / (2 * sigma ** 2)


def rdp_laplace(sensitivity: float, b: float, alpha: float) -> float:
    """
    (α, ε)-RDP for the Laplace mechanism with noise scale b and l₁-sensitivity Δ.
    ε_L(α) = 1/(α-1) · log[ α/(2α-1)·exp((α-1)·Δ/b)  +  (α-1)/(2α-1)·exp(-α·Δ/b) ]
    [Proposition 3, Wang et al. 2019]

    Special handling for α=1 (limit):  ε_L(1) = (e^s - s - 1) / something...
    We use a numerically stable version and fall back to the Gaussian bound when α→∞.
    """
    if b <= 0:
        raise ValueError("Laplace scale b must be positive")
    s = sensitivity / b
    if alpha == 1.0:
        # α→1 limit via L'Hôpital: equals the KL divergence of Lap(0,b) ‖ Lap(s,b)
        return math.exp(s) - s - 1
    if math.isinf(alpha):
        # α→∞ limit: max log-likelihood ratio = s
        return s

    a = alpha
    try:
        log_term = math.log(
            (a / (2 * a - 1)) * math.exp((a - 1) * s)
            + ((a - 1) / (2 * a - 1)) * math.exp(-a * s)
        )
    except OverflowError:
        # For large α or large s, dominating term is (a-1)*s
        log_term = (a - 1) * s
    return log_term / (a - 1)


# ---------------------------------------------------------------------------
# RDP → (ε, δ)-DP conversion
# ---------------------------------------------------------------------------

def rdp_to_dp(rdp_epsilon: float, alpha: float, delta: float) -> float:
    """
    Convert (α, ε_rdp)-RDP to (ε_dp, δ)-DP using the Balle et al. 2020
    tighter bound:
        ε_dp = ε_rdp + log((α-1)/α) - (log(δ) + log(α)) / (α-1)

    Falls back to the simpler Mironov bound when the Balle bound is worse:
        ε_dp_simple = ε_rdp + log(1/δ) / (α-1)
    """
    if delta <= 0 or delta >= 1:
        raise ValueError("delta must be in (0, 1)")
    if alpha <= 1:
        raise ValueError("alpha must be > 1 for RDP→DP conversion")

    # Mironov bound (always valid)
    simple = rdp_epsilon + math.log(1 / delta) / (alpha - 1)

    # Balle et al. tighter bound
    try:
        balle = (
            rdp_epsilon
            + math.log((alpha - 1) / alpha)
            - (math.log(delta) + math.log(alpha)) / (alpha - 1)
        )
    except (ValueError, OverflowError):
        balle = simple

    return min(simple, balle)


def best_rdp_to_dp(
    rdp_curve: List[Tuple[float, float]],
    delta: float,
) -> float:
    """
    Given a list of (α, ε_rdp) pairs, return the tightest (ε, δ)-DP bound
    by optimising over α.

    rdp_curve: [(alpha, eps_rdp), ...]
    """
    best = math.inf
    for alpha, eps_rdp in rdp_curve:
        if alpha <= 1.0:
            continue
        try:
            candidate = rdp_to_dp(eps_rdp, alpha, delta)
        except (ValueError, OverflowError):
            continue
        best = min(best, candidate)
    return best


# ---------------------------------------------------------------------------
# Composition
# ---------------------------------------------------------------------------

def compose_rdp(
    curves: List[List[Tuple[float, float]]],
) -> List[Tuple[float, float]]:
    """
    Compose k RDP curves by summing ε at each α order.
    All curves must share the same α grid.
    Returns the composed (α, Σε) curve.
    """
    if not curves:
        return [(a, 0.0) for a in ALPHA_ORDERS]

    result: dict[float, float] = {a: 0.0 for a in ALPHA_ORDERS}
    for curve in curves:
        for alpha, eps in curve:
            if alpha in result:
                result[alpha] += eps
    return sorted(result.items())


def rdp_curve_for_gaussian(
    sensitivity: float, sigma: float, orders: List[float] = ALPHA_ORDERS
) -> List[Tuple[float, float]]:
    return [(a, rdp_gaussian(sensitivity, sigma, a)) for a in orders]


def rdp_curve_for_laplace(
    sensitivity: float, b: float, orders: List[float] = ALPHA_ORDERS
) -> List[Tuple[float, float]]:
    return [(a, rdp_laplace(sensitivity, b, a)) for a in orders]


# ---------------------------------------------------------------------------
# Budget check helpers
# ---------------------------------------------------------------------------

def projected_dp_epsilon(
    accumulated_curve: List[Tuple[float, float]],
    new_curve: List[Tuple[float, float]],
    delta: float,
) -> float:
    """
    Return the (ε, δ)-DP epsilon that would result from adding new_curve
    to the already-accumulated RDP budget.
    """
    composed = [(a, acc + new) for (a, acc), (_, new) in zip(accumulated_curve, new_curve)]
    return best_rdp_to_dp(composed, delta)


def current_dp_epsilon(
    accumulated_curve: List[Tuple[float, float]],
    delta: float,
) -> float:
    """Convert current accumulated RDP curve to its (ε, δ)-DP equivalent."""
    return best_rdp_to_dp(accumulated_curve, delta)
