"""
Zero-Concentrated Differential Privacy (zCDP) accounting.

Reference: Bun & Steinke (2016) "Concentrated Differential Privacy:
           Simplifications, Extensions, and Lower Bounds"
           https://arxiv.org/abs/1605.02065

Key facts:
  - M satisfies ρ-zCDP if  D_α(M(x) ‖ M(x')) ≤ ρ·α  for all α > 1, adj. x, x'
  - Gaussian mechanism with sensitivity Δ and noise std σ:  ρ = Δ²/(2σ²)
  - Composition: ρ₁-zCDP ⊕ ρ₂-zCDP  →  (ρ₁+ρ₂)-zCDP
  - To (ε,δ)-DP:  ρ-zCDP  ⟹  (ρ + 2√(ρ·log(1/δ)), δ)-DP  ∀δ∈(0,1)
  - Tighter conversion (Canonne et al. 2020 / BS16):
      ε_dp = ρ + 2√(ρ · log(1/δ))
    which grows as O(√k) for k independent queries vs O(k) for basic composition.

zCDP is strictly tighter than (ε,δ)-DP for Gaussian-mechanism workloads and
more convenient than RDP when only the Gaussian mechanism is used.
"""
from __future__ import annotations

import math
from dataclasses import dataclass, field
from typing import List


# ---------------------------------------------------------------------------
# Per-mechanism ρ
# ---------------------------------------------------------------------------

def zcdp_gaussian(sensitivity: float, sigma: float) -> float:
    """
    ρ for the Gaussian mechanism: ρ = Δ²/(2σ²).
    The mechanism M(x) = f(x) + N(0, σ²I) with l₂-sensitivity Δ.
    """
    if sigma <= 0:
        raise ValueError("sigma must be positive")
    return sensitivity ** 2 / (2 * sigma ** 2)


def zcdp_laplace_approx(sensitivity: float, epsilon_dp: float) -> float:
    """
    Approximate ρ for the Laplace mechanism via the RDP→zCDP relationship.
    The Laplace mechanism satisfies ε_dp-DP (pure) which implies
    (ε_dp²/2)-zCDP as a conservative bound.
    (The Laplace mechanism is not naturally analysed through zCDP;
     for tight bounds prefer the RDP accountant.)
    """
    return epsilon_dp ** 2 / 2


# ---------------------------------------------------------------------------
# zCDP → (ε, δ)-DP conversion
# ---------------------------------------------------------------------------

def zcdp_to_dp(rho: float, delta: float) -> float:
    """
    Convert ρ-zCDP to (ε, δ)-DP.
    ε = ρ + 2·√(ρ · log(1/δ))
    Valid for any δ ∈ (0, 1).
    """
    if rho < 0:
        raise ValueError("rho must be non-negative")
    if delta <= 0 or delta >= 1:
        raise ValueError("delta must be in (0, 1)")
    if rho == 0:
        return 0.0
    return rho + 2 * math.sqrt(rho * math.log(1 / delta))


# ---------------------------------------------------------------------------
# Composition
# ---------------------------------------------------------------------------

def compose_zcdp(rhos: List[float]) -> float:
    """
    Compose k mechanisms: ρ_total = Σ ρᵢ.
    """
    return sum(rhos)


# ---------------------------------------------------------------------------
# Budget utilities
# ---------------------------------------------------------------------------

@dataclass
class ZCDPBudget:
    """Tracks a ρ-zCDP budget for one (dataset, analyst) pair."""

    total_rho: float
    consumed_rho: float = 0.0
    delta: float = 1e-5

    @property
    def remaining_rho(self) -> float:
        return max(0.0, self.total_rho - self.consumed_rho)

    @property
    def consumed_dp_epsilon(self) -> float:
        if self.consumed_rho == 0:
            return 0.0
        return zcdp_to_dp(self.consumed_rho, self.delta)

    @property
    def remaining_dp_epsilon(self) -> float:
        if self.remaining_rho == 0:
            return 0.0
        return zcdp_to_dp(self.remaining_rho, self.delta)

    def would_exceed(self, new_rho: float) -> bool:
        return self.consumed_rho + new_rho > self.total_rho

    def max_feasible_rho(self) -> float:
        return self.remaining_rho

    def max_feasible_sigma(self, sensitivity: float) -> float:
        """Minimum σ that keeps the query within remaining budget."""
        rho_remaining = self.remaining_rho
        if rho_remaining <= 0:
            return math.inf
        return math.sqrt(sensitivity ** 2 / (2 * rho_remaining))


def sigma_for_rho(sensitivity: float, rho: float) -> float:
    """Return σ such that the Gaussian mechanism satisfies exactly ρ-zCDP."""
    if rho <= 0:
        return math.inf
    return math.sqrt(sensitivity ** 2 / (2 * rho))


def rho_for_sigma(sensitivity: float, sigma: float) -> float:
    """Return ρ for a Gaussian mechanism with given σ."""
    return sensitivity ** 2 / (2 * sigma ** 2)


def rho_for_dp_target(epsilon: float, delta: float) -> float:
    """
    Given a target (ε, δ)-DP level, return the ρ that achieves it via
    ε = ρ + 2√(ρ · log(1/δ)).

    Let L = log(1/δ), x = √ρ.  Then ε = x² + 2x·√L.
    Quadratic in x:  x = √(L + ε) - √L
    So ρ = (√(L + ε) - √L)²
    """
    if delta <= 0 or delta >= 1:
        raise ValueError("delta must be in (0,1)")
    L = math.log(1 / delta)
    rho = (math.sqrt(L + epsilon) - math.sqrt(L)) ** 2
    return rho


def basic_composition_dp_epsilon(query_epsilons: List[float]) -> float:
    """
    Basic ε-composition: ε_total = Σ εᵢ.
    Used as a pessimistic baseline to compare against RDP/zCDP.
    """
    return sum(query_epsilons)
