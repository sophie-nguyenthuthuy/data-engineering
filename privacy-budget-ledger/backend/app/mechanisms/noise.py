"""
Noise mechanisms: Laplace and Gaussian.

Each mechanism returns:
  - the noisy result
  - the QueryCost (for the composition ledger)
  - the actual σ / noise scale used
"""
from __future__ import annotations

import math
from enum import Enum
from typing import Optional, Tuple

import numpy as np

from ..composition import (
    make_query_cost_gaussian,
    make_query_cost_laplace,
    QueryCost,
)


class Mechanism(str, Enum):
    LAPLACE = "laplace"
    GAUSSIAN = "gaussian"


class QueryType(str, Enum):
    COUNT = "count"
    SUM = "sum"
    MEAN = "mean"
    HISTOGRAM = "histogram"
    CUSTOM = "custom"


# ---------------------------------------------------------------------------
# Calibration
# ---------------------------------------------------------------------------

def calibrate_gaussian_sigma(
    sensitivity: float, epsilon: float, delta: float
) -> float:
    """
    Return σ such that the Gaussian mechanism satisfies (ε, δ)-DP.
    Classic calibration: σ = Δ · √(2 ln(1.25/δ)) / ε
    """
    if epsilon <= 0:
        raise ValueError("epsilon must be positive")
    if delta <= 0 or delta >= 1:
        raise ValueError("delta must be in (0, 1)")
    return sensitivity * math.sqrt(2 * math.log(1.25 / delta)) / epsilon


def calibrate_gaussian_sigma_rdp(
    sensitivity: float, target_epsilon: float, delta: float, alpha: float = 2.0
) -> float:
    """
    Calibrate σ using the RDP Gaussian formula: ε_rdp(α) = α·Δ²/(2σ²).
    Then convert (α, ε_rdp) → (ε,δ)-DP and solve so the result equals target_epsilon.
    This is tighter than the classic calibration for large workloads.
    """
    # ε_rdp = α·Δ²/(2σ²), convert: ε_dp ≈ ε_rdp + log(1/δ)/(α-1)
    # Set ε_dp = target_epsilon → ε_rdp = target_epsilon - log(1/δ)/(α-1)
    headroom = math.log(1 / delta) / (alpha - 1)
    eps_rdp_target = max(target_epsilon - headroom, 1e-9)
    # ε_rdp = α·Δ²/(2σ²) → σ = Δ·√(α/(2·ε_rdp_target))
    return sensitivity * math.sqrt(alpha / (2 * eps_rdp_target))


def calibrate_laplace_scale(sensitivity: float, epsilon: float) -> float:
    """b = Δ/ε for Laplace mechanism."""
    if epsilon <= 0:
        raise ValueError("epsilon must be positive")
    return sensitivity / epsilon


# ---------------------------------------------------------------------------
# Apply mechanisms
# ---------------------------------------------------------------------------

def apply_gaussian(
    true_value: float,
    sensitivity: float,
    epsilon: float,
    delta: float = 1e-5,
    sigma_override: Optional[float] = None,
) -> Tuple[float, float, QueryCost]:
    """
    Apply Gaussian mechanism.
    Returns (noisy_value, sigma_used, query_cost).
    """
    sigma = sigma_override if sigma_override is not None else calibrate_gaussian_sigma(
        sensitivity, epsilon, delta
    )
    noise = float(np.random.normal(0, sigma))
    cost = make_query_cost_gaussian(sensitivity, sigma, delta)
    return true_value + noise, sigma, cost


def apply_laplace(
    true_value: float,
    sensitivity: float,
    epsilon: float,
) -> Tuple[float, float, QueryCost]:
    """
    Apply Laplace mechanism.
    Returns (noisy_value, b_used, query_cost).
    """
    b = calibrate_laplace_scale(sensitivity, epsilon)
    noise = float(np.random.laplace(0, b))
    cost = make_query_cost_laplace(sensitivity, epsilon)
    return true_value + noise, b, cost


def apply_mechanism(
    true_value: float,
    sensitivity: float,
    mechanism: Mechanism,
    epsilon: float,
    delta: float = 1e-5,
    sigma_override: Optional[float] = None,
) -> Tuple[float, QueryCost]:
    """
    Unified interface: apply the selected mechanism.
    Returns (noisy_value, query_cost).
    """
    if mechanism == Mechanism.GAUSSIAN:
        noisy, _, cost = apply_gaussian(true_value, sensitivity, epsilon, delta, sigma_override)
    else:
        noisy, _, cost = apply_laplace(true_value, sensitivity, epsilon)
    return noisy, cost


# ---------------------------------------------------------------------------
# Default sensitivities
# ---------------------------------------------------------------------------

def default_sensitivity(query_type: QueryType, data_range: Optional[float] = None) -> float:
    if query_type == QueryType.COUNT:
        return 1.0
    if query_type in (QueryType.SUM, QueryType.MEAN):
        return data_range or 1.0
    if query_type == QueryType.HISTOGRAM:
        return 1.0
    return 1.0


# ---------------------------------------------------------------------------
# Noise statistics (for accuracy reporting)
# ---------------------------------------------------------------------------

def gaussian_std(sensitivity: float, epsilon: float, delta: float) -> float:
    return calibrate_gaussian_sigma(sensitivity, epsilon, delta)


def laplace_std(sensitivity: float, epsilon: float) -> float:
    return math.sqrt(2) * sensitivity / epsilon
