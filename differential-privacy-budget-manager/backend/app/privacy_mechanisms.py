"""
Differential privacy mechanisms: Laplace and Gaussian.
"""
import math
import numpy as np
from dataclasses import dataclass
from enum import Enum
from typing import Optional


class Mechanism(str, Enum):
    LAPLACE = "laplace"
    GAUSSIAN = "gaussian"


class QueryType(str, Enum):
    COUNT = "count"
    SUM = "sum"
    MEAN = "mean"
    HISTOGRAM = "histogram"


@dataclass
class PrivacyParams:
    epsilon: float       # privacy budget consumed by this query
    delta: float = 0.0   # relaxation parameter (only for Gaussian)
    sensitivity: float = 1.0
    mechanism: Mechanism = Mechanism.LAPLACE


def laplace_noise(sensitivity: float, epsilon: float) -> float:
    """Sample noise from Laplace(0, sensitivity/epsilon)."""
    if epsilon <= 0:
        raise ValueError("epsilon must be positive")
    scale = sensitivity / epsilon
    return float(np.random.laplace(0, scale))


def gaussian_noise(sensitivity: float, epsilon: float, delta: float) -> float:
    """
    Sample noise from Gaussian mechanism.
    Sigma = sensitivity * sqrt(2 * ln(1.25/delta)) / epsilon
    Satisfies (epsilon, delta)-DP.
    """
    if epsilon <= 0 or delta <= 0 or delta >= 1:
        raise ValueError("epsilon must be positive; delta in (0, 1)")
    sigma = sensitivity * math.sqrt(2 * math.log(1.25 / delta)) / epsilon
    return float(np.random.normal(0, sigma))


def apply_mechanism(true_value: float, params: PrivacyParams) -> float:
    """Apply the appropriate noise mechanism to a query result."""
    if params.mechanism == Mechanism.LAPLACE:
        noise = laplace_noise(params.sensitivity, params.epsilon)
    else:
        noise = gaussian_noise(params.sensitivity, params.epsilon, params.delta)
    return true_value + noise


def default_sensitivity(query_type: QueryType, data_range: Optional[float] = None) -> float:
    """
    Return global sensitivity for common query types.
    data_range is required for SUM/MEAN (max - min of the attribute).
    """
    if query_type == QueryType.COUNT:
        return 1.0
    if query_type == QueryType.SUM:
        return data_range or 1.0
    if query_type == QueryType.MEAN:
        return (data_range or 1.0)  # for bounded mean with n known
    if query_type == QueryType.HISTOGRAM:
        return 1.0
    return 1.0


def compute_epsilon_spent(query_type: QueryType, mechanism: Mechanism, delta: float = 1e-5) -> float:
    """
    Return the per-query epsilon that a standard query costs.
    Callers can override this when submitting a query.
    """
    return 1.0  # default: each query costs 1 unit of epsilon


def laplace_std(sensitivity: float, epsilon: float) -> float:
    return math.sqrt(2) * sensitivity / epsilon


def gaussian_std(sensitivity: float, epsilon: float, delta: float) -> float:
    return sensitivity * math.sqrt(2 * math.log(1.25 / delta)) / epsilon
