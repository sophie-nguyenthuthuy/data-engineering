"""Local differential-privacy randomizers.

Each function takes a true input and returns an ε₀-LDP randomized version.
"""
from __future__ import annotations

import math
import secrets
from dataclasses import dataclass

import numpy as np


@dataclass
class LocalConfig:
    eps0: float        # local epsilon
    domain_size: int   # for categorical (RR); ignored for numeric


def randomized_response(value: int, cfg: LocalConfig, rng=None) -> int:
    """k-ary randomized response. P(output = true) = e^ε / (e^ε + k - 1)."""
    k = cfg.domain_size
    eps = cfg.eps0
    rng = rng or np.random.default_rng(secrets.randbits(64))
    p_true = math.exp(eps) / (math.exp(eps) + k - 1)
    if rng.random() < p_true:
        return value
    # Else return a uniform other value
    other = int(rng.integers(0, k - 1))
    return other if other < value else other + 1


def laplace_noise(value: float, sensitivity: float, eps: float, rng=None) -> float:
    """ε-DP Laplace mechanism."""
    rng = rng or np.random.default_rng(secrets.randbits(64))
    scale = sensitivity / eps
    return value + float(rng.laplace(0.0, scale))


def gaussian_noise(value: float, sensitivity: float, eps: float, delta: float, rng=None) -> float:
    """(ε, δ)-DP Gaussian mechanism. σ = sensitivity * sqrt(2 ln(1.25/δ)) / ε."""
    rng = rng or np.random.default_rng(secrets.randbits(64))
    sigma = sensitivity * math.sqrt(2 * math.log(1.25 / delta)) / eps
    return value + float(rng.normal(0.0, sigma))


__all__ = ["LocalConfig", "randomized_response", "laplace_noise", "gaussian_noise"]
