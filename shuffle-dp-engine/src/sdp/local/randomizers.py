"""Local-DP randomizers.

  randomized_response   k-ary RR with parameter ε₀
  laplace_noise          continuous ε-DP Laplace mechanism
  gaussian_noise         (ε, δ)-DP Gaussian mechanism

These run on each user's data BEFORE the shuffler.
"""

from __future__ import annotations

import math
import secrets
from dataclasses import dataclass

import numpy as np


@dataclass(frozen=True, slots=True)
class LocalConfig:
    """Configuration for k-ary randomized response."""

    eps0: float
    domain_size: int

    def __post_init__(self) -> None:
        if self.eps0 <= 0:
            raise ValueError("eps0 must be > 0")
        if self.domain_size < 2:
            raise ValueError("domain_size must be ≥ 2")


def randomized_response(
    value: int,
    cfg: LocalConfig,
    rng: np.random.Generator | None = None,
) -> int:
    """k-ary randomized response.

    P(output = true value) = e^ε / (e^ε + k − 1)
    P(output = each other) = 1 / (e^ε + k − 1)
    """
    if not 0 <= value < cfg.domain_size:
        raise ValueError(f"value {value} out of domain [0, {cfg.domain_size})")
    rng = rng or np.random.default_rng(secrets.randbits(64))
    k = cfg.domain_size
    p_true = math.exp(cfg.eps0) / (math.exp(cfg.eps0) + k - 1)
    if rng.random() < p_true:
        return value
    other = int(rng.integers(0, k - 1))
    return other if other < value else other + 1


def laplace_noise(
    value: float,
    sensitivity: float,
    eps: float,
    rng: np.random.Generator | None = None,
) -> float:
    """Add Laplace(scale = sensitivity / ε) noise. Returns ε-DP."""
    if eps <= 0:
        raise ValueError("eps must be > 0")
    if sensitivity <= 0:
        raise ValueError("sensitivity must be > 0")
    rng = rng or np.random.default_rng(secrets.randbits(64))
    return value + float(rng.laplace(0.0, sensitivity / eps))


def gaussian_noise(
    value: float,
    sensitivity: float,
    eps: float,
    delta: float,
    rng: np.random.Generator | None = None,
) -> float:
    """Add Gaussian noise with σ = sensitivity * sqrt(2 ln(1.25/δ)) / ε.

    Returns (ε, δ)-DP.
    """
    if not 0 < eps < 1:
        raise ValueError("Gaussian mechanism requires 0 < eps < 1")
    if not 0 < delta < 1:
        raise ValueError("delta must be in (0, 1)")
    rng = rng or np.random.default_rng(secrets.randbits(64))
    sigma = sensitivity * math.sqrt(2 * math.log(1.25 / delta)) / eps
    return value + float(rng.normal(0.0, sigma))
