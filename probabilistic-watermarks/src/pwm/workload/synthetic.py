"""Synthetic event streams with known delay distributions.

Each generator yields (key, event_time, arrival_time) triples.
The event-time grid is deterministic; the delay is random.
"""

from __future__ import annotations

import random
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from collections.abc import Iterator


def exponential_delay_workload(
    n_events: int,
    lambda_rate: float = 1.0,
    n_keys: int = 1,
    seed: int = 0,
) -> Iterator[tuple[int, float, float]]:
    """Exponential delays with mean 1/lambda_rate."""
    rng = random.Random(seed)
    for t in range(1, n_events + 1):
        k = rng.randint(0, n_keys - 1)
        delay = rng.expovariate(lambda_rate)
        yield k, float(t), float(t) + delay


def lognormal_delay_workload(
    n_events: int,
    mu: float = 0.0,
    sigma: float = 1.0,
    n_keys: int = 1,
    seed: int = 0,
) -> Iterator[tuple[int, float, float]]:
    """Lognormal delay."""
    rng = random.Random(seed)
    for t in range(1, n_events + 1):
        k = rng.randint(0, n_keys - 1)
        delay = rng.lognormvariate(mu, sigma)
        yield k, float(t), float(t) + delay


def pareto_delay_workload(
    n_events: int,
    alpha: float = 1.5,
    n_keys: int = 1,
    seed: int = 0,
) -> Iterator[tuple[int, float, float]]:
    """Heavy-tailed Pareto delay."""
    rng = random.Random(seed)
    for t in range(1, n_events + 1):
        k = rng.randint(0, n_keys - 1)
        # Type-II Pareto / Lomax: shift = 1
        delay = rng.paretovariate(alpha) - 1.0
        yield k, float(t), float(t) + delay


def bimodal_workload(
    n_events: int,
    p_heavy: float = 0.05,
    mu_light: float = 0.0,
    sigma_light: float = 0.5,
    mu_heavy: float = 2.0,
    sigma_heavy: float = 1.0,
    n_keys: int = 1,
    seed: int = 0,
) -> Iterator[tuple[int, float, float]]:
    """Mixture of two lognormals: 95% short delays, 5% long.

    This is the canonical scenario where fixed-N-second watermarks fail.
    """
    rng = random.Random(seed)
    for t in range(1, n_events + 1):
        k = rng.randint(0, n_keys - 1)
        if rng.random() < p_heavy:
            delay = rng.lognormvariate(mu_heavy, sigma_heavy)
        else:
            delay = rng.lognormvariate(mu_light, sigma_light)
        yield k, float(t), float(t) + delay
