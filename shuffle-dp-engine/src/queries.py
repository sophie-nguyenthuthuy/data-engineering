"""Aggregation queries on shuffled, locally-randomized data."""
from __future__ import annotations

import math
from collections import Counter

import numpy as np

from .local_randomizers import LocalConfig, randomized_response


def private_histogram(values: list[int], cfg: LocalConfig, rng=None) -> list[float]:
    """Return debiased histogram over domain {0,..,k-1}.

    Each value goes through k-ary RR; aggregator inverts the bias.
    P(output=v | true=v) = p_true; P(output=v' | true=v) = (1-p_true)/(k-1) for v'≠v.
    """
    rng = rng or np.random.default_rng()
    k = cfg.domain_size
    eps = cfg.eps0
    p_true = math.exp(eps) / (math.exp(eps) + k - 1)
    p_other = 1.0 / (math.exp(eps) + k - 1)

    # Randomize
    randomized = [randomized_response(v, cfg, rng) for v in values]
    counts = Counter(randomized)
    n = len(values)

    # Debiased estimate: E[count_v] = n * (true_freq_v * p_true + (1-true_freq_v) * p_other)
    # Solve: true_freq_v = (count_v/n - p_other) / (p_true - p_other)
    debiased = []
    for v in range(k):
        observed = counts.get(v, 0) / n
        est = (observed - p_other) / (p_true - p_other)
        debiased.append(max(0.0, est))
    # Renormalize
    total = sum(debiased)
    if total > 0:
        debiased = [x / total for x in debiased]
    return debiased


def private_mean(values: list[float], lo: float, hi: float, eps: float, rng=None) -> float:
    """Bounded-range mean with per-user Laplace noise, then averaged.

    For range [lo, hi], each user adds Laplace(scale = (hi-lo)/ε). Aggregator
    averages. This is not the optimal noise schedule but illustrates.
    """
    rng = rng or np.random.default_rng()
    sens = hi - lo
    noisy = [v + rng.laplace(0, sens / eps) for v in values]
    return float(np.mean(noisy))


__all__ = ["private_histogram", "private_mean"]
