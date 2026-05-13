"""DP histogram + mean queries.

After local randomization (and shuffling), the aggregator collects N
randomized values and debiases them analytically to recover the true
distribution.
"""

from __future__ import annotations

import math
from collections import Counter

import numpy as np

from sdp.local.randomizers import LocalConfig, laplace_noise, randomized_response


def private_histogram(
    values: list[int],
    cfg: LocalConfig,
    rng: np.random.Generator | None = None,
) -> list[float]:
    """Compute a differentially-private histogram over a categorical domain.

    Each user runs k-ary RR locally; aggregator counts the noisy outputs
    and inverts the bias analytically.

    Returns a probability vector (sums to 1).
    """
    if not values:
        return [0.0] * cfg.domain_size
    rng = rng or np.random.default_rng()
    k = cfg.domain_size
    eps = cfg.eps0
    p_true = math.exp(eps) / (math.exp(eps) + k - 1)
    p_other = 1.0 / (math.exp(eps) + k - 1)

    randomized = [randomized_response(v, cfg, rng) for v in values]
    counts = Counter(randomized)
    n = len(values)

    # Observed = n * (p_true * true_freq + p_other * (1 - true_freq))
    # → true_freq = (observed/n − p_other) / (p_true − p_other)
    debiased: list[float] = []
    for v in range(k):
        observed = counts.get(v, 0) / n
        est = (observed - p_other) / (p_true - p_other)
        debiased.append(max(0.0, est))
    total = sum(debiased)
    if total > 0:
        debiased = [x / total for x in debiased]
    return debiased


def private_mean(
    values: list[float],
    lo: float,
    hi: float,
    eps: float,
    rng: np.random.Generator | None = None,
) -> float:
    """ε-DP mean of bounded-range values via per-record Laplace noise.

    Sensitivity = (hi - lo) / n: each user contributes (hi-lo)/n in the
    worst case to the mean. We add Laplace(sensitivity/eps) noise.
    """
    if not values:
        return 0.0
    rng = rng or np.random.default_rng()
    n = len(values)
    sensitivity = (hi - lo) / n
    true_mean = sum(values) / n
    return laplace_noise(true_mean, sensitivity=sensitivity, eps=eps, rng=rng)


__all__ = ["private_histogram", "private_mean"]
