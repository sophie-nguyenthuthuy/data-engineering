"""Per-column cardinality estimation from a sample.

We use the **Goodman estimator** for the number of unobserved values
in a population given a uniform sample — substantially better than
naive ``len(set(sample))`` when the population is large and the sample
is moderate.

``Goodman(1949): nu = sum_i (-1)^(i+1) * (n_i choose 1)`` boils down to
``N_estimated = N_observed + sum_i (-1)^(i+1) * f_i`` where ``f_i`` is
the number of values that appear exactly ``i`` times. We expose a
simpler form: observed-distinct + an adjustment for singletons.
"""

from __future__ import annotations

from collections import Counter
from dataclasses import dataclass
from typing import Any


@dataclass(frozen=True, slots=True)
class CardinalityEstimate:
    """Estimated number of distinct values + supporting numbers."""

    name: str
    sample_size: int
    observed_distinct: int
    estimated_distinct: int

    def __post_init__(self) -> None:
        if self.sample_size < 0:
            raise ValueError("sample_size must be ≥ 0")
        if self.observed_distinct < 0:
            raise ValueError("observed_distinct must be ≥ 0")
        if self.observed_distinct > self.sample_size:
            raise ValueError("observed_distinct cannot exceed sample_size")
        if self.estimated_distinct < self.observed_distinct:
            raise ValueError("estimated_distinct cannot be < observed_distinct")


def estimate_cardinality(name: str, sample: list[Any]) -> CardinalityEstimate:
    """Estimate distinct-count from a sample using the Goodman correction."""
    if not name:
        raise ValueError("name must be non-empty")
    if not sample:
        return CardinalityEstimate(
            name=name, sample_size=0, observed_distinct=0, estimated_distinct=0
        )
    counts: Counter[Any] = Counter(sample)
    observed = len(counts)
    singletons = sum(1 for v in counts.values() if v == 1)
    n = len(sample)
    # Chao1 lower bound: D + (singletons^2) / (2 * doubletons), clipped above
    # at the sample size to avoid wild over-estimation when doubletons == 0.
    doubletons = sum(1 for v in counts.values() if v == 2)
    if doubletons > 0:
        chao = observed + (singletons * singletons) // (2 * doubletons)
    else:
        chao = observed + (singletons * (singletons - 1)) // 2
    # Cap at a reasonable multiple of observed so we never claim more
    # distinct values than the sample could possibly support.
    cap = max(observed, n)
    return CardinalityEstimate(
        name=name,
        sample_size=n,
        observed_distinct=observed,
        estimated_distinct=min(chao, cap),
    )


__all__ = ["CardinalityEstimate", "estimate_cardinality"]
