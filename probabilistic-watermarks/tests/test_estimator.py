"""Per-key delay estimator."""

from __future__ import annotations

import random


def test_safe_delay_zero_for_unknown_key(estimator):
    assert estimator.safe_delay("nope") == 0.0


def test_safe_delay_grows_with_observations(estimator):
    for t in range(1, 1001):
        estimator.observe("k", float(t), float(t) + random_delay(t))
    sd = estimator.safe_delay("k")
    assert sd > 0


def test_safe_delay_monotone_nondecreasing(estimator):
    rng = random.Random(0)
    last = -1.0
    for t in range(1, 2001):
        delay = rng.expovariate(1.0)
        estimator.observe("k", float(t), float(t) + delay)
        cur = estimator.safe_delay("k")
        assert cur >= last - 1e-9, f"safe_delay decreased at t={t}: {last} -> {cur}"
        last = cur


def test_rate_estimator(estimator):
    # Each event is 1 second apart → rate ≈ 1.0
    for t in range(1, 51):
        estimator.observe("k", float(t), float(t))
    rate = estimator.rate("k")
    assert 0.5 < rate < 1.5


def test_each_key_tracked_independently(estimator):
    for t in range(1, 101):
        estimator.observe("hot", float(t), float(t) + 0.1)     # tight delays
        estimator.observe("cold", float(t), float(t) + 10.0)   # loose delays
    assert estimator.safe_delay("cold") > estimator.safe_delay("hot")


def random_delay(t: int) -> float:
    """Synthetic per-test delay."""
    return (t % 7) * 0.1
