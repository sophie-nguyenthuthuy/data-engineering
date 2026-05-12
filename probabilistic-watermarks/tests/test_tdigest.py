"""T-digest correctness + accuracy."""

from __future__ import annotations

import random

from pwm.sketch.tdigest import TDigest


def test_empty_quantile_is_nan():
    d = TDigest()
    import math
    assert math.isnan(d.quantile(0.5))


def test_single_value():
    d = TDigest()
    d.add(42.0)
    assert d.quantile(0.5) == 42.0


def test_uniform_median():
    """Uniform [0, 100] → median ≈ 50."""
    d = TDigest(delta=200)
    rng = random.Random(0)
    for _ in range(20_000):
        d.add(rng.uniform(0, 100))
    median = d.quantile(0.5)
    assert 47 < median < 53


def test_p99_for_uniform():
    """Uniform [0, 100] → p99 ≈ 99."""
    d = TDigest(delta=200)
    rng = random.Random(0)
    for _ in range(50_000):
        d.add(rng.uniform(0, 100))
    p99 = d.quantile(0.99)
    assert 96 < p99 < 100


def test_p99_for_exponential():
    """Exp(mean=1) → p99 ≈ 4.6."""
    d = TDigest(delta=200)
    rng = random.Random(0)
    for _ in range(50_000):
        d.add(rng.expovariate(1.0))
    p99 = d.quantile(0.99)
    assert 4.0 < p99 < 5.5


def test_memory_bounded():
    """Centroid count stays bounded under heavy data."""
    d = TDigest(delta=100)
    rng = random.Random(0)
    for _ in range(100_000):
        d.add(rng.uniform(0, 1))
    assert d.memory_centroids() < 500


def test_merge():
    """Merging two digests is associative."""
    d1 = TDigest(delta=100)
    d2 = TDigest(delta=100)
    rng = random.Random(0)
    samples = [rng.uniform(0, 100) for _ in range(20_000)]
    for x in samples[:10_000]:
        d1.add(x)
    for x in samples[10_000:]:
        d2.add(x)
    d1.merge(d2)
    median = d1.quantile(0.5)
    assert 47 < median < 53


def test_count():
    d = TDigest()
    for i in range(100):
        d.add(i)
    assert d.count() == 100
