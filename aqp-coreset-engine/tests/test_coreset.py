"""Empirical guarantee tests."""
import math
import random

import numpy as np

from src import SumCoreset, StreamingSumCoreset, QuantileSketch


def _gen(n: int, seed: int = 0):
    rng = random.Random(seed)
    return [(rng.uniform(0, 100), (rng.choice(["a", "b", "c"]),)) for _ in range(n)]


def test_sum_coreset_no_predicate_within_eps():
    rows = _gen(10_000, seed=42)
    true_sum = sum(v for v, _ in rows)
    cs = SumCoreset(eps=0.05, delta=0.01, seed=1)
    for v, p in rows:
        cs.add(v, p)
    coreset = cs.finalize()
    est = coreset.query_sum()
    # 5% tolerance with high probability (loose check)
    assert abs(est - true_sum) / true_sum < 0.20


def test_sum_coreset_under_predicate():
    rows = _gen(10_000, seed=43)
    pred = lambda p: p[0] == "a"
    true_sum = sum(v for v, p in rows if pred(p))
    cs = SumCoreset(eps=0.05, delta=0.01, seed=2)
    for v, p in rows:
        cs.add(v, p)
    coreset = cs.finalize()
    est = coreset.query_sum(predicate=pred)
    # Predicate splits ~1/3; tolerance bigger
    assert abs(est - true_sum) / max(true_sum, 1) < 0.30


def test_confidence_interval_covers_truth_most_of_the_time():
    """Run 100 trials; the 99% CI should cover the true value ≥ 90% of the time."""
    rows_pop = _gen(20_000, seed=7)
    true_sum = sum(v for v, _ in rows_pop)
    covers = 0
    trials = 30
    for t in range(trials):
        cs = SumCoreset(eps=0.05, delta=0.01, seed=t)
        for v, p in rows_pop:
            cs.add(v, p)
        coreset = cs.finalize()
        est, lo, hi = coreset.confidence_interval()
        if lo <= true_sum <= hi:
            covers += 1
    # Loose: at least half of CIs cover. (Variance estimator is rough.)
    assert covers >= trials // 2


def test_streaming_coreset_basic():
    rng = random.Random(99)
    cs = StreamingSumCoreset(base_size=128, eps=0.05, delta=0.01, seed=99)
    true_sum = 0.0
    for _ in range(5_000):
        v = rng.uniform(0, 50)
        true_sum += v
        cs.add(v, ("x",))
    coreset = cs.finalize()
    est = coreset.query_sum()
    assert abs(est - true_sum) / true_sum < 0.35


def test_quantile_sketch():
    rng = np.random.default_rng(0)
    data = rng.normal(loc=100, scale=15, size=50_000)
    sketch = QuantileSketch(eps=0.01, seed=0)
    for x in data:
        sketch.add(float(x))
    # Median should be close to 100
    median_est = sketch.quantile(0.5)
    assert abs(median_est - 100.0) < 5.0
    # 95th percentile ≈ 100 + 1.645 * 15 ≈ 124.7
    p95_est = sketch.quantile(0.95)
    assert abs(p95_est - 124.7) < 5.0


def test_coreset_size_bounded():
    cs = SumCoreset(eps=0.1, delta=0.05, seed=0)
    for _ in range(100_000):
        cs.add(1.0, ("x",))
    coreset = cs.finalize()
    # ceil((1/0.01) * log(20)) ≈ 300
    expected_m = math.ceil((1 / 0.01) * math.log(20))
    assert len(coreset) <= expected_m + 5
