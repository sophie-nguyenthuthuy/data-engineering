"""KLL quantile sketch tests."""

from __future__ import annotations

import numpy as np
import pytest
from hypothesis import given, settings
from hypothesis import strategies as st

from aqp.coreset.kll import KLLSketch


def test_kll_rejects_small_k():
    with pytest.raises(ValueError):
        KLLSketch(k=4)


def test_kll_for_epsilon_rejects_bad_eps():
    with pytest.raises(ValueError):
        KLLSketch.for_epsilon(0.0)
    with pytest.raises(ValueError):
        KLLSketch.for_epsilon(1.5)


def test_kll_for_epsilon_picks_growing_k():
    small = KLLSketch.for_epsilon(0.1)
    big = KLLSketch.for_epsilon(0.01)
    assert big.k > small.k


def test_kll_empty_quantile_raises():
    s = KLLSketch(k=32)
    with pytest.raises(ValueError):
        s.quantile(0.5)


def test_kll_empty_rank_raises():
    s = KLLSketch(k=32)
    with pytest.raises(ValueError):
        s.rank(0.0)


def test_kll_quantile_out_of_range_raises():
    s = KLLSketch(k=32)
    s.add(0.0)
    with pytest.raises(ValueError):
        s.quantile(-0.1)
    with pytest.raises(ValueError):
        s.quantile(1.5)


def test_kll_median_normal():
    rng = np.random.default_rng(0)
    s = KLLSketch.for_epsilon(0.01, seed=0)
    for x in rng.normal(loc=100.0, scale=15.0, size=50_000):
        s.add(float(x))
    assert abs(s.quantile(0.5) - 100.0) < 3.0


def test_kll_p95_normal():
    rng = np.random.default_rng(7)
    s = KLLSketch.for_epsilon(0.01, seed=7)
    for x in rng.normal(loc=100.0, scale=15.0, size=50_000):
        s.add(float(x))
    # 95th pctile ≈ 100 + 1.6449 · 15 ≈ 124.67
    assert abs(s.quantile(0.95) - 124.67) < 3.0


def test_kll_quantile_extremes():
    s = KLLSketch(k=64, seed=0)
    for v in range(1000):
        s.add(float(v))
    assert s.quantile(0.0) <= 50.0
    assert s.quantile(1.0) >= 950.0


def test_kll_rank_bounded_in_unit_interval():
    rng = np.random.default_rng(3)
    s = KLLSketch(k=64, seed=3)
    for x in rng.uniform(0.0, 1.0, size=5_000):
        s.add(float(x))
    for q in (0.0, 0.25, 0.5, 0.75, 1.0):
        r = s.rank(q)
        assert 0.0 <= r <= 1.0


def test_kll_rank_is_monotone_in_threshold():
    rng = np.random.default_rng(4)
    s = KLLSketch(k=64, seed=4)
    for x in rng.uniform(0.0, 100.0, size=5_000):
        s.add(float(x))
    ranks = [s.rank(t) for t in (10.0, 30.0, 50.0, 70.0, 90.0)]
    assert ranks == sorted(ranks)


def test_kll_merge_associative_count():
    s1 = KLLSketch(k=64, seed=1)
    s2 = KLLSketch(k=64, seed=2)
    for v in range(500):
        s1.add(float(v))
    for v in range(500, 1000):
        s2.add(float(v))
    merged = s1.merge(s2)
    assert merged.n == s1.n + s2.n
    # Merged median ≈ 500
    assert abs(merged.quantile(0.5) - 500.0) < 80.0


def test_kll_merge_rejects_mismatched_k():
    s1 = KLLSketch(k=32)
    s2 = KLLSketch(k=64)
    with pytest.raises(ValueError):
        s1.merge(s2)


def test_kll_n_tracks_adds():
    s = KLLSketch(k=32)
    for _ in range(100):
        s.add(0.0)
    assert s.n == 100


@settings(max_examples=20, deadline=None)
@given(st.lists(st.floats(0.0, 1.0, allow_nan=False), min_size=50, max_size=200))
def test_property_kll_quantile_monotone(values):
    s = KLLSketch(k=32, seed=0)
    for v in values:
        s.add(v)
    qs = [s.quantile(p) for p in (0.1, 0.25, 0.5, 0.75, 0.9)]
    assert qs == sorted(qs)
