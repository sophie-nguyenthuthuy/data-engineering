"""LatencyStats tests."""

from __future__ import annotations

import pytest
from hypothesis import given, settings
from hypothesis import strategies as st

from pvc.stats import LatencyStats, _percentile, summarise


def test_summarise_single_sample():
    s = summarise([0.1])
    assert s.n == 1
    assert s.mean == s.min == s.max == s.p50 == s.p95 == s.p99 == 0.1


def test_summarise_rejects_empty():
    with pytest.raises(ValueError):
        summarise([])


def test_summarise_rejects_negative():
    with pytest.raises(ValueError):
        summarise([0.1, -0.05])


def test_summarise_basic_percentiles():
    samples = [0.001, 0.002, 0.003, 0.004, 0.005, 0.006, 0.007, 0.008, 0.009, 0.010]
    s = summarise(samples)
    assert s.n == 10
    assert s.min == 0.001
    assert s.max == 0.010
    # p50 at rank 5 → 0.005; p95 at rank 10 → 0.010 (nearest-rank)
    assert s.p50 == 0.005
    assert s.p95 == 0.010


def test_summarise_p99_nearest_rank():
    samples = list(range(1, 101))  # 100 samples
    s = summarise([float(x) for x in samples])
    # rank = ceil(0.99 * 100) = 99 → value at index 98 (1-indexed 99) = 99
    assert s.p99 == 99.0


def test_percentile_rejects_bad_q():
    with pytest.raises(ValueError):
        _percentile([1.0, 2.0], -0.1)
    with pytest.raises(ValueError):
        _percentile([1.0, 2.0], 1.1)


def test_latency_stats_to_dict():
    s = LatencyStats(n=1, mean=0.1, p50=0.1, p95=0.1, p99=0.1, min=0.1, max=0.1)
    d = s.to_dict()
    assert d["n"] == 1
    assert d["p99"] == 0.1


@settings(max_examples=30, deadline=None)
@given(st.lists(st.floats(0.0, 10.0, allow_nan=False), min_size=1, max_size=50))
def test_property_p99_le_max(samples):
    s = summarise(samples)
    assert s.p99 <= s.max
    assert s.p50 <= s.p95 <= s.p99
    assert s.min <= s.mean <= s.max
