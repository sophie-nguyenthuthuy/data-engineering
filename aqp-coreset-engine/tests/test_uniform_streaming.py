"""Uniform-baseline + streaming merge-and-reduce tests."""

from __future__ import annotations

import random

import pytest

from aqp.coreset.streaming import StreamingSumCoreset
from aqp.coreset.uniform import UniformCoreset


def test_uniform_rejects_bad_m():
    with pytest.raises(ValueError):
        UniformCoreset(m=0)


def test_uniform_empty_input():
    cs = UniformCoreset(m=10).finalize()
    assert len(cs) == 0


def test_uniform_size_capped_by_m_for_large_streams():
    cs_builder = UniformCoreset(m=64, seed=0)
    rng = random.Random(0)
    for _ in range(2000):
        cs_builder.add(rng.uniform(0, 10), (0.0,))
    cs = cs_builder.finalize()
    assert len(cs) == 64


def test_uniform_size_equals_n_for_small_streams():
    cs_builder = UniformCoreset(m=200, seed=0)
    for _ in range(50):
        cs_builder.add(1.0, (0.0,))
    cs = cs_builder.finalize()
    assert len(cs) == 50


def test_uniform_sum_estimate_close_to_truth():
    rng = random.Random(42)
    rows = [(rng.uniform(0.0, 100.0), (0.0,)) for _ in range(10_000)]
    truth = sum(v for v, _ in rows)
    cs_builder = UniformCoreset(m=500, seed=1)
    for v, p in rows:
        cs_builder.add(v, p)
    cs = cs_builder.finalize()
    assert abs(cs.query_sum() - truth) / truth < 0.10


# ----------------------------------------------------------------- streaming


def test_streaming_rejects_bad_base_size():
    with pytest.raises(ValueError):
        StreamingSumCoreset(base_size=1)


def test_streaming_empty_finalize_is_empty():
    cs = StreamingSumCoreset(base_size=16).finalize()
    assert len(cs) == 0


def test_streaming_small_stream_keeps_buffered_rows():
    s = StreamingSumCoreset(base_size=64, seed=0)
    for _ in range(10):
        s.add(1.0, (0.0,))
    cs = s.finalize()
    assert cs.query_sum() == pytest.approx(10.0)


def test_streaming_sum_estimate_close_to_truth():
    rng = random.Random(99)
    s = StreamingSumCoreset(base_size=256, seed=99)
    truth = 0.0
    for _ in range(8_000):
        v = rng.uniform(0.0, 50.0)
        truth += v
        s.add(v, (0.0,))
    est = s.finalize().query_sum()
    assert abs(est - truth) / truth < 0.30


def test_streaming_creates_log_many_levels():
    s = StreamingSumCoreset(base_size=64, seed=0)
    for i in range(1024):
        s.add(float(i), (0.0,))
    # 1024 / 64 = 16 = 2^4, so ~4 levels are created.
    assert 3 <= s.n_levels <= 6


def test_streaming_tracks_n_rows():
    s = StreamingSumCoreset(base_size=32)
    for _ in range(100):
        s.add(1.0, (0.0,))
    assert s.n_rows == 100
