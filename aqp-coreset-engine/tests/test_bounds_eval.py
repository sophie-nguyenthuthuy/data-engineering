"""Bounds + empirical-validator tests."""

from __future__ import annotations

import random

import pytest

from aqp.bounds.size import coreset_size_sum, hoeffding_count_size
from aqp.coreset.sensitivity import SensitivityCoreset
from aqp.coreset.uniform import UniformCoreset
from aqp.eval import validate_coverage


def test_size_formulas_reject_bad_args():
    with pytest.raises(ValueError):
        coreset_size_sum(0.0, 0.05)
    with pytest.raises(ValueError):
        coreset_size_sum(0.05, 0.0)
    with pytest.raises(ValueError):
        coreset_size_sum(0.05, 0.05, vc=0)
    with pytest.raises(ValueError):
        hoeffding_count_size(1.0, 0.05)


def test_size_decreases_with_larger_eps():
    big = coreset_size_sum(0.1, 0.01)
    small = coreset_size_sum(0.01, 0.01)
    assert small > big


def test_size_increases_with_smaller_delta():
    loose = coreset_size_sum(0.05, 0.5)
    tight = coreset_size_sum(0.05, 0.001)
    assert tight > loose


def test_hoeffding_grows_with_smaller_eps():
    assert hoeffding_count_size(0.01, 0.05) > hoeffding_count_size(0.1, 0.05)


def _stream(n: int, seed: int = 0):
    rng = random.Random(seed)
    return [(rng.uniform(0.0, 100.0), (float(rng.randrange(3)),)) for _ in range(n)]


def test_validate_returns_perfect_coverage_on_empty():
    cs = SensitivityCoreset(eps=0.1, delta=0.05).finalize()
    rep = validate_coverage(cs, [], n_queries=10)
    assert rep.coverage == 1.0
    assert rep.n_queries == 0


def test_validate_rejects_zero_queries():
    cs = SensitivityCoreset(eps=0.1, delta=0.05).finalize()
    with pytest.raises(ValueError):
        validate_coverage(cs, [(1.0, (0.0,))], n_queries=0)


def test_sensitivity_validation_reports_reasonable_coverage():
    rows = _stream(5_000, seed=42)
    sens = SensitivityCoreset(eps=0.05, delta=0.05, seed=0)
    for v, p in rows:
        sens.add(v, p)
    rep = validate_coverage(sens.finalize(), rows, n_queries=120, level=0.95, seed=1)
    # Coverage will be high (Gaussian CIs at z=1.96 are conservative for sums).
    assert rep.coverage > 0.7
    assert rep.mean_relative_error < 0.5


def test_uniform_validation_runs_without_error():
    rows = _stream(2_000, seed=10)
    cs_builder = UniformCoreset(m=200, seed=0)
    for v, p in rows:
        cs_builder.add(v, p)
    rep = validate_coverage(cs_builder.finalize(), rows, n_queries=50, seed=1)
    assert 0.0 <= rep.coverage <= 1.0
