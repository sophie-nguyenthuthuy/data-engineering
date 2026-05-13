"""Sensitivity-sampling coreset tests."""

from __future__ import annotations

import random

import pytest

from aqp.coreset.sensitivity import SensitivityCoreset
from aqp.queries.predicates import eq_pred


def _stream(n: int, seed: int = 0, n_cats: int = 3):
    rng = random.Random(seed)
    return [(rng.uniform(0.0, 100.0), (float(rng.randrange(n_cats)),)) for _ in range(n)]


def test_sensitivity_rejects_bad_eps():
    with pytest.raises(ValueError):
        SensitivityCoreset(eps=0.0)
    with pytest.raises(ValueError):
        SensitivityCoreset(eps=1.0)


def test_sensitivity_rejects_bad_delta():
    with pytest.raises(ValueError):
        SensitivityCoreset(delta=0.0)
    with pytest.raises(ValueError):
        SensitivityCoreset(delta=1.0)


def test_sensitivity_rejects_bad_vc():
    with pytest.raises(ValueError):
        SensitivityCoreset(vc=0)


def test_finalize_empty_returns_empty_coreset():
    cs = SensitivityCoreset(eps=0.1, delta=0.05).finalize()
    assert len(cs) == 0
    assert cs.query_sum() == 0.0


def test_finalize_small_input_keeps_all_rows():
    sens = SensitivityCoreset(eps=0.1, delta=0.05, seed=0)
    sens.add(1.0, (0.0,))
    sens.add(2.0, (0.0,))
    cs = sens.finalize()
    # Target size is large; n=2 ≪ target → all rows retained with weight 1.
    assert len(cs) == 2
    assert cs.query_sum() == pytest.approx(3.0)


def test_finalize_all_zero_values_falls_back_to_unit_weights():
    sens = SensitivityCoreset(eps=0.1, delta=0.05, seed=0)
    for _ in range(20):
        sens.add(0.0, (0.0,))
    cs = sens.finalize()
    # Degenerate stream → keep all rows with weight 1.
    assert len(cs) == 20
    assert cs.query_sum() == 0.0


def test_size_target_grows_with_smaller_eps():
    big = SensitivityCoreset(eps=0.1, delta=0.05).target_size()
    small = SensitivityCoreset(eps=0.02, delta=0.05).target_size()
    assert small > big


def test_sum_estimate_within_relative_error():
    rows = _stream(20_000, seed=42)
    true_sum = sum(v for v, _ in rows)
    sens = SensitivityCoreset(eps=0.05, delta=0.01, seed=1)
    for v, p in rows:
        sens.add(v, p)
    cs = sens.finalize()
    est = cs.query_sum()
    assert abs(est - true_sum) / true_sum < 0.15


def test_unbiasedness_average_over_seeds_close_to_truth():
    """Across many seeds the mean SUM estimate is within ~3% of truth."""
    rows = _stream(5_000, seed=7)
    true_sum = sum(v for v, _ in rows)
    estimates: list[float] = []
    for s in range(20):
        sens = SensitivityCoreset(eps=0.1, delta=0.05, seed=s)
        for v, p in rows:
            sens.add(v, p)
        estimates.append(sens.finalize().query_sum())
    mean_est = sum(estimates) / len(estimates)
    assert abs(mean_est - true_sum) / true_sum < 0.03


def test_size_bounded_by_target_for_large_streams():
    sens = SensitivityCoreset(eps=0.1, delta=0.05, seed=0)
    for v in range(20_000):
        sens.add(1.0 + (v % 10), (0.0,))
    cs = sens.finalize()
    assert len(cs) <= sens.target_size()


def test_sum_with_predicate_close_to_truth():
    rows = _stream(15_000, seed=11, n_cats=3)
    pred = eq_pred(0, 0.0)
    true_sum = sum(v for v, p in rows if pred(p))
    sens = SensitivityCoreset(eps=0.05, delta=0.05, seed=2)
    for v, p in rows:
        sens.add(v, p)
    cs = sens.finalize()
    est = cs.query_sum(pred)
    assert abs(est - true_sum) / max(true_sum, 1.0) < 0.25


def test_target_size_used_when_explicitly_consulted():
    sens = SensitivityCoreset(eps=0.1, delta=0.1, vc=2)
    assert sens.target_size() >= 1
