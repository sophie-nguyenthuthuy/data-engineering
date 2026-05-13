"""Coreset core API + CI tests."""

from __future__ import annotations

import math

import pytest
from hypothesis import given, settings
from hypothesis import strategies as st

from aqp.coreset.core import Coreset, WeightedRow, _inverse_phi, _zscore
from aqp.queries.predicates import eq_pred, range_pred


def test_weighted_row_rejects_negative_weight():
    with pytest.raises(ValueError):
        WeightedRow(value=1.0, payload=(0.0,), weight=-0.1)


def test_empty_coreset_queries():
    cs = Coreset.from_list([])
    assert len(cs) == 0
    assert cs.query_count() == 0.0
    assert cs.query_sum() == 0.0
    assert cs.query_avg() == 0.0


def test_query_count_sums_weights():
    cs = Coreset.from_list([WeightedRow(1.0, (1.0,), 0.5), WeightedRow(2.0, (2.0,), 1.5)])
    assert cs.query_count() == 2.0


def test_query_sum_is_weighted():
    cs = Coreset.from_list([WeightedRow(3.0, (0.0,), 2.0), WeightedRow(4.0, (0.0,), 1.0)])
    assert cs.query_sum() == pytest.approx(3.0 * 2.0 + 4.0 * 1.0)


def test_query_avg_is_ratio():
    cs = Coreset.from_list([WeightedRow(10.0, (0.0,), 1.0), WeightedRow(20.0, (0.0,), 3.0)])
    # SUM = 10 + 60 = 70; COUNT = 4; AVG = 17.5
    assert cs.query_avg() == pytest.approx(17.5)


def test_query_avg_returns_zero_on_empty_selection():
    cs = Coreset.from_list([WeightedRow(1.0, (5.0,), 1.0)])
    pred = eq_pred(0, 999.0)
    assert cs.query_avg(pred) == 0.0


def test_predicate_filters_query():
    rows = [
        WeightedRow(1.0, (0.0,), 1.0),
        WeightedRow(2.0, (1.0,), 1.0),
        WeightedRow(4.0, (1.0,), 1.0),
    ]
    cs = Coreset.from_list(rows)
    pred = eq_pred(0, 1.0)
    assert cs.query_sum(pred) == pytest.approx(6.0)
    assert cs.query_count(pred) == 2.0


def test_range_predicate_filters_query():
    rows = [WeightedRow(float(i), (float(i),), 1.0) for i in range(10)]
    cs = Coreset.from_list(rows)
    pred = range_pred(0, 2.0, 5.0)
    assert cs.query_sum(pred) == pytest.approx(2 + 3 + 4 + 5)


def test_total_weight_matches_query_count_no_predicate():
    rows = [WeightedRow(0.0, (0.0,), float(i + 1)) for i in range(5)]
    cs = Coreset.from_list(rows)
    assert cs.total_weight() == pytest.approx(cs.query_count())


def test_sum_ci_contains_estimate():
    rows = [WeightedRow(1.0, (0.0,), 1.0)] * 10
    cs = Coreset.from_list(rows)
    ci = cs.sum_confidence_interval(level=0.95)
    assert ci.lo <= ci.estimate <= ci.hi
    assert ci.level == 0.95


def test_count_ci_contains_estimate():
    rows = [WeightedRow(0.0, (0.0,), 1.0)] * 5
    cs = Coreset.from_list(rows)
    ci = cs.count_confidence_interval(level=0.99)
    assert ci.contains(ci.estimate)
    assert ci.level == 0.99


def test_empty_ci_is_zero():
    cs = Coreset.from_list([])
    ci = cs.sum_confidence_interval()
    assert ci.estimate == ci.lo == ci.hi == 0.0


def test_zscore_known_levels():
    assert _zscore(0.95) == pytest.approx(1.96)
    assert _zscore(0.99) == pytest.approx(2.5758)


def test_zscore_rejects_bad_level():
    with pytest.raises(ValueError):
        _zscore(0.0)
    with pytest.raises(ValueError):
        _zscore(1.0)


def test_inverse_phi_matches_known_quantiles():
    # Φ⁻¹(0.975) ≈ 1.96; Φ⁻¹(0.995) ≈ 2.5758
    assert abs(_inverse_phi(0.975) - 1.96) < 0.005
    assert abs(_inverse_phi(0.995) - 2.5758) < 0.005


def test_inverse_phi_monotone_in_p():
    assert _inverse_phi(0.6) < _inverse_phi(0.8) < _inverse_phi(0.95)


@settings(max_examples=30, deadline=None)
@given(
    st.lists(
        st.tuples(
            st.floats(-1e3, 1e3, allow_nan=False),
            st.floats(0.0, 10.0, allow_nan=False),
        ),
        min_size=1,
        max_size=40,
    )
)
def test_property_query_count_equals_total_weight_no_predicate(rows):
    cs = Coreset.from_list([WeightedRow(v, (0.0,), w) for v, w in rows])
    assert math.isclose(cs.query_count(), cs.total_weight(), rel_tol=1e-9, abs_tol=1e-9)
