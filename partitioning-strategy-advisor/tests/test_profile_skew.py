"""Profiler + skew + cardinality tests."""

from __future__ import annotations

import pytest

from psa.cardinality import estimate_cardinality
from psa.profile import Profiler
from psa.skew import detect_skew

# ------------------------------------------------------------- profile


def test_profile_aggregates_filter_counts():
    p = Profiler()
    p.add("SELECT * FROM o WHERE country = 'US'")
    p.add("SELECT * FROM o WHERE country = 'CA'")
    p.add("SELECT * FROM o WHERE status = 'ok'")
    profile = p.build()
    by = profile.by_name()
    assert by["country"].filter_count == 2
    assert by["status"].filter_count == 1


def test_profile_top_filter_columns():
    p = Profiler()
    for _ in range(5):
        p.add("SELECT * FROM o WHERE a = 1")
    for _ in range(2):
        p.add("SELECT * FROM o WHERE b = 2")
    profile = p.build()
    top = profile.top_filter_columns(2)
    assert top[0].name == "a"
    assert top[0].filter_count == 5


def test_profile_separates_filter_join_group():
    p = Profiler()
    p.add(
        "SELECT c.region, SUM(o.amount) FROM orders o "
        "JOIN customers c ON o.cid = c.cid WHERE c.region = 'EU' GROUP BY c.region"
    )
    profile = p.build().by_name()
    assert profile["region"].filter_count == 1
    assert profile["cid"].join_count == 1
    assert profile["region"].group_count == 1


def test_profile_n_queries_tracks_count():
    p = Profiler()
    p.consume(["SELECT * FROM o WHERE a = 1", "SELECT * FROM o WHERE b = 2"])
    assert p.build().n_queries == 2


# ----------------------------------------------------------- cardinality


def test_cardinality_empty_sample():
    e = estimate_cardinality("x", [])
    assert e.sample_size == 0
    assert e.observed_distinct == 0
    assert e.estimated_distinct == 0


def test_cardinality_unique_sample():
    e = estimate_cardinality("x", list(range(20)))
    assert e.observed_distinct == 20
    # Many singletons → estimator nudges above observed.
    assert e.estimated_distinct >= 20


def test_cardinality_with_repeated_values():
    sample = ["a"] * 10 + ["b"] * 5 + ["c"] * 2
    e = estimate_cardinality("x", sample)
    assert e.observed_distinct == 3
    assert e.estimated_distinct >= 3


def test_cardinality_rejects_empty_name():
    with pytest.raises(ValueError):
        estimate_cardinality("", [1, 2])


def test_cardinality_caps_at_sample_size():
    """Chao1 can blow up when doubletons == 0; the cap must protect us."""
    sample = list(range(50))  # all singletons
    e = estimate_cardinality("x", sample)
    assert e.estimated_distinct <= e.sample_size


# -------------------------------------------------------------- skew


def test_skew_uniform_distribution_has_low_cv():
    # 20 distinct values each appearing 50 times → top-3 share = 0.15,
    # well under the 0.5 is_skewed threshold.
    values = [f"v{i}" for i in range(20)] * 50
    s = detect_skew("col", values)
    assert s.coefficient_of_variation < 0.5
    assert s.top_3_share < 0.5
    assert not s.is_skewed()


def test_skew_heavy_hitter_flagged():
    values = ["A"] * 90 + ["B"] * 5 + ["C"] * 5
    s = detect_skew("col", values)
    assert s.coefficient_of_variation > 1.0
    assert s.is_skewed()


def test_skew_top3_share_reflects_heavy_hitters():
    values = ["A", "B", "C", "D", "E"] * 10 + ["A"] * 100
    s = detect_skew("col", values)
    assert s.top_3_share > 0.5


def test_skew_rejects_empty_name():
    with pytest.raises(ValueError):
        detect_skew("", [1])


def test_skew_empty_values_is_safe():
    s = detect_skew("x", [])
    assert s.n == 0
    assert not s.is_skewed()
