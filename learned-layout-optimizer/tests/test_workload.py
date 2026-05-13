"""WorkloadProfile + drift tests."""

from __future__ import annotations

import pytest

from llo.workload.drift import DriftDetector
from llo.workload.profile import Query, WorkloadProfile


def test_profile_rejects_empty_columns():
    with pytest.raises(ValueError):
        WorkloadProfile(columns=[])


def test_profile_rejects_duplicate_columns():
    with pytest.raises(ValueError):
        WorkloadProfile(columns=["a", "a"])


def test_observe_unknown_column_raises():
    p = WorkloadProfile(columns=["a"])
    with pytest.raises(ValueError):
        p.observe(Query({"b": ("=", 1.0)}))


def test_freq_zero_before_observations():
    p = WorkloadProfile(columns=["a", "b"])
    assert p.freq("a") == 0.0


def test_freq_tracks_basic_count():
    p = WorkloadProfile(columns=["a", "b"])
    for _ in range(10):
        p.observe(Query({"a": ("=", 1.0)}))
    for _ in range(3):
        p.observe(Query({"b": ("range", 0.0, 5.0)}))
    assert p.freq("a") > p.freq("b") > 0


def test_range_fraction():
    p = WorkloadProfile(columns=["a"])
    for _ in range(4):
        p.observe(Query({"a": ("range", 0.0, 1.0)}))
    for _ in range(6):
        p.observe(Query({"a": ("=", 1.0)}))
    assert p.range_fraction("a") == pytest.approx(0.4)


def test_top_cols_orders_by_freq():
    p = WorkloadProfile(columns=["a", "b", "c"])
    for _ in range(5):
        p.observe(Query({"b": ("=", 1.0)}))
    for _ in range(2):
        p.observe(Query({"a": ("=", 1.0)}))
    assert p.top_cols(2) == ["b", "a"]


def test_top_cols_excludes_zero_freq():
    p = WorkloadProfile(columns=["a", "b", "c"])
    for _ in range(5):
        p.observe(Query({"a": ("=", 1.0)}))
    assert p.top_cols(3) == ["a"]


def test_co_occurrence_counts():
    p = WorkloadProfile(columns=["a", "b", "c"])
    for _ in range(4):
        p.observe(Query({"a": ("=", 1.0), "b": ("=", 2.0)}))
    assert p.co_occurrence("a", "b") == 4
    assert p.co_occurrence("a", "c") == 0


def test_set_domain_validates():
    p = WorkloadProfile(columns=["a"])
    with pytest.raises(ValueError):
        p.set_domain("b", 0, 1)
    with pytest.raises(ValueError):
        p.set_domain("a", 1, 1)


def test_mean_selectivity_with_domain():
    p = WorkloadProfile(columns=["a"])
    p.set_domain("a", 0.0, 100.0)
    p.observe(Query({"a": ("range", 0.0, 10.0)}))  # 10% width
    p.observe(Query({"a": ("range", 40.0, 60.0)}))  # 20% width
    assert p.mean_selectivity("a") == pytest.approx(0.15)


def test_snapshot_returns_flat_vector():
    p = WorkloadProfile(columns=["a", "b"])
    p.observe(Query({"a": ("=", 1.0)}))
    snap = p.snapshot()
    assert "freq:a" in snap and "range:a" in snap
    assert snap["freq:a"] == 1.0


# ----------------------------------------------------------------- drift


def test_drift_zero_when_uncalibrated_returns_zero():
    p = WorkloadProfile(columns=["a", "b"])
    d = DriftDetector(threshold=0.1)
    assert d.score(p) == 0.0


def test_drift_threshold_validates():
    with pytest.raises(ValueError):
        DriftDetector(threshold=0.0)
    with pytest.raises(ValueError):
        DriftDetector(threshold=1.5)


def test_drift_zero_after_calibration_then_unchanged():
    p = WorkloadProfile(columns=["a", "b"])
    for _ in range(10):
        p.observe(Query({"a": ("=", 1.0)}))
    d = DriftDetector(threshold=0.1)
    d.calibrate(p)
    assert d.score(p) == pytest.approx(0.0)


def test_drift_detects_full_swap():
    p = WorkloadProfile(columns=["a", "b"])
    for _ in range(10):
        p.observe(Query({"a": ("=", 1.0)}))
    d = DriftDetector(threshold=0.1)
    d.calibrate(p)
    for _ in range(40):
        p.observe(Query({"b": ("=", 2.0)}))
    assert d.score(p) > 0.1
    assert d.has_drifted(p)
