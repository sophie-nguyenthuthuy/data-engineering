"""Replay-reward tests."""

from __future__ import annotations

import numpy as np
import pytest

from llo.policy.bandit import Action
from llo.replay.pages import apply_layout, expected_pages, pages_scanned, reward
from llo.workload.profile import Query


def test_apply_layout_noop_is_identity():
    data = np.array([[3, 1], [1, 2], [2, 0]], dtype=np.int64)
    perm = apply_layout(data, ["x", "y"], Action("noop", ()))
    assert list(perm) == [0, 1, 2]


def test_apply_layout_sortkey_orders_rows():
    data = np.array([[3, 1], [1, 2], [2, 0]], dtype=np.int64)
    perm = apply_layout(data, ["x", "y"], Action("sortkey", ("x",)))
    assert list(data[perm][:, 0]) == [1, 2, 3]


def test_apply_layout_unknown_column_raises():
    data = np.array([[1, 2]], dtype=np.int64)
    with pytest.raises(ValueError):
        apply_layout(data, ["x", "y"], Action("sortkey", ("z",)))


def test_pages_scanned_zero_for_empty_data():
    data = np.zeros((0, 2), dtype=np.int64)
    perm = np.zeros(0, dtype=np.int64)
    q = Query({"x": ("=", 1.0)})
    assert pages_scanned(data, perm, ["x", "y"], q) == 0


def test_expected_pages_zero_for_empty_workload():
    data = np.zeros((4, 2), dtype=np.int64)
    assert expected_pages(data, ["x", "y"], Action("noop", ()), []) == 0.0


def test_zorder_reduces_pages_for_box_query():
    rng = np.random.default_rng(0)
    n = 1000
    data = rng.integers(0, 100, size=(n, 2), dtype=np.int64)
    q = Query({"x": ("range", 40.0, 60.0), "y": ("range", 40.0, 60.0)})
    pages_noop = expected_pages(data, ["x", "y"], Action("noop", ()), [q])
    pages_zord = expected_pages(data, ["x", "y"], Action("zorder", ("x", "y")), [q])
    assert pages_zord < pages_noop


def test_hilbert_beats_or_matches_zorder_for_box_query():
    rng = np.random.default_rng(1)
    n = 1024
    data = rng.integers(0, 64, size=(n, 2), dtype=np.int64)
    q = Query({"x": ("range", 20.0, 40.0), "y": ("range", 20.0, 40.0)})
    pages_z = expected_pages(data, ["x", "y"], Action("zorder", ("x", "y")), [q])
    pages_h = expected_pages(data, ["x", "y"], Action("hilbert", ("x", "y")), [q])
    # Locality property: Hilbert ≤ Z-order on contiguous box (with slack for tie cases).
    assert pages_h <= pages_z + 1


def test_sortkey_beats_noop_for_equality_query():
    rng = np.random.default_rng(2)
    n = 800
    data = rng.integers(0, 50, size=(n, 2), dtype=np.int64)
    q = Query({"x": ("=", 25.0)})
    pages_noop = expected_pages(data, ["x", "y"], Action("noop", ()), [q] * 20)
    pages_sk = expected_pages(data, ["x", "y"], Action("sortkey", ("x",)), [q] * 20)
    assert pages_sk < pages_noop


def test_reward_zero_on_empty_workload():
    data = np.zeros((4, 2), dtype=np.int64)
    assert reward(data, ["x", "y"], Action("noop", ()), []) == 0.0


def test_reward_penalises_pointless_rewrite():
    rng = np.random.default_rng(7)
    data = rng.integers(0, 100, size=(500, 2), dtype=np.int64)
    q = Query({"x": ("=", 50.0)})
    workload = [q] * 100
    r_noop = reward(data, ["x", "y"], Action("noop", ()), workload)
    r_useless = reward(data, ["x", "y"], Action("hilbert", ("x", "y")), workload, io_cost=10_000.0)
    assert r_useless < r_noop


def test_apply_layout_hilbert_nd_path():
    rng = np.random.default_rng(11)
    data = rng.integers(0, 16, size=(64, 3), dtype=np.int64)
    perm = apply_layout(data, ["a", "b", "c"], Action("hilbert", ("a", "b", "c")))
    assert sorted(perm.tolist()) == list(range(64))
