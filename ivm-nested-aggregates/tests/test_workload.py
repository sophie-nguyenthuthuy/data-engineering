"""Workload generators."""

from __future__ import annotations

from ivm.workload import burst_workload, mixed_workload, sliding_workload


def test_mixed_yields_expected_count():
    ops = list(mixed_workload(n_ops=100, n_keys=4))
    assert len(ops) == 100


def test_mixed_insert_fraction():
    ops = list(mixed_workload(n_ops=1000, n_keys=4, insert_fraction=0.8))
    inserts = sum(1 for op, *_ in ops if op == "insert")
    assert 700 < inserts < 900


def test_burst_workload_has_bursts():
    ops = list(burst_workload(n_ops=200, n_keys=4, burst_size=50))
    # First 50 should all be inserts
    first_burst = [op for op, *_ in ops[:50]]
    assert first_burst.count("insert") == 50


def test_sliding_workload_emits_deletes():
    ops = list(sliding_workload(n_ops=500, n_partitions=2, window_size=10))
    deletes = sum(1 for op, *_ in ops if op == "delete")
    # Each partition window has > 10 items → some deletes should fire
    assert deletes > 0
