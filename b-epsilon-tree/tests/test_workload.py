"""Workload generators."""

from __future__ import annotations

from beps.workload.generator import mixed_workload, read_heavy, write_heavy


def test_mixed_count():
    ops = list(mixed_workload(n_ops=100, n_keys=10))
    assert len(ops) == 100


def test_write_heavy_fraction():
    ops = list(write_heavy(n_ops=1000, n_keys=100))
    writes = sum(1 for op, _, _ in ops if op == "put")
    assert writes / 1000 > 0.8


def test_read_heavy_fraction():
    ops = list(read_heavy(n_ops=1000, n_keys=100))
    reads = sum(1 for op, _, _ in ops if op == "get")
    assert reads / 1000 > 0.8


def test_keys_in_range():
    ops = list(mixed_workload(n_ops=1000, n_keys=50))
    for _, k, _ in ops:
        assert k.startswith(b"k")
        num = int(k[1:])
        assert 0 <= num < 50
