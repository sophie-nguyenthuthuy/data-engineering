"""Tests for the Recursive Model Index."""
import numpy as np
import pytest

from lsm_learned.indexes.rmi import RMI


def test_lookup_all_keys(small_uniform_keys):
    rmi = RMI(num_stage2=100)
    rmi.train(small_uniform_keys)
    missing = 0
    for k in small_uniform_keys[::50]:  # sample every 50th
        assert rmi.lookup(float(k)) is not None, f"key {k} not found"


def test_lookup_missing_key(small_uniform_keys):
    rmi = RMI(num_stage2=100)
    rmi.train(small_uniform_keys)
    # A key that cannot be in the set
    absent = float(small_uniform_keys[-1]) + 1_000_000
    assert rmi.lookup(absent) is None


def test_search_range_contains_key(tiny_keys):
    rmi = RMI(num_stage2=10)
    rmi.train(tiny_keys)
    for k in tiny_keys:
        lo, hi = rmi.search_range(float(k))
        assert lo <= tiny_keys.tolist().index(k) <= hi, (
            f"key {k}: idx={tiny_keys.tolist().index(k)}, range=[{lo},{hi}]"
        )


def test_stats_coverage(small_uniform_keys):
    rmi = RMI(num_stage2=50)
    rmi.train(small_uniform_keys)
    stats = rmi.stats()
    assert stats.coverage > 0.5, "less than half the stage-2 models were assigned keys"
    assert stats.mean_search_range < len(small_uniform_keys) * 0.1, (
        "search range should be much smaller than n"
    )


def test_not_trained_raises():
    rmi = RMI()
    with pytest.raises(RuntimeError):
        rmi.search_range(1.0)


def test_single_element():
    rmi = RMI(num_stage2=1)
    rmi.train(np.array([42.0]))
    assert rmi.lookup(42.0) == 0
    assert rmi.lookup(43.0) is None


def test_zipfian_distribution(small_zipfian_keys):
    rmi = RMI(num_stage2=100)
    rmi.train(small_zipfian_keys)
    # All present keys must be findable
    for k in small_zipfian_keys[::100]:
        assert rmi.lookup(float(k)) is not None


def test_rmi_num_stage2_validation():
    with pytest.raises(ValueError):
        RMI(num_stage2=0)
