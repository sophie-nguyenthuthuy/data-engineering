"""Unit tests for vector clock causality semantics."""
import pytest
from src.models import VectorClock


def test_increment():
    vc = VectorClock()
    vc2 = vc.increment("region-a")
    assert vc2.clocks == {"region-a": 1}
    vc3 = vc2.increment("region-a")
    assert vc3.clocks == {"region-a": 2}


def test_merge_takes_max():
    vc_a = VectorClock(clocks={"region-a": 3, "region-b": 1})
    vc_b = VectorClock(clocks={"region-a": 1, "region-b": 5})
    merged = vc_a.merge(vc_b)
    assert merged.clocks == {"region-a": 3, "region-b": 5}


def test_dominates_simple():
    vc_old = VectorClock(clocks={"region-a": 1})
    vc_new = VectorClock(clocks={"region-a": 2})
    assert vc_new.dominates(vc_old)
    assert not vc_old.dominates(vc_new)


def test_dominates_multi_region():
    vc_a = VectorClock(clocks={"region-a": 3, "region-b": 2})
    vc_b = VectorClock(clocks={"region-a": 2, "region-b": 2})
    assert vc_a.dominates(vc_b)
    assert not vc_b.dominates(vc_a)


def test_concurrent():
    # Both advanced different keys — neither dominates
    vc_a = VectorClock(clocks={"region-a": 2, "region-b": 1})
    vc_b = VectorClock(clocks={"region-a": 1, "region-b": 2})
    assert vc_a.concurrent_with(vc_b)
    assert vc_b.concurrent_with(vc_a)


def test_equal_clocks_not_dominant():
    vc = VectorClock(clocks={"region-a": 1})
    assert not vc.dominates(vc)
    assert not vc.concurrent_with(vc)


def test_merge_with_missing_keys():
    vc_a = VectorClock(clocks={"region-a": 5})
    vc_b = VectorClock(clocks={"region-b": 3})
    merged = vc_a.merge(vc_b)
    assert merged.clocks == {"region-a": 5, "region-b": 3}
