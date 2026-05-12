"""Tests for HyperLogLog CRDT."""
import pytest
from src.crdts import HyperLogLogCRDT


def test_empty_count():
    h = HyperLogLogCRDT(node_id="a", precision=10)
    assert h.count() == 0


def test_single_element():
    h = HyperLogLogCRDT(node_id="a", precision=10)
    h.add("hello")
    assert h.count() > 0


def test_approximate_count_within_error():
    h = HyperLogLogCRDT(node_id="a", precision=10)
    n = 10_000
    for i in range(n):
        h.add(f"item_{i}")
    estimate = h.count()
    error = abs(estimate - n) / n
    assert error < 0.10, f"Error {error:.2%} exceeds 10% for n={n}"


def test_merge_commutativity():
    a = HyperLogLogCRDT(node_id="a", precision=10)
    b = HyperLogLogCRDT(node_id="b", precision=10)
    for i in range(1000):
        a.add(f"a_{i}")
    for i in range(1000):
        b.add(f"b_{i}")

    ab = a.merge(b)
    ba = b.merge(a)
    assert ab.count() == ba.count()


def test_merge_idempotency():
    h = HyperLogLogCRDT(node_id="a", precision=10)
    for i in range(500):
        h.add(f"x_{i}")
    assert h.merge(h).count() == h.count()


def test_merge_monotone():
    a = HyperLogLogCRDT(node_id="a", precision=10)
    b = HyperLogLogCRDT(node_id="b", precision=10)
    for i in range(500):
        a.add(f"a_{i}")

    merged = a.merge(b)
    assert merged.count() >= a.count() or merged.count() >= b.count()


def test_merge_superset_count():
    """After merging two disjoint sets the count should be higher than either."""
    a = HyperLogLogCRDT(node_id="a", precision=12)
    b = HyperLogLogCRDT(node_id="b", precision=12)
    for i in range(5000):
        a.add(f"a_{i}")
    for i in range(5000):
        b.add(f"b_{i}")

    merged = a.merge(b)
    assert merged.count() > a.count()
    assert merged.count() > b.count()


def test_precision_mismatch_raises():
    a = HyperLogLogCRDT(node_id="a", precision=10)
    b = HyperLogLogCRDT(node_id="b", precision=12)
    with pytest.raises(ValueError):
        a.merge(b)


def test_serialization_roundtrip():
    h = HyperLogLogCRDT(node_id="a", precision=10)
    for i in range(200):
        h.add(f"v_{i}")
    h2 = HyperLogLogCRDT.from_dict(h.to_dict())
    assert h2.count() == h.count()
    assert h2.precision == h.precision
