"""Tests for G-Counter CRDT properties."""
import pytest
from src.crdts import GCounter


def make(node_id, **slots):
    c = GCounter(node_id=node_id)
    c.counters = dict(slots)
    return c


def test_increment_increases_value():
    c = GCounter(node_id="a")
    c.increment(5)
    assert c.value() == 5


def test_increment_only_own_slot():
    c = GCounter(node_id="a")
    c.increment(3)
    assert c.counters["a"] == 3
    assert all(k == "a" for k in c.counters)


def test_no_negative_increment():
    c = GCounter(node_id="a")
    with pytest.raises(ValueError):
        c.increment(-1)


def test_merge_takes_max():
    a = make("a", a=5, b=2)
    b = make("b", a=3, b=7, c=1)
    m = a.merge(b)
    assert m.counters == {"a": 5, "b": 7, "c": 1}


def test_merge_commutativity():
    a = make("a", a=5, b=2)
    b = make("b", a=3, b=7, c=1)
    assert a.merge(b).value() == b.merge(a).value()
    assert a.merge(b).counters == b.merge(a).counters


def test_merge_associativity():
    a = make("a", a=4)
    b = make("b", b=3)
    c = make("c", c=5)
    assert a.merge(b).merge(c).value() == a.merge(b.merge(c)).value()


def test_merge_idempotency():
    a = make("a", a=5, b=2)
    assert a.merge(a).value() == a.value()
    assert a.merge(a).counters == a.counters


def test_partial_order():
    a = make("a", a=2, b=1)
    b = make("b", a=3, b=2)
    assert a <= b
    assert not b <= a


def test_merge_is_least_upper_bound():
    a = make("a", a=5, b=2)
    b = make("b", a=3, b=7)
    m = a.merge(b)
    assert a <= m
    assert b <= m


def test_serialization_roundtrip():
    c = GCounter(node_id="x")
    c.increment(42)
    c2 = GCounter.from_dict(c.to_dict())
    assert c2.value() == 42
    assert c2.node_id == "x"
