"""Tests for PNCounter CRDT properties."""
import pytest
from src.crdts import PNCounter


def test_increment_and_decrement():
    c = PNCounter(node_id="a")
    c.increment(10)
    c.decrement(3)
    assert c.value() == 7


def test_value_can_go_negative():
    c = PNCounter(node_id="a")
    c.decrement(5)
    assert c.value() == -5


def test_merge_commutativity():
    a = PNCounter(node_id="a")
    a.increment(10)
    a.decrement(2)

    b = PNCounter(node_id="b")
    b.increment(5)
    b.decrement(1)

    assert a.merge(b).value() == b.merge(a).value()


def test_merge_associativity():
    a = PNCounter(node_id="a")
    a.increment(4)
    b = PNCounter(node_id="b")
    b.decrement(2)
    c = PNCounter(node_id="c")
    c.increment(6)

    left = a.merge(b).merge(c)
    right = a.merge(b.merge(c))
    assert left.value() == right.value()


def test_merge_idempotency():
    a = PNCounter(node_id="a")
    a.increment(7)
    a.decrement(3)
    assert a.merge(a).value() == a.value()


def test_concurrent_updates_converge():
    # Two nodes start from the same state and diverge
    a = PNCounter(node_id="a")
    a.increment(100)

    b = PNCounter(node_id="b")
    b = b.merge(a)  # b receives a's state

    a.increment(20)  # a advances
    b.decrement(10)  # b advances independently

    merged_ab = a.merge(b)
    merged_ba = b.merge(a)
    assert merged_ab.value() == merged_ba.value()
    assert merged_ab.value() == 110  # 100+20-10


def test_serialization_roundtrip():
    c = PNCounter(node_id="x")
    c.increment(50)
    c.decrement(15)
    c2 = PNCounter.from_dict(c.to_dict())
    assert c2.value() == 35
