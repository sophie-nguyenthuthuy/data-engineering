"""Tests for OR-Set CRDT properties."""
from src.crdts import ORSet


def test_add_and_contains():
    s = ORSet(node_id="a")
    s.add("apple")
    assert s.contains("apple")


def test_remove():
    s = ORSet(node_id="a")
    s.add("banana")
    s.remove("banana")
    assert not s.contains("banana")


def test_add_wins_over_concurrent_remove():
    """
    Concurrent add (node b) and remove (node a) of same element.
    After merge the element is present because b added a new token.
    """
    a = ORSet(node_id="a")
    a.add("x")

    b = ORSet(node_id="b")
    b = b.merge(a)       # b observes x

    a.remove("x")        # a removes (tombstones a's token)
    b.add("x")           # b concurrently re-adds (new token)

    merged = a.merge(b)
    assert merged.contains("x")  # b's new token survives


def test_remove_of_observed_tokens_wins():
    """
    If node b removes x before a re-adds it, the remove wins for that token.
    """
    a = ORSet(node_id="a")
    a.add("y")

    b = ORSet(node_id="b")
    b = b.merge(a)   # b sees y
    b.remove("y")    # b removes (tombstones a's token)
    # a does NOT re-add — so no surviving tokens after merge
    merged = a.merge(b)
    assert not merged.contains("y")


def test_merge_commutativity():
    a = ORSet(node_id="a")
    a.add("p")
    a.add("q")

    b = ORSet(node_id="b")
    b.add("q")
    b.add("r")

    ab = a.merge(b)
    ba = b.merge(a)
    assert ab.elements() == ba.elements()


def test_merge_associativity():
    a = ORSet(node_id="a")
    a.add("1")
    b = ORSet(node_id="b")
    b.add("2")
    c = ORSet(node_id="c")
    c.add("3")

    assert a.merge(b).merge(c).elements() == a.merge(b.merge(c)).elements()


def test_merge_idempotency():
    a = ORSet(node_id="a")
    a.add("z")
    assert a.merge(a).elements() == a.elements()


def test_len():
    s = ORSet(node_id="a")
    s.add("a")
    s.add("b")
    s.add("a")  # duplicate; same element, new token
    assert len(s) == 2


def test_serialization_roundtrip():
    s = ORSet(node_id="x")
    s.add("hello")
    s.add("world")
    s2 = ORSet.from_dict(s.to_dict())
    assert s2.elements() == s.elements()
