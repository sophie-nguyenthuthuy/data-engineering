"""Flush behaviour: cascading flushes, buffered messages staying visible."""

from __future__ import annotations

from beps.tree.tree import BEpsilonTree


def test_buffered_writes_visible_before_flush(small_tree):
    """Newly written keys must be observable even when they sit in buffers."""
    for i in range(20):
        small_tree.put(f"k{i:04d}".encode(), i)
    for i in range(20):
        assert small_tree.get(f"k{i:04d}".encode()) == i


def test_buffer_drains_under_pressure():
    """Many writes must eventually flush down — root buffer doesn't grow
    unboundedly."""
    t = BEpsilonTree(node_size=8, epsilon=0.5)
    for i in range(500):
        t.put(f"k{i:04d}".encode(), i)
    # The total buffer should be bounded by O(n_internal_nodes * buffer_cap)
    # We just verify it's not the full input.
    assert t.buffer_total() < 500


def test_force_flush_empties_buffers():
    t = BEpsilonTree(node_size=8, epsilon=0.5)
    for i in range(300):
        t.put(f"k{i:04d}".encode(), i)
    t.flush_all()
    assert t.buffer_total() == 0
    # Everything still retrievable
    for i in range(300):
        assert t.get(f"k{i:04d}".encode()) == i


def test_descend_order_of_flush_doesnt_lose_messages():
    """Regression: an earlier MVP lost messages when children split during
    flush because indices shifted. We process descending now."""
    t = BEpsilonTree(node_size=4, epsilon=0.5)
    # Tight node_size forces many splits during cascading flushes
    expected = {f"k{i:04d}".encode(): i for i in range(200)}
    for k, v in expected.items():
        t.put(k, v)
    for k, v in expected.items():
        assert t.get(k) == v


def test_delete_through_buffer():
    t = BEpsilonTree(node_size=4, epsilon=0.5)
    t.put(b"k", "v1")
    t.flush_all()    # ensure value is at leaf
    t.delete(b"k")   # delete via buffer
    assert t.get(b"k") is None


def test_overwrite_through_buffer():
    t = BEpsilonTree(node_size=4, epsilon=0.5)
    t.put(b"k", "v1")
    t.flush_all()
    t.put(b"k", "v2")
    # Buffered v2 must take precedence over leaf v1
    assert t.get(b"k") == "v2"
    t.flush_all()
    assert t.get(b"k") == "v2"
