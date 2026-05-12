"""Tests for the augmented AVL interval tree."""
import pytest
from temporal_join.interval_tree import IntervalTree


def build(*pairs) -> IntervalTree:
    """Helper: IntervalTree from (key, value) pairs."""
    t = IntervalTree()
    for k, v in pairs:
        t.insert(k, v)
    return t


class TestBasicOperations:
    def test_empty_tree_len(self):
        assert len(IntervalTree()) == 0

    def test_empty_tree_bool(self):
        assert not IntervalTree()

    def test_insert_single(self):
        t = build((10, "a"))
        assert len(t) == 1
        assert 10 in t

    def test_insert_multiple_distinct(self):
        t = build((10, "a"), (20, "b"), (5, "c"))
        assert len(t) == 3

    def test_duplicate_key(self):
        t = build((10, "a"), (10, "b"))
        assert len(t) == 1  # one distinct key
        res = t.predecessor(10)
        assert res is not None
        assert res[0] == 10
        assert set(res[1]) == {"a", "b"}

    def test_contains_missing(self):
        t = build((10, "a"), (20, "b"))
        assert 15 not in t

    def test_delete_value(self):
        t = build((10, "a"), (10, "b"))
        t.delete(10, "a")
        res = t.predecessor(10)
        assert res[1] == ["b"]

    def test_delete_last_value_removes_key(self):
        t = build((10, "a"))
        t.delete(10, "a")
        assert 10 not in t
        assert len(t) == 0

    def test_delete_nonexistent_value_noop(self):
        t = build((10, "a"))
        t.delete(10, "MISSING")  # should not raise
        assert 10 in t

    def test_delete_nonexistent_key_noop(self):
        t = build((10, "a"))
        t.delete(99, "a")  # should not raise
        assert 10 in t


class TestPredecessor:
    def test_exact_match(self):
        t = build((10, "a"), (20, "b"), (30, "c"))
        k, vals = t.predecessor(20)
        assert k == 20

    def test_between_keys(self):
        t = build((10, "a"), (30, "c"))
        k, vals = t.predecessor(25)
        assert k == 10

    def test_below_all_keys(self):
        t = build((10, "a"), (20, "b"))
        assert t.predecessor(5) is None

    def test_above_all_keys(self):
        t = build((10, "a"), (20, "b"))
        k, _ = t.predecessor(100)
        assert k == 20

    def test_empty_tree(self):
        assert IntervalTree().predecessor(42) is None


class TestSuccessor:
    def test_exact_match(self):
        t = build((10, "a"), (20, "b"), (30, "c"))
        k, _ = t.successor(20)
        assert k == 20

    def test_between_keys(self):
        t = build((10, "a"), (30, "c"))
        k, _ = t.successor(15)
        assert k == 30

    def test_above_all_keys(self):
        t = build((10, "a"), (20, "b"))
        assert t.successor(100) is None

    def test_empty_tree(self):
        assert IntervalTree().successor(0) is None


class TestRangeQuery:
    def test_full_range(self):
        t = build((10, "a"), (20, "b"), (30, "c"))
        results = t.range_query(0, 100)
        assert [k for k, _ in results] == [10, 20, 30]

    def test_partial_range(self):
        t = build((10, "a"), (20, "b"), (30, "c"), (40, "d"))
        results = t.range_query(15, 35)
        assert [k for k, _ in results] == [20, 30]

    def test_exact_bounds(self):
        t = build((10, "a"), (20, "b"), (30, "c"))
        results = t.range_query(10, 30)
        assert [k for k, _ in results] == [10, 20, 30]

    def test_empty_range(self):
        t = build((10, "a"), (20, "b"))
        assert t.range_query(12, 18) == []

    def test_empty_tree(self):
        assert IntervalTree().range_query(0, 100) == []

    def test_sorted_order(self):
        t = IntervalTree()
        for k in [50, 10, 30, 70, 20]:
            t.insert(k, str(k))
        results = t.range_query(0, 100)
        keys = [k for k, _ in results]
        assert keys == sorted(keys)


class TestBalancing:
    """Ensure the AVL tree stays balanced under skewed insertion patterns."""

    def test_ascending_insert(self):
        t = IntervalTree()
        for i in range(100):
            t.insert(i, i)
        assert len(t) == 100
        for i in range(100):
            assert i in t

    def test_descending_insert(self):
        t = IntervalTree()
        for i in range(99, -1, -1):
            t.insert(i, i)
        assert len(t) == 100

    def test_interleaved_delete(self):
        t = IntervalTree()
        for i in range(50):
            t.insert(i * 2, i)
        for i in range(50):
            t.delete(i * 2, i)
        assert len(t) == 0
        assert t.predecessor(100) is None


class TestMinMax:
    def test_min_max(self):
        t = build((5, "a"), (1, "b"), (9, "c"), (3, "d"))
        assert t.min_key() == 1
        assert t.max_key() == 9

    def test_empty(self):
        assert IntervalTree().min_key() is None
        assert IntervalTree().max_key() is None
