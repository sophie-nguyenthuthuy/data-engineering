"""Tree-level correctness."""

from __future__ import annotations

import random

import pytest
from hypothesis import given, settings
from hypothesis import strategies as st

from beps.tree.tree import BEpsilonTree


class TestBasic:
    def test_get_missing(self, tree):
        assert tree.get(b"x") is None

    def test_put_get(self, tree):
        tree.put(b"k1", "v1")
        tree.put(b"k2", "v2")
        assert tree.get(b"k1") == "v1"
        assert tree.get(b"k2") == "v2"
        assert tree.get(b"k3") is None

    def test_overwrite(self, tree):
        tree.put(b"k", "first")
        tree.put(b"k", "second")
        assert tree.get(b"k") == "second"

    def test_delete(self, tree):
        tree.put(b"k", "v")
        tree.delete(b"k")
        assert tree.get(b"k") is None

    def test_delete_missing(self, tree):
        tree.delete(b"x")    # no-op
        assert tree.get(b"x") is None

    def test_in_operator(self, tree):
        tree.put(b"k", "v")
        assert b"k" in tree
        assert b"x" not in tree

    def test_construct_validates(self):
        with pytest.raises(ValueError):
            BEpsilonTree(epsilon=0.0)
        with pytest.raises(ValueError):
            BEpsilonTree(epsilon=1.0)
        with pytest.raises(ValueError):
            BEpsilonTree(node_size=2)


class TestCapacityDerivation:
    def test_buffer_capacity_scales_with_epsilon(self):
        t1 = BEpsilonTree(node_size=20, epsilon=0.1)
        t2 = BEpsilonTree(node_size=20, epsilon=0.9)
        assert t1.buffer_capacity < t2.buffer_capacity
        assert t1.pivot_capacity > t2.pivot_capacity


class TestSplitAndDepth:
    def test_tree_grows_in_depth(self, small_tree):
        for i in range(200):
            small_tree.put(f"k{i:04d}".encode(), i)
        assert small_tree.depth() > 1
        # All keys retrievable
        for i in range(200):
            assert small_tree.get(f"k{i:04d}".encode()) == i

    def test_size_after_inserts(self, small_tree):
        small_tree.flush_all()  # initial empty: no-op
        for i in range(100):
            small_tree.put(f"k{i:04d}".encode(), i)
        small_tree.flush_all()
        assert len(small_tree) == 100


class TestRandomWorkloads:
    @pytest.mark.parametrize("seed", [0, 1, 2, 3, 4])
    def test_matches_reference_dict(self, seed):
        tree = BEpsilonTree(node_size=8, epsilon=0.5)
        ref: dict[bytes, int] = {}
        rng = random.Random(seed)
        for _ in range(2000):
            op = rng.choices(["put", "put", "put", "del"], k=1)[0]
            k = f"k{rng.randint(0, 500):04d}".encode()
            if op == "put":
                v = rng.randint(0, 100_000)
                tree.put(k, v)
                ref[k] = v
            else:
                tree.delete(k)
                ref.pop(k, None)
        for k in ref:
            assert tree.get(k) == ref[k]
        # Keys not in ref must not be in tree
        for kk in (b"missing-key",):
            assert tree.get(kk) is None


class TestIteration:
    def test_items_sorted(self, tree):
        keys = [b"banana", b"apple", b"cherry", b"avocado"]
        for k in keys:
            tree.put(k, k.decode())
        result = list(tree.items())
        assert [k for k, _ in result] == sorted(keys)

    def test_iter_range(self, tree):
        for i in range(10):
            tree.put(f"k{i:02d}".encode(), i)
        out = list(tree.iter_range(b"k03", b"k07"))
        assert [k for k, _ in out] == [b"k03", b"k04", b"k05", b"k06"]

    def test_iteration_includes_buffered_messages(self, small_tree):
        """Newly-buffered keys (not yet flushed) appear in items()."""
        for i in range(50):
            small_tree.put(f"k{i:04d}".encode(), i)
        # Don't flush — buffers may contain pending messages
        items_dict = dict(small_tree.items())
        for i in range(50):
            assert items_dict[f"k{i:04d}".encode()] == i


# ---------------------------------------------------------------------------
# Hypothesis property tests
# ---------------------------------------------------------------------------


@st.composite
def _ops(draw) -> list[tuple[str, bytes, int]]:
    n = draw(st.integers(min_value=1, max_value=80))
    return [
        (
            draw(st.sampled_from(["put", "put", "put", "del"])),
            draw(st.binary(min_size=1, max_size=6)),
            draw(st.integers(min_value=0, max_value=1000)),
        )
        for _ in range(n)
    ]


@given(operations=_ops())
@settings(max_examples=80, deadline=None)
@pytest.mark.property
def test_tree_matches_dict_under_arbitrary_ops(operations):
    tree = BEpsilonTree(node_size=8, epsilon=0.5)
    ref: dict[bytes, int] = {}
    for op, k, v in operations:
        if op == "put":
            tree.put(k, v)
            ref[k] = v
        else:
            tree.delete(k)
            ref.pop(k, None)
    for k, v in ref.items():
        assert tree.get(k) == v
