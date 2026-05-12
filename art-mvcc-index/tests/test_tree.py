"""ART tree-level correctness."""

from __future__ import annotations

import random

import pytest
from hypothesis import given, settings
from hypothesis import strategies as st

from art_mvcc.art.tree import ART


class TestBasic:
    def test_empty(self, art):
        assert len(art) == 0
        assert art.get(b"x") is None

    def test_put_get(self, art):
        art.put(b"k1", "v1")
        art.put(b"k2", "v2")
        assert art.get(b"k1") == "v1"
        assert art.get(b"k2") == "v2"
        assert art.get(b"k3") is None
        assert len(art) == 2

    def test_overwrite(self, art):
        art.put(b"k", "first")
        art.put(b"k", "second")
        assert art.get(b"k") == "second"
        assert len(art) == 1

    def test_delete_existing(self, art):
        art.put(b"k", "v")
        assert art.delete(b"k")
        assert art.get(b"k") is None
        assert len(art) == 0

    def test_delete_missing(self, art):
        assert not art.delete(b"missing")

    def test_in_operator(self, art):
        art.put(b"k", "v")
        assert b"k" in art
        assert b"x" not in art

    def test_reject_missing_sentinel(self, art):
        from art_mvcc.art.nodes import MISSING
        with pytest.raises(ValueError):
            art.put(b"k", MISSING)


class TestPathCompression:
    def test_shared_prefix(self, art):
        for k, v in [(b"hello", 1), (b"help", 2), (b"helmet", 3)]:
            art.put(k, v)
        assert art.get(b"hello") == 1
        assert art.get(b"help") == 2
        assert art.get(b"helmet") == 3
        # Path compression keeps depth low — root has prefix "hel" + child fanout
        assert art.depth() <= 4

    def test_long_unique_prefix(self, art):
        art.put(b"a" * 1000, "deep")
        assert art.get(b"a" * 1000) == "deep"

    def test_divergent_prefix_split(self, art):
        art.put(b"hello world", 1)
        # Insert a key that diverges before "world"
        art.put(b"help me", 2)
        assert art.get(b"hello world") == 1
        assert art.get(b"help me") == 2


class TestIteration:
    def test_items_sorted(self, art):
        keys = [b"banana", b"apple", b"cherry", b"avocado", b"blueberry"]
        for k in keys:
            art.put(k, k.decode())
        result = list(art.items())
        assert [k for k, _ in result] == sorted(keys)

    def test_iter_prefix(self, art):
        for k in [b"helmet", b"help", b"hello", b"world", b"hello!"]:
            art.put(k, True)
        seen = {k for k, _ in art.iter_prefix(b"hel")}
        assert seen == {b"helmet", b"help", b"hello", b"hello!"}

    def test_iter_prefix_empty(self, art):
        for k in [b"abc", b"def"]:
            art.put(k, True)
        assert list(art.iter_prefix(b"xyz")) == []

    def test_iter_range(self, art):
        for i in range(10):
            art.put(bytes([0x30 + i]), i)
        result = list(art.iter_range(b"\x33", b"\x37"))
        assert [k for k, _ in result] == [b"\x33", b"\x34", b"\x35", b"\x36"]


class TestNodeShape:
    def test_grows_through_node_types(self, art):
        # 300 distinct first bytes forces Node256 at root
        for i in range(300):
            art.put(bytes([i // 256, i % 256]), i)
        shape = art.node_count_by_kind()
        assert "Node4" in shape
        # At least one of Node48/Node256 must appear with 300 root-level keys
        assert any(k in shape for k in ["Node48", "Node256"])


class TestRandomWorkload:
    @pytest.mark.parametrize("seed", [0, 1, 2, 3, 4])
    def test_matches_reference_dict(self, seed, art):
        rng = random.Random(seed)
        ref: dict[bytes, int] = {}
        for _ in range(2000):
            op = rng.choice(["put", "put", "put", "delete"])  # write-heavy
            klen = rng.randint(1, 8)
            k = bytes(rng.randint(0, 255) for _ in range(klen))
            if op == "put":
                v = rng.randint(0, 10_000)
                art.put(k, v)
                ref[k] = v
            else:
                if art.delete(k):
                    ref.pop(k, None)
        assert len(art) == len(ref)
        for k, v in ref.items():
            assert art.get(k) == v


# ---------------------------------------------------------------------------
# Hypothesis property tests
# ---------------------------------------------------------------------------


@st.composite
def _ops(draw) -> list[tuple[str, bytes, int]]:
    n = draw(st.integers(min_value=1, max_value=80))
    return [
        (
            draw(st.sampled_from(["put", "put", "put", "delete"])),
            draw(st.binary(min_size=1, max_size=6)),
            draw(st.integers(min_value=0, max_value=1000)),
        )
        for _ in range(n)
    ]


@given(operations=_ops())
@settings(max_examples=80, deadline=None)
def test_art_matches_dict_under_arbitrary_ops(operations: list[tuple[str, bytes, int]]):
    art = ART()
    ref: dict[bytes, int] = {}
    for op, k, v in operations:
        if op == "put":
            art.put(k, v)
            ref[k] = v
        elif op == "delete":
            if art.delete(k):
                ref.pop(k, None)
    assert len(art) == len(ref)
    for k, v in ref.items():
        assert art.get(k) == v
    # Plus: art keys exactly match ref keys
    assert {k for k, _ in art.items()} == set(ref.keys())
