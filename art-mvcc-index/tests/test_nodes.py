"""Node-level tests: each node type's CRUD + adaptive transitions."""

from __future__ import annotations

from art_mvcc.art.nodes import (
    Node4,
    Node16,
    Node48,
    Node256,
)


class TestNode4:
    def test_empty(self):
        n = Node4()
        assert n.n_children == 0
        assert n.find_child(0x41) is None
        assert n.children() == []

    def test_add_and_lookup(self):
        n = Node4()
        a, b, c = Node4(), Node4(), Node4()
        n = n.add_child(0x41, a)
        n = n.add_child(0x42, b)
        n = n.add_child(0x40, c)
        assert n.find_child(0x41) is a
        assert n.find_child(0x42) is b
        assert n.find_child(0x40) is c
        assert n.find_child(0x99) is None
        # children() returns sorted
        kbs = [k for k, _ in n.children()]
        assert kbs == sorted(kbs)

    def test_replace_doesnt_grow(self):
        n: Node4 = Node4()
        a, b = Node4(), Node4()
        n = n.add_child(0x41, a)  # type: ignore[assignment]
        n2 = n.add_child(0x41, b)
        assert n2 is n
        assert n.find_child(0x41) is b

    def test_widens_to_node16(self):
        n = Node4()
        children = [Node4() for _ in range(5)]
        for i, c in enumerate(children):
            n = n.add_child(0x10 + i, c)
        assert isinstance(n, Node16)
        for i, c in enumerate(children):
            assert n.find_child(0x10 + i) is c

    def test_remove_to_empty(self):
        n: Node4 = Node4()
        a = Node4()
        n = n.add_child(0x41, a)  # type: ignore[assignment]
        result = n.remove_child(0x41)
        assert result is None  # node-with-no-value collapses


class TestNode16:
    def test_keeps_keys_sorted(self):
        n: Node16 = Node16()
        for byte in [0x40, 0x60, 0x50, 0x42, 0x55]:
            n = n.add_child(byte, Node4())  # type: ignore[assignment]
        bytes_seen = [k for k, _ in n.children()]
        assert bytes_seen == sorted(bytes_seen)

    def test_widens_to_node48(self):
        n: Node16 = Node16()
        for i in range(17):
            n = n.add_child(i, Node4())  # type: ignore[assignment]
        assert isinstance(n, Node48)
        for i in range(17):
            assert n.find_child(i) is not None

    def test_narrows_to_node4(self):
        n: Node16 = Node16()
        kids = [Node4() for _ in range(8)]
        for i, c in enumerate(kids):
            n = n.add_child(i, c)  # type: ignore[assignment]
        # Remove enough to fall below MIN=4
        for i in range(5):
            n = n.remove_child(i)  # type: ignore[assignment]
        assert isinstance(n, Node4)


class TestNode48:
    def test_index_bookkeeping(self):
        n: Node48 = Node48()
        kids = [Node4() for _ in range(30)]
        for i, c in enumerate(kids):
            n = n.add_child(i, c)  # type: ignore[assignment]
        assert n.n_children == 30
        for i, c in enumerate(kids):
            assert n.find_child(i) is c

    def test_widens_to_node256(self):
        n: Node48 = Node48()
        for i in range(49):
            n = n.add_child(i, Node4())  # type: ignore[assignment]
        assert isinstance(n, Node256)
        for i in range(49):
            assert n.find_child(i) is not None

    def test_narrows_to_node16(self):
        n: Node48 = Node48()
        for i in range(20):
            n = n.add_child(i, Node4())  # type: ignore[assignment]
        # MIN=13, so removing down to 12 narrows
        for i in range(8):
            n = n.remove_child(i)  # type: ignore[assignment]
        assert isinstance(n, Node16)


class TestNode256:
    def test_direct_addressing(self):
        n = Node256()
        for byte in [0, 100, 255, 128]:
            n = n.add_child(byte, Node4())  # type: ignore[assignment]
        for byte in [0, 100, 255, 128]:
            assert n.find_child(byte) is not None
        assert n.find_child(50) is None

    def test_narrows_to_node48(self):
        n: Node256 = Node256()
        for i in range(60):
            n = n.add_child(i, Node4())  # type: ignore[assignment]
        # MIN=49, so removing 20 → 40 narrows
        for i in range(20):
            n = n.remove_child(i)  # type: ignore[assignment]
        assert isinstance(n, Node48)


def test_terminator_keeps_node_alive_after_remove_all_children():
    n: Node4 = Node4(prefix=b"hello", value="leaf")
    child = Node4()
    n = n.add_child(0x41, child)  # type: ignore[assignment]
    result = n.remove_child(0x41)
    # No children, but has value → must NOT collapse
    assert result is n
    assert n.is_terminator
