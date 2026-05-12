"""Node primitives."""

from __future__ import annotations

from beps.tree.message import Message, Op
from beps.tree.node import InternalNode, LeafNode


class TestLeafNode:
    def test_empty_lookup(self):
        leaf = LeafNode()
        assert leaf.lookup(b"x") is None

    def test_apply_put_inserts_sorted(self):
        leaf = LeafNode()
        leaf.apply_message(Message(op=Op.PUT, key=b"c", value=3, seq=1))
        leaf.apply_message(Message(op=Op.PUT, key=b"a", value=1, seq=2))
        leaf.apply_message(Message(op=Op.PUT, key=b"b", value=2, seq=3))
        assert leaf.pivots == [b"a", b"b", b"c"]

    def test_newer_seq_wins(self):
        leaf = LeafNode()
        leaf.apply_message(Message(op=Op.PUT, key=b"k", value="v1", seq=1))
        leaf.apply_message(Message(op=Op.PUT, key=b"k", value="v2", seq=2))
        result = leaf.lookup(b"k")
        assert result == ("v2", 2)

    def test_older_seq_loses(self):
        """A 'older' message arriving after the leaf already has a newer
        version must NOT overwrite it."""
        leaf = LeafNode()
        leaf.apply_message(Message(op=Op.PUT, key=b"k", value="newest", seq=10))
        leaf.apply_message(Message(op=Op.PUT, key=b"k", value="older",  seq=5))
        result = leaf.lookup(b"k")
        assert result == ("newest", 10)

    def test_delete_removes(self):
        leaf = LeafNode()
        leaf.apply_message(Message(op=Op.PUT, key=b"k", value="v", seq=1))
        leaf.apply_message(Message(op=Op.DEL, key=b"k", seq=2))
        assert leaf.lookup(b"k") is None

    def test_older_delete_ignored(self):
        leaf = LeafNode()
        leaf.apply_message(Message(op=Op.PUT, key=b"k", value="v", seq=2))
        leaf.apply_message(Message(op=Op.DEL, key=b"k", seq=1))
        # Delete with older seq must NOT remove the newer put.
        assert leaf.lookup(b"k") == ("v", 2)


class TestInternalNode:
    def test_child_index(self):
        n = InternalNode(pivots=[b"c", b"f"], children=[LeafNode(), LeafNode(), LeafNode()])
        assert n.child_index(b"a") == 0
        assert n.child_index(b"c") == 1  # bisect_right
        assert n.child_index(b"d") == 1
        assert n.child_index(b"f") == 2
        assert n.child_index(b"z") == 2

    def test_buffer_partition_groups_correctly(self):
        n = InternalNode(
            pivots=[b"c", b"f"],
            children=[LeafNode(), LeafNode(), LeafNode()],
            buffer=[
                Message(op=Op.PUT, key=b"a", value=1, seq=1),
                Message(op=Op.PUT, key=b"b", value=2, seq=2),
                Message(op=Op.PUT, key=b"d", value=3, seq=3),
                Message(op=Op.PUT, key=b"z", value=4, seq=4),
            ],
        )
        groups = n.buffer_partition()
        assert sorted(groups.keys()) == [0, 1, 2]
        assert len(groups[0]) == 2   # a, b
        assert len(groups[1]) == 1   # d
        assert len(groups[2]) == 1   # z
