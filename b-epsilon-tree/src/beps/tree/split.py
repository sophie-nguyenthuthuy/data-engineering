"""Leaf and internal-node splits.

Splits happen when a node grows past its pivot capacity. For internal
nodes we also partition the buffer messages between the two halves based
on the new separator key.
"""

from __future__ import annotations

from beps.tree.node import InternalNode, LeafNode


def split_leaf(leaf: LeafNode) -> tuple[LeafNode, LeafNode, bytes]:
    """Split `leaf` into two; return (left, right, separator_key)."""
    mid = len(leaf.pivots) // 2
    left = LeafNode(
        pivots=leaf.pivots[:mid],
        values=leaf.values[:mid],
        seqs=leaf.seqs[:mid],
    )
    right = LeafNode(
        pivots=leaf.pivots[mid:],
        values=leaf.values[mid:],
        seqs=leaf.seqs[mid:],
    )
    return left, right, right.pivots[0]


def split_internal(node: InternalNode) -> tuple[InternalNode, InternalNode, bytes]:
    """Split internal node; partition buffer by separator.

    If node has N children (N-1 pivots), the split point is mid = N // 2:
        left  children: [0 .. mid)        pivots: [0 .. mid-1)
        right children: [mid .. N)        pivots: [mid .. N-1)
        separator = pivots[mid - 1]   ←   bubbled up to parent
    """
    n_children = len(node.children)
    if n_children < 2:
        raise ValueError("cannot split internal node with < 2 children")
    mid = n_children // 2
    sep = node.pivots[mid - 1]

    left = InternalNode(
        pivots=node.pivots[: mid - 1],
        children=node.children[:mid],
        buffer=[m for m in node.buffer if m.key < sep],
    )
    right = InternalNode(
        pivots=node.pivots[mid:],
        children=node.children[mid:],
        buffer=[m for m in node.buffer if m.key >= sep],
    )
    return left, right, sep


def is_overflow_leaf(leaf: LeafNode, leaf_capacity: int) -> bool:
    return len(leaf.pivots) > leaf_capacity


def is_overflow_internal(node: InternalNode, pivot_capacity: int) -> bool:
    return len(node.children) > pivot_capacity
