"""Write-optimized B^ε-tree.

Each internal node has:
  - pivots (fraction (1-ε) of node size)
  - a message buffer (fraction ε of node size)

Inserts/updates land in the root buffer as messages. When a buffer overflows,
the most-occupied child's worth of messages flushes down.

Reads walk top-to-bottom, checking buffers along the way.
"""
from __future__ import annotations

from bisect import bisect_left, bisect_right
from dataclasses import dataclass, field

# Logical "node size" in messages — small for clarity.
B = 16


@dataclass
class Message:
    op: str         # 'put' or 'del'
    key: int
    value: object | None = None
    seq: int = 0    # monotone insertion order for newest-wins


@dataclass
class Node:
    leaf: bool
    pivots: list = field(default_factory=list)        # sorted keys (n-1 pivots in internal, n keys in leaf)
    children: list = field(default_factory=list)      # n Node children (internal only)
    values: list = field(default_factory=list)        # n values (leaf only)
    seqs:   list = field(default_factory=list)        # n seqs (leaf only) — last-write seq per key
    buffer: list = field(default_factory=list)        # list[Message] (internal only)


class BEpsilonTree:
    def __init__(self, epsilon: float = 0.5):
        """ε in (0, 1). ε close to 1 → B+-tree; ε close to 0 → all buffer."""
        self.epsilon = epsilon
        self.B = B
        self.buf_size = max(1, int(self.B * epsilon))
        self.pivot_size = max(2, self.B - self.buf_size)
        self.root = Node(leaf=True)
        self._seq = 0

    # ---- Public API -------------------------------------------------------

    def put(self, key: int, value) -> None:
        self._seq += 1
        msg = Message("put", key, value, self._seq)
        self._apply_root_message(msg)

    def delete(self, key: int) -> None:
        self._seq += 1
        msg = Message("del", key, None, self._seq)
        self._apply_root_message(msg)

    def get(self, key: int):
        # Walk from root, checking buffer at each level (newest wins by seq).
        node = self.root
        latest: Message | None = None
        while not node.leaf:
            for m in node.buffer:
                if m.key == key:
                    if latest is None or m.seq > latest.seq:
                        latest = m
            idx = bisect_right(node.pivots, key)
            node = node.children[idx]
        # Leaf
        leaf_seq = -1
        leaf_val = None
        leaf_present = False
        if key in node.pivots:
            i = node.pivots.index(key)
            leaf_val = node.values[i]
            leaf_seq = node.seqs[i]
            leaf_present = True

        # If buffer has nothing or buffer's seq is older than leaf, use leaf
        if latest is None or latest.seq < leaf_seq:
            return leaf_val if leaf_present else None
        # Buffer's seq dominates leaf
        if latest.op == "del":
            return None
        return latest.value

    # ---- Internals --------------------------------------------------------

    def _apply_root_message(self, msg: Message) -> None:
        if self.root.leaf:
            self._apply_leaf(self.root, msg)
            if len(self.root.pivots) > self.pivot_size:
                self._split_leaf_root()
            return
        # Internal root → drop into buffer
        self.root.buffer.append(msg)
        if len(self.root.buffer) >= self.buf_size:
            self._flush(self.root)
        # Root may have grown; check split
        if len(self.root.pivots) > self.pivot_size:
            self._split_internal_root()

    def _apply_leaf(self, node: Node, msg: Message) -> None:
        if msg.op == "put":
            i = bisect_left(node.pivots, msg.key)
            if i < len(node.pivots) and node.pivots[i] == msg.key:
                # Only overwrite if msg is newer than what's there
                if msg.seq > node.seqs[i]:
                    node.values[i] = msg.value
                    node.seqs[i] = msg.seq
            else:
                node.pivots.insert(i, msg.key)
                node.values.insert(i, msg.value)
                node.seqs.insert(i, msg.seq)
        elif msg.op == "del":
            if msg.key in node.pivots:
                i = node.pivots.index(msg.key)
                # Only delete if msg is newer
                if msg.seq > node.seqs[i]:
                    node.pivots.pop(i)
                    node.values.pop(i)
                    node.seqs.pop(i)

    def _flush(self, node: Node) -> None:
        """Group buffer messages by child, push them down, recurse if child overflows.

        Iterate in DESCENDING index order so splits (which insert new children
        to the right) don't shift the indices we're about to use.
        """
        if not node.buffer:
            return
        groups: dict[int, list] = {}
        for m in node.buffer:
            idx = bisect_right(node.pivots, m.key)
            groups.setdefault(idx, []).append(m)
        node.buffer.clear()

        for idx in sorted(groups.keys(), reverse=True):
            msgs = groups[idx]
            child = node.children[idx]
            if child.leaf:
                for m in msgs:
                    self._apply_leaf(child, m)
                if len(child.pivots) > self.pivot_size:
                    self._split_leaf(node, idx)
            else:
                child.buffer.extend(msgs)
                if len(child.buffer) >= self.buf_size:
                    self._flush(child)
                if len(child.pivots) > self.pivot_size:
                    self._split_internal(node, idx)

    # ---- Splits -----------------------------------------------------------

    def _split_leaf_root(self) -> None:
        left, right, sep = self._do_split_leaf(self.root)
        new_root = Node(leaf=False)
        new_root.pivots = [sep]
        new_root.children = [left, right]
        self.root = new_root

    def _split_leaf(self, parent: Node, idx: int) -> None:
        child = parent.children[idx]
        left, right, sep = self._do_split_leaf(child)
        parent.pivots.insert(idx, sep)
        parent.children[idx] = left
        parent.children.insert(idx + 1, right)

    def _do_split_leaf(self, leaf: Node) -> tuple[Node, Node, int]:
        mid = len(leaf.pivots) // 2
        left = Node(leaf=True,
                    pivots=leaf.pivots[:mid],
                    values=leaf.values[:mid],
                    seqs=leaf.seqs[:mid])
        right = Node(leaf=True,
                     pivots=leaf.pivots[mid:],
                     values=leaf.values[mid:],
                     seqs=leaf.seqs[mid:])
        return left, right, right.pivots[0]

    def _split_internal_root(self) -> None:
        left, right, sep = self._do_split_internal(self.root)
        new_root = Node(leaf=False)
        new_root.pivots = [sep]
        new_root.children = [left, right]
        self.root = new_root

    def _split_internal(self, parent: Node, idx: int) -> None:
        child = parent.children[idx]
        left, right, sep = self._do_split_internal(child)
        parent.pivots.insert(idx, sep)
        parent.children[idx] = left
        parent.children.insert(idx + 1, right)

    def _do_split_internal(self, node: Node) -> tuple[Node, Node, int]:
        mid = len(node.pivots) // 2
        sep = node.pivots[mid]
        left = Node(leaf=False,
                    pivots=node.pivots[:mid],
                    children=node.children[:mid + 1])
        right = Node(leaf=False,
                     pivots=node.pivots[mid + 1:],
                     children=node.children[mid + 1:])
        # Flush buffer to children based on which side they belong
        for m in node.buffer:
            if m.key < sep:
                left.buffer.append(m)
            else:
                right.buffer.append(m)
        return left, right, sep

    # ---- Introspection ----------------------------------------------------

    def depth(self) -> int:
        d = 0
        node = self.root
        while not node.leaf:
            d += 1
            node = node.children[0]
        return d + 1

    def size(self) -> int:
        return self._count(self.root)

    def _count(self, node: Node) -> int:
        if node.leaf:
            return len(node.pivots)
        return sum(self._count(c) for c in node.children)


__all__ = ["BEpsilonTree", "Message", "Node"]
