"""B^epsilon-tree.

Configuration:
    B           total "size" of a node, in messages/pivots
    epsilon     fraction of B reserved for the buffer  (0 < ε < 1)
        - ε close to 1 → all-buffer, all-write
        - ε close to 0 → all-pivots, B+-tree-like reads
    leaf_capacity = B  (leaves don't have buffers in our model)

Operations:
    put(key, value)
    get(key)        → value or None
    delete(key)
    items()         → sorted (key, value) iterator
    iter_range(lo, hi)

The tree maintains a monotone `_seq` counter; every message stamps its seq
so newest-wins is unambiguous.
"""

from __future__ import annotations

import threading
from typing import Any

from beps.stats.amplification import WriteAmpStats
from beps.tree.message import Message, Op
from beps.tree.node import InternalNode, LeafNode, Node
from beps.tree.split import (
    is_overflow_internal,
    is_overflow_leaf,
    split_internal,
    split_leaf,
)


class BEpsilonTree:
    """Single-process B^ε-tree."""

    def __init__(
        self,
        node_size: int = 16,
        epsilon: float = 0.5,
        amp_stats: WriteAmpStats | None = None,
    ) -> None:
        if not 0.0 < epsilon < 1.0:
            raise ValueError("epsilon must be in (0, 1)")
        if node_size < 4:
            raise ValueError("node_size must be at least 4")
        self.node_size: int = node_size
        self.epsilon: float = epsilon
        self._root: Node = LeafNode()
        self._seq: int = 0
        self._lock = threading.RLock()
        self.amp_stats: WriteAmpStats = amp_stats or WriteAmpStats()

    # ---- Capacity bookkeeping --------------------------------------------

    @property
    def buffer_capacity(self) -> int:
        return max(1, int(self.node_size * self.epsilon))

    @property
    def pivot_capacity(self) -> int:
        return max(2, self.node_size - self.buffer_capacity)

    @property
    def leaf_capacity(self) -> int:
        return self.node_size

    # ---- Public API -------------------------------------------------------

    def __len__(self) -> int:
        with self._lock:
            return self._leaf_count(self._root)

    def put(self, key: bytes, value: Any) -> None:
        with self._lock:
            self._seq += 1
            self._inject(Message(op=Op.PUT, key=key, value=value, seq=self._seq))

    def delete(self, key: bytes) -> None:
        with self._lock:
            self._seq += 1
            self._inject(Message(op=Op.DEL, key=key, value=None, seq=self._seq))

    def get(self, key: bytes) -> Any:
        with self._lock:
            return self._lookup(self._root, key)

    def __contains__(self, key: bytes) -> bool:
        return self.get(key) is not None

    def items(self):
        with self._lock:
            yield from self._items(self._root)

    def iter_range(self, lo: bytes, hi: bytes):
        for k, v in self.items():
            if k < lo:
                continue
            if k >= hi:
                break
            yield k, v

    # ---- Insertion --------------------------------------------------------

    def _inject(self, msg: Message) -> None:
        """Insert a single message at the root, flush + split as needed."""
        if isinstance(self._root, LeafNode):
            self._root.apply_message(msg)
            self.amp_stats.record_leaf_apply()
            if is_overflow_leaf(self._root, self.leaf_capacity):
                self._split_leaf_root()
            return

        # Internal root → push into buffer
        assert isinstance(self._root, InternalNode)
        self._root.buffer.append(msg)
        self.amp_stats.record_buffer_insert()
        if len(self._root.buffer) >= self.buffer_capacity:
            self._flush(self._root)
        if is_overflow_internal(self._root, self.pivot_capacity):
            self._split_internal_root()

    def _flush(self, node: InternalNode) -> None:
        """Push the buffer of `node` down to its children.

        Process child indices in DESCENDING order so that any splits during
        flushing don't shift the indices we're about to use.
        """
        if not node.buffer:
            return
        groups = node.buffer_partition()
        node.buffer.clear()

        for idx in sorted(groups.keys(), reverse=True):
            msgs = groups[idx]
            child = node.children[idx]
            self.amp_stats.record_flush_messages(len(msgs))
            if isinstance(child, LeafNode):
                for m in msgs:
                    child.apply_message(m)
                    self.amp_stats.record_leaf_apply()
                if is_overflow_leaf(child, self.leaf_capacity):
                    self._split_child_leaf(node, idx)
            else:
                assert isinstance(child, InternalNode)
                child.buffer.extend(msgs)
                for _ in msgs:
                    self.amp_stats.record_buffer_insert()
                if len(child.buffer) >= self.buffer_capacity:
                    self._flush(child)
                if is_overflow_internal(child, self.pivot_capacity):
                    self._split_child_internal(node, idx)

    # ---- Splits -----------------------------------------------------------

    def _split_leaf_root(self) -> None:
        assert isinstance(self._root, LeafNode)
        left, right, sep = split_leaf(self._root)
        new_root = InternalNode(pivots=[sep], children=[left, right])
        self._root = new_root
        self.amp_stats.record_split()

    def _split_internal_root(self) -> None:
        assert isinstance(self._root, InternalNode)
        left, right, sep = split_internal(self._root)
        new_root = InternalNode(pivots=[sep], children=[left, right])
        self._root = new_root
        self.amp_stats.record_split()

    def _split_child_leaf(self, parent: InternalNode, idx: int) -> None:
        child = parent.children[idx]
        assert isinstance(child, LeafNode)
        left, right, sep = split_leaf(child)
        parent.pivots.insert(idx, sep)
        parent.children[idx] = left
        parent.children.insert(idx + 1, right)
        self.amp_stats.record_split()

    def _split_child_internal(self, parent: InternalNode, idx: int) -> None:
        child = parent.children[idx]
        assert isinstance(child, InternalNode)
        left, right, sep = split_internal(child)
        parent.pivots.insert(idx, sep)
        parent.children[idx] = left
        parent.children.insert(idx + 1, right)
        self.amp_stats.record_split()

    # ---- Lookup -----------------------------------------------------------

    def _lookup(self, node: Node, key: bytes) -> Any:
        """Walk root→leaf checking buffered messages; newest seq wins."""
        latest: Message | None = None
        cur: Node = node
        while not cur.is_leaf:
            assert isinstance(cur, InternalNode)
            for m in cur.buffer:
                if m.key == key and (latest is None or m.seq > latest.seq):
                    latest = m
            cur = cur.child_for_key(key)

        # Leaf lookup
        assert isinstance(cur, LeafNode)
        leaf_hit = cur.lookup(key)
        if latest is not None and leaf_hit is not None:
            _, leaf_seq = leaf_hit
            if latest.seq > leaf_seq:
                return None if latest.op == Op.DEL else latest.value
            # Leaf seq is newer; ignore the buffered message
            return leaf_hit[0]
        if latest is not None:
            return None if latest.op == Op.DEL else latest.value
        if leaf_hit is not None:
            return leaf_hit[0]
        return None

    # ---- Iteration --------------------------------------------------------

    def _items(self, node: Node):
        """Tree-ordered (key, value) iteration. We materialise the buffer's
        net effect by replaying messages onto a copy-on-write per-leaf view.

        For each leaf in order: collect buffered messages from ancestor
        buffers that target a key in this leaf's range; apply them to a
        scratch view; emit the merged set.
        """
        if isinstance(node, LeafNode):
            yield from zip(node.pivots, node.values, strict=False)
            return
        assert isinstance(node, InternalNode)

        # Build a map ancestor_messages keyed by child index
        for idx, child in enumerate(node.children):
            lo = node.pivots[idx - 1] if idx > 0 else None
            hi = node.pivots[idx] if idx < len(node.pivots) else None
            relevant = [
                m for m in node.buffer
                if (lo is None or m.key >= lo) and (hi is None or m.key < hi)
            ]
            yield from self._items_with_overrides(child, relevant)

    def _items_with_overrides(self, node: Node, overrides: list[Message]):
        """Same as _items but the caller passes additional messages from
        ancestor buffers that target keys in this subtree."""
        if isinstance(node, LeafNode):
            # Build a temporary leaf with overrides applied
            tmp = LeafNode(
                pivots=list(node.pivots),
                values=list(node.values),
                seqs=list(node.seqs),
            )
            # Sort overrides by seq ascending so older are applied first
            for m in sorted(overrides, key=lambda x: x.seq):
                tmp.apply_message(m)
            yield from zip(tmp.pivots, tmp.values, strict=False)
            return

        assert isinstance(node, InternalNode)
        local = node.buffer + overrides
        for idx, child in enumerate(node.children):
            lo = node.pivots[idx - 1] if idx > 0 else None
            hi = node.pivots[idx] if idx < len(node.pivots) else None
            relevant = [
                m for m in local
                if (lo is None or m.key >= lo) and (hi is None or m.key < hi)
            ]
            yield from self._items_with_overrides(child, relevant)

    # ---- Introspection ----------------------------------------------------

    def depth(self) -> int:
        with self._lock:
            d = 0
            cur = self._root
            while isinstance(cur, InternalNode):
                d += 1
                cur = cur.children[0]
            return d + 1

    def buffer_total(self) -> int:
        """Total messages in all internal-node buffers."""
        total = 0
        stack: list[Node] = [self._root]
        while stack:
            n = stack.pop()
            if isinstance(n, InternalNode):
                total += len(n.buffer)
                stack.extend(n.children)
        return total

    def _leaf_count(self, node: Node) -> int:
        if isinstance(node, LeafNode):
            return len(node.pivots)
        assert isinstance(node, InternalNode)
        return sum(self._leaf_count(c) for c in node.children)

    def node_count(self) -> int:
        count = 0
        stack: list[Node] = [self._root]
        while stack:
            n = stack.pop()
            count += 1
            if isinstance(n, InternalNode):
                stack.extend(n.children)
        return count

    def flush_all(self) -> None:
        """Force-flush every buffer all the way to leaves. Used for
        deterministic state inspection in tests."""
        with self._lock:
            self._force_flush(self._root)
            # Root might have over-split during flushing; rebalance if needed
            while isinstance(self._root, InternalNode) and is_overflow_internal(
                self._root, self.pivot_capacity
            ):
                self._split_internal_root()

    def _force_flush(self, node: Node) -> None:
        if isinstance(node, LeafNode):
            return
        assert isinstance(node, InternalNode)
        if node.buffer:
            self._flush(node)
        for c in list(node.children):
            self._force_flush(c)
