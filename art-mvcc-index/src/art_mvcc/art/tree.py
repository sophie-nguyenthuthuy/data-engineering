"""Adaptive Radix Tree with path compression.

Public API:
    art = ART()
    art.put(b"key", value)
    art.get(b"key") -> value | None
    art.delete(b"key") -> bool
    for k, v in art.iter_prefix(b"prefix"): ...
    for k, v in art.iter_range(b"a", b"z"): ...

Path compression: each node carries a `prefix` shared by all descendants.
This shrinks the tree depth from O(key_len) to O(log_radix(N)) for typical
data.
"""

from __future__ import annotations

import threading
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from collections.abc import Iterator

from art_mvcc.art.nodes import MISSING, Node, Node4


class ART:
    """Single-threaded ART. For concurrent use, wrap with MVCCArt (which
    serialises writes and uses snapshot reads)."""

    __slots__ = ("_lock", "_root", "_size")

    def __init__(self) -> None:
        self._root: Node | None = None
        self._size: int = 0
        self._lock = threading.RLock()

    # ---- Public API -------------------------------------------------------

    def __len__(self) -> int:
        return self._size

    def put(self, key: bytes, value: Any) -> None:
        if value is MISSING:
            raise ValueError("cannot store MISSING sentinel")
        with self._lock:
            if self._root is None:
                self._root = Node4(prefix=key, value=value)
                self._size += 1
                return
            new_root, inserted = self._insert(self._root, key, value)
            self._root = new_root
            if inserted:
                self._size += 1

    def get(self, key: bytes) -> Any:
        with self._lock:
            if self._root is None:
                return None
            return self._lookup(self._root, key)

    def delete(self, key: bytes) -> bool:
        with self._lock:
            if self._root is None:
                return False
            new_root, ok = self._delete(self._root, key)
            self._root = new_root
            if ok:
                self._size -= 1
            return ok

    def __contains__(self, key: bytes) -> bool:
        return self.get(key) is not None

    def items(self) -> Iterator[tuple[bytes, Any]]:
        """All (key, value) pairs in key-sorted order."""
        with self._lock:
            if self._root is None:
                return
            yield from self._walk(self._root, b"")

    def keys(self) -> Iterator[bytes]:
        for k, _ in self.items():
            yield k

    def values(self) -> Iterator[Any]:
        for _, v in self.items():
            yield v

    def iter_prefix(self, prefix: bytes) -> Iterator[tuple[bytes, Any]]:
        with self._lock:
            if self._root is None:
                return
            sub = self._descend_to_prefix(self._root, prefix, b"")
            if sub is None:
                return
            node, key_so_far = sub
            yield from self._walk(node, key_so_far)

    def iter_range(self, lo: bytes, hi: bytes) -> Iterator[tuple[bytes, Any]]:
        """Inclusive `lo`, exclusive `hi`."""
        for k, v in self.items():
            if k < lo:
                continue
            if k >= hi:
                break
            yield k, v

    # ---- Internals --------------------------------------------------------

    @staticmethod
    def _common_prefix_len(a: bytes, b: bytes) -> int:
        n = min(len(a), len(b))
        for i in range(n):
            if a[i] != b[i]:
                return i
        return n

    def _insert(self, node: Node, key: bytes, value: Any) -> tuple[Node, bool]:
        """Returns (new_subtree, was_new_key)."""
        plen = self._common_prefix_len(node.prefix, key)

        # Case 1: key exactly equals prefix
        if plen == len(node.prefix) == len(key):
            was_new = not node.is_terminator
            node.value = value
            return node, was_new

        # Case 2: key extends past prefix → recurse into matching child
        if plen == len(node.prefix):
            remaining = key[plen:]
            byte = remaining[0]
            child = node.find_child(byte)
            if child is None:
                new_leaf = Node4(prefix=remaining[1:], value=value)
                new_self = node.add_child(byte, new_leaf)
                return new_self, True
            new_child, was_new = self._insert(child, remaining[1:], value)
            new_self = node.add_child(byte, new_child)
            return new_self, was_new

        # Case 3: prefix divergence → split this node
        # Create a new parent at the shared prefix; the old node becomes one
        # child (with the rest of its prefix), and the new value lands either
        # as the parent's terminator (if key is entirely consumed) or as a
        # sibling child.
        new_parent = Node4(prefix=node.prefix[:plen])
        old_first_byte = node.prefix[plen]
        node.prefix = node.prefix[plen + 1:]
        new_parent_node = new_parent.add_child(old_first_byte, node)

        if plen == len(key):
            new_parent_node.value = value
        else:
            new_byte = key[plen]
            new_leaf = Node4(prefix=key[plen + 1:], value=value)
            new_parent_node = new_parent_node.add_child(new_byte, new_leaf)

        return new_parent_node, True

    def _lookup(self, node: Node, key: bytes) -> Any:
        if not key.startswith(node.prefix):
            return None
        rest = key[len(node.prefix):]
        if not rest:
            return None if node.value is MISSING else node.value
        child = node.find_child(rest[0])
        if child is None:
            return None
        return self._lookup(child, rest[1:])

    def _delete(self, node: Node, key: bytes) -> tuple[Node | None, bool]:
        if not key.startswith(node.prefix):
            return node, False
        rest = key[len(node.prefix):]
        if not rest:
            if node.value is MISSING:
                return node, False
            node.value = MISSING
            # If this node has zero children AND no value, collapse it.
            if node.n_children == 0:
                return None, True
            return node, True

        child = node.find_child(rest[0])
        if child is None:
            return node, False
        new_child, ok = self._delete(child, rest[1:])
        if not ok:
            return node, False
        if new_child is None:
            new_self = node.remove_child(rest[0])
            return new_self, True
        # Path compression: if the child node has 1 child AND no value AND its
        # parent has 1 entry, we could merge — kept out for clarity; full ART
        # papers describe this collapse but it's an optimisation, not
        # correctness.
        new_self = node.add_child(rest[0], new_child)
        return new_self, True

    def _walk(self, node: Node, key_so_far: bytes) -> Iterator[tuple[bytes, Any]]:
        my_key = key_so_far + node.prefix
        if node.is_terminator:
            yield my_key, node.value
        for byte, child in node.children():
            yield from self._walk(child, my_key + bytes([byte]))

    def _descend_to_prefix(
        self, node: Node, target: bytes, key_so_far: bytes
    ) -> tuple[Node, bytes] | None:
        """Find the deepest node whose accumulated key is a prefix of `target`.

        Returns (node, accumulated_key) — every descendant is in the prefix's
        range. If the prefix path diverges, returns None.
        """
        my_key = key_so_far + node.prefix
        if target.startswith(my_key):
            # All my keys live under `target` if my_key extends past `target`,
            # or we may need to descend further.
            if len(my_key) >= len(target):
                return node, key_so_far
            next_byte = target[len(my_key)]
            child = node.find_child(next_byte)
            if child is None:
                return None
            return self._descend_to_prefix(child, target, my_key + bytes([next_byte]))
        # Otherwise check whether `target` is itself a prefix of my_key
        if my_key.startswith(target):
            return node, key_so_far
        return None

    # ---- Diagnostics ------------------------------------------------------

    def depth(self) -> int:
        """Maximum tree depth (in nodes); useful for benchmarks."""
        if self._root is None:
            return 0
        return self._max_depth(self._root)

    def _max_depth(self, node: Node) -> int:
        if node.n_children == 0:
            return 1
        return 1 + max(self._max_depth(c) for _, c in node.children())

    def node_count_by_kind(self) -> dict[str, int]:
        """{Node4: N, Node16: M, ...} for memory-shape introspection."""
        from collections import Counter
        c: Counter[str] = Counter()
        if self._root is None:
            return dict(c)
        stack: list[Node] = [self._root]
        while stack:
            n = stack.pop()
            c[type(n).__name__] += 1
            for _, child in n.children():
                stack.append(child)
        return dict(c)


__all__ = ["ART"]
