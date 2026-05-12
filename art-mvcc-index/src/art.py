"""Adaptive Radix Tree (Leis et al. 2013).

Node types adapt to fanout:
  Node4    — up to 4 keys (linear scan)
  Node16   — up to 16 keys (binary search / SIMD)
  Node48   — up to 48 keys (index array of 256 → 48 child slots)
  Node256  — up to 256 keys (direct lookup)

This is a *clean* pedagogical implementation: correctness first, layout next.
Keys are bytes; the tree branches on one byte per level.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Optional

# Sentinel for "no value stored here".
MISSING = object()


class Node:
    """Base class. Each subclass tracks (key_byte → child) differently."""
    __slots__ = ("prefix", "value")

    def __init__(self, prefix: bytes = b"", value=MISSING):
        self.prefix = prefix         # common prefix below this node
        self.value = value           # if set, terminator for "prefix"

    def find_child(self, byte: int) -> Optional["Node"]:
        raise NotImplementedError

    def add_child(self, byte: int, child: "Node") -> "Node":
        """Returns the (possibly grown) node holding the new child."""
        raise NotImplementedError

    def children(self):
        raise NotImplementedError


class Node4(Node):
    __slots__ = ("keys", "child_ptrs")

    CAP = 4

    def __init__(self, prefix=b"", value=MISSING):
        super().__init__(prefix, value)
        self.keys: list[int] = []
        self.child_ptrs: list[Node] = []

    def find_child(self, byte: int) -> Optional[Node]:
        for i, k in enumerate(self.keys):
            if k == byte:
                return self.child_ptrs[i]
        return None

    def add_child(self, byte: int, child: Node) -> Node:
        # If replacement
        for i, k in enumerate(self.keys):
            if k == byte:
                self.child_ptrs[i] = child
                return self
        if len(self.keys) < self.CAP:
            # Keep keys sorted
            i = 0
            while i < len(self.keys) and self.keys[i] < byte:
                i += 1
            self.keys.insert(i, byte)
            self.child_ptrs.insert(i, child)
            return self
        # Grow to Node16
        n = Node16(prefix=self.prefix, value=self.value)
        for k, c in zip(self.keys, self.child_ptrs):
            n.add_child(k, c)
        return n.add_child(byte, child)

    def children(self):
        return list(zip(self.keys, self.child_ptrs))


class Node16(Node):
    __slots__ = ("keys", "child_ptrs")
    CAP = 16

    def __init__(self, prefix=b"", value=MISSING):
        super().__init__(prefix, value)
        self.keys: list[int] = []
        self.child_ptrs: list[Node] = []

    def find_child(self, byte: int) -> Optional[Node]:
        # Binary search would be faster; linear is fine for correctness.
        for i, k in enumerate(self.keys):
            if k == byte:
                return self.child_ptrs[i]
        return None

    def add_child(self, byte: int, child: Node) -> Node:
        for i, k in enumerate(self.keys):
            if k == byte:
                self.child_ptrs[i] = child
                return self
        if len(self.keys) < self.CAP:
            i = 0
            while i < len(self.keys) and self.keys[i] < byte:
                i += 1
            self.keys.insert(i, byte)
            self.child_ptrs.insert(i, child)
            return self
        n = Node48(prefix=self.prefix, value=self.value)
        for k, c in zip(self.keys, self.child_ptrs):
            n.add_child(k, c)
        return n.add_child(byte, child)

    def children(self):
        return list(zip(self.keys, self.child_ptrs))


class Node48(Node):
    __slots__ = ("index", "child_slots", "n")
    CAP = 48

    def __init__(self, prefix=b"", value=MISSING):
        super().__init__(prefix, value)
        # index[byte] = slot index (1..n), 0 = absent
        self.index: list[int] = [0] * 256
        self.child_slots: list[Optional[Node]] = [None] * self.CAP
        self.n = 0

    def find_child(self, byte: int) -> Optional[Node]:
        slot = self.index[byte]
        if slot == 0:
            return None
        return self.child_slots[slot - 1]

    def add_child(self, byte: int, child: Node) -> Node:
        slot = self.index[byte]
        if slot != 0:
            self.child_slots[slot - 1] = child
            return self
        if self.n < self.CAP:
            self.child_slots[self.n] = child
            self.index[byte] = self.n + 1
            self.n += 1
            return self
        # Grow to Node256
        n = Node256(prefix=self.prefix, value=self.value)
        for b in range(256):
            if self.index[b]:
                n.add_child(b, self.child_slots[self.index[b] - 1])
        return n.add_child(byte, child)

    def children(self):
        out = []
        for b in range(256):
            if self.index[b]:
                out.append((b, self.child_slots[self.index[b] - 1]))
        return out


class Node256(Node):
    __slots__ = ("child_slots",)
    CAP = 256

    def __init__(self, prefix=b"", value=MISSING):
        super().__init__(prefix, value)
        self.child_slots: list[Optional[Node]] = [None] * 256

    def find_child(self, byte: int) -> Optional[Node]:
        return self.child_slots[byte]

    def add_child(self, byte: int, child: Node) -> Node:
        self.child_slots[byte] = child
        return self

    def children(self):
        return [(b, c) for b, c in enumerate(self.child_slots) if c is not None]


# ---------------------------------------------------------------------------
# Tree API
# ---------------------------------------------------------------------------

class ART:
    def __init__(self):
        self.root: Optional[Node] = None

    def put(self, key: bytes, value) -> None:
        if self.root is None:
            self.root = Node4(prefix=key, value=value)
            return
        self.root = self._insert(self.root, key, value)

    def get(self, key: bytes):
        if self.root is None:
            return None
        return self._lookup(self.root, key)

    def delete(self, key: bytes) -> bool:
        if self.root is None:
            return False
        ok, new_root = self._delete(self.root, key)
        self.root = new_root
        return ok

    # ---- Internals --------------------------------------------------------

    @staticmethod
    def _common_prefix_len(a: bytes, b: bytes) -> int:
        n = min(len(a), len(b))
        for i in range(n):
            if a[i] != b[i]:
                return i
        return n

    def _insert(self, node: Node, key: bytes, value) -> Node:
        plen = self._common_prefix_len(node.prefix, key)
        # Case 1: key matches prefix exactly
        if plen == len(node.prefix) == len(key):
            node.value = value
            return node
        # Case 2: key extends past prefix → recurse into child
        if plen == len(node.prefix):
            remaining = key[plen:]
            byte = remaining[0]
            child = node.find_child(byte)
            if child is None:
                new_leaf = Node4(prefix=remaining[1:], value=value)
                return node.add_child(byte, new_leaf)
            new_child = self._insert(child, remaining[1:], value)
            return node.add_child(byte, new_child)
        # Case 3: prefix divergence → must split node
        # The node's old prefix splits into [shared | remainder]; create new
        # parent at the shared part, with two children:
        #   - old node (now with shorter prefix = old_prefix[plen+1:])
        #   - new leaf (with prefix = key[plen+1:])
        new_parent = Node4(prefix=node.prefix[:plen])
        # Split: only if the new prefix exactly matches key length, attach value to parent
        # Move the old node down
        old_byte = node.prefix[plen]
        node.prefix = node.prefix[plen + 1:]
        new_parent = new_parent.add_child(old_byte, node)
        if plen == len(key):
            new_parent.value = value
        else:
            new_byte = key[plen]
            new_leaf = Node4(prefix=key[plen + 1:], value=value)
            new_parent = new_parent.add_child(new_byte, new_leaf)
        return new_parent

    def _lookup(self, node: Node, key: bytes):
        if not key.startswith(node.prefix):
            return None
        rest = key[len(node.prefix):]
        if not rest:
            return None if node.value is MISSING else node.value
        child = node.find_child(rest[0])
        if child is None:
            return None
        return self._lookup(child, rest[1:])

    def _delete(self, node: Node, key: bytes) -> tuple[bool, Optional[Node]]:
        if not key.startswith(node.prefix):
            return False, node
        rest = key[len(node.prefix):]
        if not rest:
            if node.value is MISSING:
                return False, node
            node.value = MISSING
            return True, node
        child = node.find_child(rest[0])
        if child is None:
            return False, node
        ok, new_child = self._delete(child, rest[1:])
        # Skip simplifications; child stays in place.
        return ok, node


__all__ = ["ART", "Node", "Node4", "Node16", "Node48", "Node256", "MISSING"]
