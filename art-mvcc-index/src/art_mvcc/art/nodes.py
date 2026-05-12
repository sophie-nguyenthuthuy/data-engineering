"""ART node types (Leis et al., ICDE 2013).

Four node types, with adaptive widening based on fanout:

  Node4    — up to 4 children: keys[] + child[] linear scan
  Node16   — up to 16 children: keys[] sorted + child[] binary search
  Node48   — up to 48 children: byte→slot index[256] + child[48]
  Node256  — up to 256 children: child[256] direct addressing

When a node fills, it widens to the next type; when it shrinks below the
lower bound, it narrows.

Each node also carries:
  - `prefix`: bytes shared by all descendants below this node (path compression)
  - `value`: optional terminator (set when a key exactly matches `prefix`)
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Any, ClassVar


# Sentinel for "no value stored here".
class _Missing:
    __slots__ = ()

    def __repr__(self) -> str:
        return "MISSING"

    def __bool__(self) -> bool:
        return False


MISSING: Any = _Missing()


class Node(ABC):
    """Base class. Each node has a path-compressed `prefix` and optional `value`."""

    __slots__ = ("prefix", "value")

    CAP: ClassVar[int] = 0
    MIN: ClassVar[int] = 0

    def __init__(self, prefix: bytes = b"", value: Any = MISSING) -> None:
        self.prefix: bytes = prefix
        self.value: Any = value

    @abstractmethod
    def find_child(self, byte: int) -> Node | None: ...

    @abstractmethod
    def add_child(self, byte: int, child: Node) -> Node:
        """Insert/replace `byte → child`. Returns the (possibly widened) node."""

    @abstractmethod
    def remove_child(self, byte: int) -> Node | None:
        """Remove `byte`. Returns the (possibly narrowed) node or None if empty."""

    @abstractmethod
    def children(self) -> list[tuple[int, Node]]:
        """All (byte, child) pairs in key-ascending order."""

    @property
    @abstractmethod
    def n_children(self) -> int: ...

    @property
    def is_terminator(self) -> bool:
        return self.value is not MISSING


# ---------------------------------------------------------------------------
# Node4 — linear scan up to 4 keys
# ---------------------------------------------------------------------------


class Node4(Node):
    __slots__ = ("_children", "keys")
    CAP = 4
    MIN = 0   # collapses when reaches 1

    def __init__(self, prefix: bytes = b"", value: Any = MISSING) -> None:
        super().__init__(prefix, value)
        self.keys: list[int] = []
        self._children: list[Node] = []

    @property
    def n_children(self) -> int:
        return len(self.keys)

    def find_child(self, byte: int) -> Node | None:
        for i, k in enumerate(self.keys):
            if k == byte:
                return self._children[i]
        return None

    def add_child(self, byte: int, child: Node) -> Node:
        for i, k in enumerate(self.keys):
            if k == byte:
                self._children[i] = child
                return self
        if len(self.keys) < self.CAP:
            # insertion-sorted
            i = 0
            while i < len(self.keys) and self.keys[i] < byte:
                i += 1
            self.keys.insert(i, byte)
            self._children.insert(i, child)
            return self
        # grow → Node16
        n = Node16(prefix=self.prefix, value=self.value)
        for k, c in zip(self.keys, self._children, strict=False):
            n.add_child(k, c)
        return n.add_child(byte, child)

    def remove_child(self, byte: int) -> Node | None:
        for i, k in enumerate(self.keys):
            if k == byte:
                self.keys.pop(i)
                self._children.pop(i)
                break
        else:
            return self
        if len(self.keys) == 0 and not self.is_terminator:
            return None
        return self

    def children(self) -> list[tuple[int, Node]]:
        return list(zip(self.keys, self._children, strict=False))


# ---------------------------------------------------------------------------
# Node16 — sorted keys, binary-searchable (up to 16)
# ---------------------------------------------------------------------------


class Node16(Node):
    __slots__ = ("_children", "keys")
    CAP = 16
    MIN = 4

    def __init__(self, prefix: bytes = b"", value: Any = MISSING) -> None:
        super().__init__(prefix, value)
        self.keys: list[int] = []
        self._children: list[Node] = []

    @property
    def n_children(self) -> int:
        return len(self.keys)

    def find_child(self, byte: int) -> Node | None:
        # Binary search would beat linear for 16 items; in pure Python the
        # overhead of bisect calls cancels the advantage. We keep it simple.
        for i, k in enumerate(self.keys):
            if k == byte:
                return self._children[i]
            if k > byte:
                return None
        return None

    def add_child(self, byte: int, child: Node) -> Node:
        for i, k in enumerate(self.keys):
            if k == byte:
                self._children[i] = child
                return self
        if len(self.keys) < self.CAP:
            i = 0
            while i < len(self.keys) and self.keys[i] < byte:
                i += 1
            self.keys.insert(i, byte)
            self._children.insert(i, child)
            return self
        # grow → Node48
        n = Node48(prefix=self.prefix, value=self.value)
        for k, c in zip(self.keys, self._children, strict=False):
            n.add_child(k, c)
        return n.add_child(byte, child)

    def remove_child(self, byte: int) -> Node | None:
        for i, k in enumerate(self.keys):
            if k == byte:
                self.keys.pop(i)
                self._children.pop(i)
                break
        else:
            return self
        if len(self.keys) < self.MIN:
            # shrink → Node4
            n4 = Node4(prefix=self.prefix, value=self.value)
            for k, c in zip(self.keys, self._children, strict=False):
                n4.add_child(k, c)
            return n4
        return self

    def children(self) -> list[tuple[int, Node]]:
        return list(zip(self.keys, self._children, strict=False))


# ---------------------------------------------------------------------------
# Node48 — index[256] -> slot[1..48]
# ---------------------------------------------------------------------------


class Node48(Node):
    __slots__ = ("index", "n", "slots")
    CAP = 48
    MIN = 13

    def __init__(self, prefix: bytes = b"", value: Any = MISSING) -> None:
        super().__init__(prefix, value)
        self.index: list[int] = [0] * 256          # 0 = absent; else 1..n
        self.slots: list[Node | None] = [None] * self.CAP
        self.n = 0

    @property
    def n_children(self) -> int:
        return self.n

    def find_child(self, byte: int) -> Node | None:
        slot = self.index[byte]
        if slot == 0:
            return None
        return self.slots[slot - 1]

    def add_child(self, byte: int, child: Node) -> Node:
        slot = self.index[byte]
        if slot != 0:
            self.slots[slot - 1] = child
            return self
        if self.n < self.CAP:
            # Find the lowest free slot
            for s in range(self.CAP):
                if self.slots[s] is None:
                    self.slots[s] = child
                    self.index[byte] = s + 1
                    self.n += 1
                    return self
            raise AssertionError("unreachable: n_children < CAP but no free slot")
        # grow → Node256
        n = Node256(prefix=self.prefix, value=self.value)
        for b in range(256):
            slot = self.index[b]
            if slot:
                child_b = self.slots[slot - 1]
                if child_b is not None:
                    n.add_child(b, child_b)
        return n.add_child(byte, child)

    def remove_child(self, byte: int) -> Node | None:
        slot = self.index[byte]
        if slot == 0:
            return self
        self.slots[slot - 1] = None
        self.index[byte] = 0
        self.n -= 1
        if self.n < self.MIN:
            # shrink → Node16
            n16 = Node16(prefix=self.prefix, value=self.value)
            for b in range(256):
                s = self.index[b]
                if s:
                    child_b = self.slots[s - 1]
                    if child_b is not None:
                        n16.add_child(b, child_b)
            return n16
        return self

    def children(self) -> list[tuple[int, Node]]:
        out: list[tuple[int, Node]] = []
        for b in range(256):
            s = self.index[b]
            if s:
                child = self.slots[s - 1]
                if child is not None:
                    out.append((b, child))
        return out


# ---------------------------------------------------------------------------
# Node256 — direct 256-slot array
# ---------------------------------------------------------------------------


class Node256(Node):
    __slots__ = ("n", "slots")
    CAP = 256
    MIN = 49

    def __init__(self, prefix: bytes = b"", value: Any = MISSING) -> None:
        super().__init__(prefix, value)
        self.slots: list[Node | None] = [None] * 256
        self.n = 0

    @property
    def n_children(self) -> int:
        return self.n

    def find_child(self, byte: int) -> Node | None:
        return self.slots[byte]

    def add_child(self, byte: int, child: Node) -> Node:
        if self.slots[byte] is None:
            self.n += 1
        self.slots[byte] = child
        return self

    def remove_child(self, byte: int) -> Node | None:
        if self.slots[byte] is None:
            return self
        self.slots[byte] = None
        self.n -= 1
        if self.n < self.MIN:
            # shrink → Node48
            n48 = Node48(prefix=self.prefix, value=self.value)
            for b in range(256):
                child = self.slots[b]
                if child is not None:
                    n48.add_child(b, child)
            return n48
        return self

    def children(self) -> list[tuple[int, Node]]:
        return [(b, c) for b, c in enumerate(self.slots) if c is not None]


# ---------------------------------------------------------------------------
# Utility: detect node type
# ---------------------------------------------------------------------------


def node_kind(node: Node) -> str:
    return type(node).__name__


__all__ = ["MISSING", "Node", "Node4", "Node16", "Node48", "Node256", "node_kind"]
