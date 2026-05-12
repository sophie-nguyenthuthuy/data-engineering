"""Tree nodes.

LeafNode:
    pivots[i] is the i-th sorted key
    values[i] is its current value
    seqs[i] is the seq that produced values[i] (for newest-wins)

InternalNode:
    pivots[i] is the i-th separator (i < len(children) - 1)
    children[i] is the i-th child node
    buffer is the list of pending Messages destined for descendants
"""

from __future__ import annotations

from bisect import bisect_left, bisect_right
from dataclasses import dataclass, field

from beps.tree.message import Message, Op


@dataclass
class Node:
    """Base class. Concrete subclasses below."""

    @property
    def is_leaf(self) -> bool:  # pragma: no cover
        raise NotImplementedError

    @property
    def size(self) -> int:  # pragma: no cover
        raise NotImplementedError


@dataclass
class LeafNode(Node):
    pivots: list[bytes] = field(default_factory=list)
    values: list[object] = field(default_factory=list)
    seqs: list[int] = field(default_factory=list)

    @property
    def is_leaf(self) -> bool:
        return True

    @property
    def size(self) -> int:
        return len(self.pivots)

    def lookup(self, key: bytes) -> tuple[object, int] | None:
        """Return (value, seq) if present, else None."""
        i = bisect_left(self.pivots, key)
        if i < len(self.pivots) and self.pivots[i] == key:
            return self.values[i], self.seqs[i]
        return None

    def apply_message(self, msg: Message) -> None:
        """Apply a message in-place. Newest-wins on seq."""
        i = bisect_left(self.pivots, msg.key)
        present = i < len(self.pivots) and self.pivots[i] == msg.key
        if msg.op == Op.PUT:
            if present:
                if msg.seq > self.seqs[i]:
                    self.values[i] = msg.value
                    self.seqs[i] = msg.seq
            else:
                self.pivots.insert(i, msg.key)
                self.values.insert(i, msg.value)
                self.seqs.insert(i, msg.seq)
        elif msg.op == Op.DEL and present and msg.seq > self.seqs[i]:
            self.pivots.pop(i)
            self.values.pop(i)
            self.seqs.pop(i)


@dataclass
class InternalNode(Node):
    pivots: list[bytes] = field(default_factory=list)
    children: list[Node] = field(default_factory=list)
    buffer: list[Message] = field(default_factory=list)

    @property
    def is_leaf(self) -> bool:
        return False

    @property
    def size(self) -> int:
        return len(self.children)

    def child_index(self, key: bytes) -> int:
        """Index of the child responsible for `key`."""
        return bisect_right(self.pivots, key)

    def child_for_key(self, key: bytes) -> Node:
        return self.children[self.child_index(key)]

    def buffer_messages_for_key(self, key: bytes) -> list[Message]:
        return [m for m in self.buffer if m.key == key]

    def buffer_partition(self) -> dict[int, list[Message]]:
        """Group buffer messages by child index."""
        groups: dict[int, list[Message]] = {}
        for m in self.buffer:
            idx = self.child_index(m.key)
            groups.setdefault(idx, []).append(m)
        return groups
