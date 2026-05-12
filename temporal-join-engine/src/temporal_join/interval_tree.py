"""
Augmented AVL BST keyed by integer timestamps.

Supports O(log n) insert/delete/predecessor/successor and O(log n + k) range queries.
Multiple values may share the same key (timestamp collisions).
"""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, List, Optional, Tuple


# ---------------------------------------------------------------------------
# Node and low-level tree operations
# ---------------------------------------------------------------------------

@dataclass
class _Node:
    key: int
    values: list = field(default_factory=list)
    left: Optional[_Node] = field(default=None, repr=False)
    right: Optional[_Node] = field(default=None, repr=False)
    height: int = 1
    size: int = 1  # number of distinct keys in subtree


def _h(n: Optional[_Node]) -> int:
    return n.height if n else 0


def _sz(n: Optional[_Node]) -> int:
    return n.size if n else 0


def _upd(n: _Node) -> None:
    n.height = 1 + max(_h(n.left), _h(n.right))
    n.size = 1 + _sz(n.left) + _sz(n.right)


def _bf(n: _Node) -> int:
    return _h(n.left) - _h(n.right)


def _rot_r(y: _Node) -> _Node:
    x = y.left
    y.left = x.right
    x.right = y
    _upd(y)
    _upd(x)
    return x


def _rot_l(x: _Node) -> _Node:
    y = x.right
    x.right = y.left
    y.left = x
    _upd(x)
    _upd(y)
    return y


def _balance(n: _Node) -> _Node:
    _upd(n)
    if _bf(n) > 1:
        if _bf(n.left) < 0:
            n.left = _rot_l(n.left)
        return _rot_r(n)
    if _bf(n) < -1:
        if _bf(n.right) > 0:
            n.right = _rot_r(n.right)
        return _rot_l(n)
    return n


def _insert(node: Optional[_Node], key: int, value: Any) -> _Node:
    if node is None:
        return _Node(key=key, values=[value])
    if key < node.key:
        node.left = _insert(node.left, key, value)
    elif key > node.key:
        node.right = _insert(node.right, key, value)
    else:
        node.values.append(value)
        return node
    return _balance(node)


def _min_node(node: _Node) -> _Node:
    while node.left:
        node = node.left
    return node


def _del_min(node: _Node) -> Optional[_Node]:
    if node.left is None:
        return node.right
    node.left = _del_min(node.left)
    return _balance(node)


def _delete(node: Optional[_Node], key: int, value: Any) -> Optional[_Node]:
    if node is None:
        return None
    if key < node.key:
        node.left = _delete(node.left, key, value)
    elif key > node.key:
        node.right = _delete(node.right, key, value)
    else:
        try:
            node.values.remove(value)
        except ValueError:
            return node
        if node.values:
            return node  # key still has other values — keep the node
        if node.right is None:
            return node.left
        if node.left is None:
            return node.right
        succ = _min_node(node.right)
        replacement = _Node(
            key=succ.key,
            values=list(succ.values),
            left=node.left,
            right=_del_min(node.right),
        )
        return _balance(replacement)
    return _balance(node)


def _pred(node: Optional[_Node], key: int) -> Optional[_Node]:
    best: Optional[_Node] = None
    while node:
        if node.key <= key:
            best = node
            node = node.right
        else:
            node = node.left
    return best


def _succ(node: Optional[_Node], key: int) -> Optional[_Node]:
    best: Optional[_Node] = None
    while node:
        if node.key >= key:
            best = node
            node = node.left
        else:
            node = node.right
    return best


def _range(node: Optional[_Node], lo: int, hi: int, out: list) -> None:
    if node is None:
        return
    if node.key > lo:
        _range(node.left, lo, hi, out)
    if lo <= node.key <= hi:
        out.append((node.key, node.values))
    if node.key < hi:
        _range(node.right, lo, hi, out)


# ---------------------------------------------------------------------------
# Public class
# ---------------------------------------------------------------------------

class IntervalTree:
    """
    Augmented AVL tree keyed by integer timestamps.

    Each inserted value is associated with an integer key (event_time).  Multiple
    values may share the same key.  The tree answers:

    - predecessor(k)   – largest key ≤ k
    - successor(k)     – smallest key ≥ k
    - range_query(lo, hi) – all (key, values) pairs with lo ≤ key ≤ hi

    The name "IntervalTree" reflects its role in the temporal join engine:
    each right-side event at time T_r is valid AS OF any left event at time T_l
    where T_l ∈ [T_r, T_r + lookback], an interval membership test answered by
    predecessor + range queries on this structure.
    """

    def __init__(self) -> None:
        self._root: Optional[_Node] = None

    def __len__(self) -> int:
        return _sz(self._root)

    def __bool__(self) -> bool:
        return self._root is not None

    def __contains__(self, key: int) -> bool:
        n = self._root
        while n:
            if key == n.key:
                return True
            n = n.left if key < n.key else n.right
        return False

    def insert(self, key: int, value: Any) -> None:
        """Insert *value* under *key*.  O(log n)."""
        self._root = _insert(self._root, key, value)

    def delete(self, key: int, value: Any) -> None:
        """Remove the first occurrence of *value* under *key*.  O(log n)."""
        self._root = _delete(self._root, key, value)

    def predecessor(self, key: int) -> Optional[Tuple[int, list]]:
        """Return (k, values) for the largest stored key ≤ *key*, or None."""
        n = _pred(self._root, key)
        return (n.key, n.values) if n else None

    def successor(self, key: int) -> Optional[Tuple[int, list]]:
        """Return (k, values) for the smallest stored key ≥ *key*, or None."""
        n = _succ(self._root, key)
        return (n.key, n.values) if n else None

    def range_query(self, lo: int, hi: int) -> List[Tuple[int, list]]:
        """Return all (key, values) pairs with lo ≤ key ≤ hi, sorted by key."""
        out: list = []
        _range(self._root, lo, hi, out)
        return out

    def min_key(self) -> Optional[int]:
        if self._root is None:
            return None
        return _min_node(self._root).key

    def max_key(self) -> Optional[int]:
        if self._root is None:
            return None
        n = self._root
        while n.right:
            n = n.right
        return n.key
