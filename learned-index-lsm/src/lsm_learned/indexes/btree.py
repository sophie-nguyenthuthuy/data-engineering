"""
B-tree index backed by a sorted list (SortedList from sortedcontainers).

Provides O(log n) point lookups and range scans as the classic baseline
against which the RMI is benchmarked.
"""

from __future__ import annotations

from typing import Optional

from sortedcontainers import SortedList


class BTreeIndex:
    """
    Sorted-list index supporting point lookup and range queries.

    The underlying SortedList maintains O(log n) amortized insertion and
    O(log n) bisect-based search — comparable to a balanced BST / B-tree.
    """

    def __init__(self) -> None:
        self._keys: SortedList[int] = SortedList()

    # ------------------------------------------------------------------
    # Mutators
    # ------------------------------------------------------------------

    def add(self, key: int) -> None:
        self._keys.add(key)

    def build(self, keys: list[int] | SortedList[int]) -> None:
        """Bulk-load sorted or unsorted keys."""
        self._keys = SortedList(keys)

    def remove(self, key: int) -> None:
        self._keys.remove(key)

    # ------------------------------------------------------------------
    # Queries
    # ------------------------------------------------------------------

    def contains(self, key: int) -> bool:
        idx = self._keys.bisect_left(key)
        return idx < len(self._keys) and self._keys[idx] == key

    def lookup(self, key: int) -> Optional[int]:
        """Return the sorted-array index of *key*, or None if absent."""
        idx = self._keys.bisect_left(key)
        if idx < len(self._keys) and self._keys[idx] == key:
            return idx
        return None

    def range_keys(self, lo: int, hi: int) -> list[int]:
        """Return all keys k such that lo <= k <= hi."""
        i = self._keys.bisect_left(lo)
        j = self._keys.bisect_right(hi)
        return list(self._keys[i:j])

    # ------------------------------------------------------------------
    # Properties
    # ------------------------------------------------------------------

    def __len__(self) -> int:
        return len(self._keys)

    def memory_estimate_bytes(self) -> int:
        # SortedList uses ~8 bytes per stored Python int + list overhead
        return len(self._keys) * 28  # sys.getsizeof(int) ≈ 28
