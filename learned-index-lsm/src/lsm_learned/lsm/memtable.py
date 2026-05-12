"""
In-memory write buffer (MemTable) backed by a sorted dict.

Keys and values are both int64.  When ``size >= capacity`` the caller
(LSMEngine) must flush the MemTable to an SSTable.
"""

from __future__ import annotations

from sortedcontainers import SortedDict

_TOMBSTONE = object()  # sentinel for deleted keys


class MemTable:
    """
    Sorted mutable key-value store.  Supports point writes, deletes, and reads.
    """

    def __init__(self, capacity: int = 1_000_000) -> None:
        if capacity < 1:
            raise ValueError("capacity must be >= 1")
        self._capacity = capacity
        self._data: SortedDict[int, int | object] = SortedDict()

    # ------------------------------------------------------------------
    # Write path
    # ------------------------------------------------------------------

    def put(self, key: int, value: int) -> None:
        self._data[key] = value

    def delete(self, key: int) -> None:
        self._data[key] = _TOMBSTONE

    # ------------------------------------------------------------------
    # Read path
    # ------------------------------------------------------------------

    def get(self, key: int) -> int | None:
        v = self._data.get(key)
        if v is None:
            return None
        if v is _TOMBSTONE:
            return None
        return v  # type: ignore[return-value]

    def contains(self, key: int) -> bool:
        return key in self._data and self._data[key] is not _TOMBSTONE

    # ------------------------------------------------------------------
    # Flush helpers
    # ------------------------------------------------------------------

    def is_full(self) -> bool:
        return len(self._data) >= self._capacity

    def items(self) -> list[tuple[int, int | None]]:
        """Sorted (key, value) pairs; tombstones appear as value=None."""
        result = []
        for k, v in self._data.items():
            result.append((k, None if v is _TOMBSTONE else v))  # type: ignore
        return result

    def clear(self) -> None:
        self._data.clear()

    # ------------------------------------------------------------------
    # Properties
    # ------------------------------------------------------------------

    def __len__(self) -> int:
        return len(self._data)

    @property
    def capacity(self) -> int:
        return self._capacity
