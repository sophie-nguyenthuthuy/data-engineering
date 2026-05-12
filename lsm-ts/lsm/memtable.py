"""In-memory write buffer (memtable).

Backed by a sorted dict so iteration yields keys in order,
enabling O(n) SSTable construction during flush.

A "tombstone" (None value) marks deleted keys.
"""
from __future__ import annotations

from sortedcontainers import SortedDict

from .types import TSKey, TSValue

_TOMBSTONE = None


class Memtable:
    def __init__(self, size_limit_bytes: int = 64 * 1024 * 1024):
        self._data: SortedDict = SortedDict()
        self._size_bytes = 0
        self.size_limit = size_limit_bytes

    # --- mutations ---

    def put(self, key: TSKey, value: TSValue) -> None:
        encoded_key = key.encode()
        old = self._data.get(encoded_key)
        if old is not None:
            self._size_bytes -= len(old)
        self._data[encoded_key] = value.encode()
        self._size_bytes += len(encoded_key) + TSValue.SIZE

    def delete(self, key: TSKey) -> None:
        encoded_key = key.encode()
        if encoded_key in self._data:
            self._size_bytes -= len(encoded_key) + TSValue.SIZE
        self._data[encoded_key] = _TOMBSTONE

    # --- reads ---

    def get(self, key: TSKey) -> TSValue | None:
        raw = self._data.get(key.encode())
        if raw is _TOMBSTONE:
            return None  # deleted
        if raw is None:
            return None  # not found
        return TSValue.decode(raw)

    def range_scan(
        self,
        start_key: TSKey,
        end_key: TSKey,
    ) -> list[tuple[bytes, bytes | None]]:
        """Return encoded (key, value|None) pairs in [start, end)."""
        start = start_key.encode()
        end = end_key.encode()
        result = []
        for k in self._data.irange(start, end, inclusive=(True, False)):
            result.append((k, self._data[k]))
        return result

    def prefix_scan(self, prefix: bytes) -> list[tuple[bytes, bytes | None]]:
        """All entries whose encoded key starts with prefix."""
        result = []
        for k in self._data.irange(prefix, None):
            if not k.startswith(prefix):
                break
            result.append((k, self._data[k]))
        return result

    # --- introspection ---

    @property
    def size_bytes(self) -> int:
        return self._size_bytes

    @property
    def is_full(self) -> bool:
        return self._size_bytes >= self.size_limit

    def __len__(self) -> int:
        return len(self._data)

    def items(self):
        """Yield (encoded_key, encoded_value_or_None) in sorted order."""
        return self._data.items()

    def clear(self) -> None:
        self._data.clear()
        self._size_bytes = 0
