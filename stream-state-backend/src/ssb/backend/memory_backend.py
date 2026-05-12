"""In-memory storage backend backed by sortedcontainers.SortedDict."""

from __future__ import annotations

import threading
from typing import Iterator

from sortedcontainers import SortedDict

from .base import StorageBackend


class MemoryBackend(StorageBackend):
    """
    Thread-safe, dict-backed storage backend used in tests and as the
    graceful fall-back when python-rocksdb is not installed.

    Each column family is a ``SortedDict[bytes, bytes]`` so that prefix
    scans and pagination behave the same as RocksDB.
    """

    def __init__(self) -> None:
        # cf_name → SortedDict[bytes, bytes]
        self._stores: dict[str, SortedDict] = {}
        self._lock = threading.RLock()
        self._open = False

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    def open(self) -> None:
        with self._lock:
            if not self._open:
                # Always ensure the meta CF exists
                self._stores.setdefault("__meta__", SortedDict())
                self._open = True

    def close(self) -> None:
        with self._lock:
            self._open = False

    # ------------------------------------------------------------------
    # Column-family management
    # ------------------------------------------------------------------

    def create_cf(self, cf_name: str) -> None:
        with self._lock:
            self._stores.setdefault(cf_name, SortedDict())

    def drop_cf(self, cf_name: str) -> None:
        with self._lock:
            self._stores.pop(cf_name, None)

    def list_cfs(self) -> list[str]:
        with self._lock:
            return list(self._stores.keys())

    # ------------------------------------------------------------------
    # Single-key operations
    # ------------------------------------------------------------------

    def put(self, cf: str, key: bytes, value: bytes) -> None:
        with self._lock:
            store = self._get_store(cf)
            store[key] = value

    def get(self, cf: str, key: bytes) -> bytes | None:
        with self._lock:
            store = self._stores.get(cf)
            if store is None:
                return None
            return store.get(key)

    def delete(self, cf: str, key: bytes) -> None:
        with self._lock:
            store = self._stores.get(cf)
            if store is not None:
                store.pop(key, None)

    # ------------------------------------------------------------------
    # Range / prefix scan
    # ------------------------------------------------------------------

    def scan(
        self,
        cf: str,
        prefix: bytes = b"",
        start_key: bytes | None = None,
        limit: int | None = None,
    ) -> Iterator[tuple[bytes, bytes]]:
        with self._lock:
            store = self._stores.get(cf)
            if store is None:
                return

            # Determine starting index
            if start_key is not None:
                idx = store.bisect_left(start_key)
            elif prefix:
                idx = store.bisect_left(prefix)
            else:
                idx = 0

            keys = store.keys()
            count = 0
            while idx < len(keys):
                k = keys[idx]
                if prefix and not k.startswith(prefix):
                    break
                v = store[k]
                yield k, v
                count += 1
                if limit is not None and count >= limit:
                    break
                idx += 1

    # ------------------------------------------------------------------
    # Batch write
    # ------------------------------------------------------------------

    def write_batch(self, operations: list[tuple[str, bytes, bytes | None]]) -> None:
        with self._lock:
            for cf, key, value in operations:
                if value is None:
                    store = self._stores.get(cf)
                    if store is not None:
                        store.pop(key, None)
                else:
                    store = self._get_store(cf)
                    store[key] = value

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _get_store(self, cf: str) -> SortedDict:
        """Return store for *cf*, creating it if necessary."""
        if cf not in self._stores:
            self._stores[cf] = SortedDict()
        return self._stores[cf]
