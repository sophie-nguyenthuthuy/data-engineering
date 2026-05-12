"""RocksDB storage backend using column families."""

from __future__ import annotations

import threading
from typing import Iterator

from .base import StorageBackend

try:
    import rocksdb  # type: ignore[import]

    _ROCKSDB_AVAILABLE = True
except ImportError:  # pragma: no cover
    _ROCKSDB_AVAILABLE = False


class RocksDBBackend(StorageBackend):
    """
    RocksDB-backed storage using one column family per
    ``(operator_id, state_name)`` pair.

    Column-family handles are created lazily and cached in
    ``self._cf_handles``.  The special ``__meta__`` CF stores topology
    version info and the CF registry.

    If ``python-rocksdb`` is not installed this class raises
    ``ImportError`` at construction time; callers should fall back to
    ``MemoryBackend``.
    """

    _META_CF = "__meta__"

    def __init__(self, db_path: str) -> None:
        if not _ROCKSDB_AVAILABLE:
            raise ImportError(
                "python-rocksdb is not installed. "
                "Install it with: pip install 'stream-state-backend[rocksdb]'"
            )
        self._db_path = db_path
        self._db: "rocksdb.DB | None" = None
        self._cf_handles: dict[str, "rocksdb.ColumnFamilyHandle"] = {}
        self._lock = threading.RLock()

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    def open(self) -> None:
        with self._lock:
            if self._db is not None:
                return

            opts = rocksdb.Options()
            opts.create_if_missing = True
            opts.create_missing_column_families = True

            # Discover existing CFs on disk so we can open them all at once.
            try:
                existing_cfs = rocksdb.list_column_families(self._db_path, opts)
            except Exception:
                existing_cfs = ["default"]

            # Ensure __meta__ is always present
            cf_names = list(existing_cfs)
            if self._META_CF not in cf_names:
                cf_names.append(self._META_CF)

            cf_opts = {name: rocksdb.ColumnFamilyOptions() for name in cf_names}
            self._db, handles = rocksdb.DB.open_with_column_families(
                self._db_path, opts, cf_opts
            )
            # Build name → handle mapping
            for name, handle in zip(cf_names, handles):
                self._cf_handles[name] = handle

    def close(self) -> None:
        with self._lock:
            if self._db is not None:
                # Explicitly release handles before closing
                self._cf_handles.clear()
                del self._db
                self._db = None

    # ------------------------------------------------------------------
    # Column-family management
    # ------------------------------------------------------------------

    def create_cf(self, cf_name: str) -> None:
        with self._lock:
            if cf_name in self._cf_handles:
                return
            cf_opts = rocksdb.ColumnFamilyOptions()
            handle = self._db.create_column_family(cf_name, cf_opts)
            self._cf_handles[cf_name] = handle

    def drop_cf(self, cf_name: str) -> None:
        with self._lock:
            if cf_name not in self._cf_handles:
                return
            self._db.drop_column_family(self._cf_handles[cf_name])
            del self._cf_handles[cf_name]

    def list_cfs(self) -> list[str]:
        with self._lock:
            return list(self._cf_handles.keys())

    # ------------------------------------------------------------------
    # Single-key operations
    # ------------------------------------------------------------------

    def put(self, cf: str, key: bytes, value: bytes) -> None:
        with self._lock:
            handle = self._get_cf_handle(cf)
            self._db.put((handle, key), value)

    def get(self, cf: str, key: bytes) -> bytes | None:
        with self._lock:
            handle = self._get_cf_handle(cf)
            return self._db.get((handle, key))

    def delete(self, cf: str, key: bytes) -> None:
        with self._lock:
            handle = self._get_cf_handle(cf)
            self._db.delete((handle, key))

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
            handle = self._get_cf_handle(cf)
            it = self._db.iteritems(handle)

            seek_key = start_key if start_key is not None else prefix if prefix else b""
            if seek_key:
                it.seek(seek_key)
            else:
                it.seek_to_first()

            count = 0
            for k, v in it:
                if prefix and not k.startswith(prefix):
                    break
                yield k, v
                count += 1
                if limit is not None and count >= limit:
                    break

    # ------------------------------------------------------------------
    # Batch write
    # ------------------------------------------------------------------

    def write_batch(self, operations: list[tuple[str, bytes, bytes | None]]) -> None:
        with self._lock:
            batch = rocksdb.WriteBatch()
            for cf, key, value in operations:
                handle = self._get_cf_handle(cf)
                if value is None:
                    batch.delete((handle, key))
                else:
                    batch.put((handle, key), value)
            self._db.write(batch)

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _get_cf_handle(self, cf_name: str) -> "rocksdb.ColumnFamilyHandle":
        """Return cached handle, creating CF lazily if needed."""
        if cf_name not in self._cf_handles:
            self.create_cf(cf_name)
        return self._cf_handles[cf_name]
