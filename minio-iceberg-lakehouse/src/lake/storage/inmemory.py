"""Thread-safe in-memory object store."""

from __future__ import annotations

import hashlib
import threading
from dataclasses import dataclass, field

from lake.storage.base import CASMismatch, Storage


def _etag(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()[:32]


@dataclass
class InMemoryStorage(Storage):
    """RLock-guarded dict-backed object store with CAS semantics."""

    _objects: dict[str, tuple[bytes, str]] = field(default_factory=dict, repr=False)
    _lock: threading.RLock = field(default_factory=threading.RLock, repr=False)

    def put(self, path: str, data: bytes) -> str:
        if not path:
            raise ValueError("path must be non-empty")
        with self._lock:
            etag = _etag(data)
            self._objects[path] = (data, etag)
            return etag

    def atomic_put(self, path: str, data: bytes, *, expected_etag: str | None) -> str:
        if not path:
            raise ValueError("path must be non-empty")
        with self._lock:
            existing = self._objects.get(path)
            current_etag = existing[1] if existing else None
            if current_etag != expected_etag:
                raise CASMismatch(
                    f"CAS mismatch at {path!r}: expected {expected_etag!r} got {current_etag!r}"
                )
            etag = _etag(data)
            self._objects[path] = (data, etag)
            return etag

    def get(self, path: str) -> bytes:
        with self._lock:
            if path not in self._objects:
                raise KeyError(f"no object at {path!r}")
            return self._objects[path][0]

    def head_etag(self, path: str) -> str | None:
        with self._lock:
            entry = self._objects.get(path)
            return entry[1] if entry else None

    def exists(self, path: str) -> bool:
        with self._lock:
            return path in self._objects


__all__ = ["InMemoryStorage"]
