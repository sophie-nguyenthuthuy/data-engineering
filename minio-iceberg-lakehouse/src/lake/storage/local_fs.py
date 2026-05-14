"""Local-filesystem storage.

Atomicity is provided by ``os.replace`` (POSIX guarantees an atomic
rename within the same filesystem); CAS is enforced by reading the
current file + computing its etag inside the storage lock so two
writers don't trample each other.
"""

from __future__ import annotations

import hashlib
import os
import threading
from dataclasses import dataclass, field
from pathlib import Path

from lake.storage.base import CASMismatch, Storage


def _etag(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()[:32]


@dataclass
class LocalFSStorage(Storage):
    """File-system backed storage. ``root`` is the prefix for all paths."""

    root: Path
    _lock: threading.RLock = field(default_factory=threading.RLock, repr=False)

    def __post_init__(self) -> None:
        if not isinstance(self.root, Path):
            self.root = Path(self.root)
        self.root.mkdir(parents=True, exist_ok=True)

    def _resolve(self, path: str) -> Path:
        if not path or path.startswith("/"):
            raise ValueError("path must be a non-empty relative key")
        return self.root / path

    def put(self, path: str, data: bytes) -> str:
        target = self._resolve(path)
        target.parent.mkdir(parents=True, exist_ok=True)
        tmp = target.with_suffix(target.suffix + ".tmp")
        with self._lock:
            tmp.write_bytes(data)
            os.replace(tmp, target)
            return _etag(data)

    def atomic_put(self, path: str, data: bytes, *, expected_etag: str | None) -> str:
        target = self._resolve(path)
        target.parent.mkdir(parents=True, exist_ok=True)
        with self._lock:
            current = target.read_bytes() if target.exists() else None
            current_etag = _etag(current) if current is not None else None
            if current_etag != expected_etag:
                raise CASMismatch(
                    f"CAS mismatch at {path!r}: expected {expected_etag!r} got {current_etag!r}"
                )
            tmp = target.with_suffix(target.suffix + ".tmp")
            tmp.write_bytes(data)
            os.replace(tmp, target)
            return _etag(data)

    def get(self, path: str) -> bytes:
        target = self._resolve(path)
        if not target.exists():
            raise KeyError(f"no object at {path!r}")
        return target.read_bytes()

    def head_etag(self, path: str) -> str | None:
        target = self._resolve(path)
        if not target.exists():
            return None
        return _etag(target.read_bytes())

    def exists(self, path: str) -> bool:
        return self._resolve(path).exists()


__all__ = ["LocalFSStorage"]
