"""Object-store protocol.

Iceberg expects two primitives from the storage layer:

  * ``put(path, bytes)`` — write a new immutable file. Manifests, data
    files, and metadata blobs all go through this.
  * ``atomic_put(path, bytes, *, expected_etag)`` — compare-and-swap
    the metadata pointer. This is what gives Iceberg its ACID story.

Real S3/MinIO provide CAS via ``If-Match`` headers; we expose the same
shape so the table layer is wire-format-agnostic.
"""

from __future__ import annotations

from abc import ABC, abstractmethod


class StorageError(RuntimeError):
    """Raised when storage cannot honour a write."""


class CASMismatch(StorageError):
    """Raised when ``atomic_put`` finds the current etag != expected."""


class Storage(ABC):
    """Minimal object-store interface."""

    @abstractmethod
    def put(self, path: str, data: bytes) -> str:
        """Write a file; returns the etag of the new object."""

    @abstractmethod
    def atomic_put(self, path: str, data: bytes, *, expected_etag: str | None) -> str:
        """Compare-and-swap; returns the new etag on success."""

    @abstractmethod
    def get(self, path: str) -> bytes: ...

    @abstractmethod
    def head_etag(self, path: str) -> str | None: ...

    @abstractmethod
    def exists(self, path: str) -> bool: ...


__all__ = ["CASMismatch", "Storage", "StorageError"]
