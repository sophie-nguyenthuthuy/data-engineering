"""Abstract StorageBackend interface."""

from __future__ import annotations

import abc
from typing import Iterator


class StorageBackend(abc.ABC):
    """
    Abstract key-value storage backend with column-family support.

    Column families are created lazily. Keys and values are raw bytes.
    Values stored by the state layer always include an 8-byte timestamp
    prefix written by the caller; the backend is timestamp-unaware.
    """

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    @abc.abstractmethod
    def open(self) -> None:
        """Open (or create) the underlying storage."""

    @abc.abstractmethod
    def close(self) -> None:
        """Flush and close the underlying storage."""

    # ------------------------------------------------------------------
    # Column-family management
    # ------------------------------------------------------------------

    @abc.abstractmethod
    def create_cf(self, cf_name: str) -> None:
        """Create a column family if it does not yet exist."""

    @abc.abstractmethod
    def drop_cf(self, cf_name: str) -> None:
        """Drop a column family and all its data."""

    @abc.abstractmethod
    def list_cfs(self) -> list[str]:
        """Return names of all existing column families."""

    # ------------------------------------------------------------------
    # Single-key operations
    # ------------------------------------------------------------------

    @abc.abstractmethod
    def put(self, cf: str, key: bytes, value: bytes) -> None:
        """Write *key* → *value* in column family *cf*."""

    @abc.abstractmethod
    def get(self, cf: str, key: bytes) -> bytes | None:
        """Return the value for *key* in *cf*, or ``None`` if absent."""

    @abc.abstractmethod
    def delete(self, cf: str, key: bytes) -> None:
        """Delete *key* from *cf* (no-op if absent)."""

    # ------------------------------------------------------------------
    # Range / prefix scan
    # ------------------------------------------------------------------

    @abc.abstractmethod
    def scan(
        self,
        cf: str,
        prefix: bytes = b"",
        start_key: bytes | None = None,
        limit: int | None = None,
    ) -> Iterator[tuple[bytes, bytes]]:
        """
        Iterate ``(key, value)`` pairs in *cf* in lexicographic order.

        Parameters
        ----------
        cf:
            Column-family name.
        prefix:
            If non-empty, only yield keys that *start with* this prefix.
        start_key:
            If given, begin iteration at (or after) this key.  Used for
            cursor-based pagination in the read API.
        limit:
            Maximum number of pairs to yield.
        """

    # ------------------------------------------------------------------
    # Batch write
    # ------------------------------------------------------------------

    @abc.abstractmethod
    def write_batch(self, operations: list[tuple[str, bytes, bytes | None]]) -> None:
        """
        Apply a list of operations atomically (best-effort for memory backend).

        Each element is one of:
        * ``(cf, key, value)``  → put
        * ``(cf, key, None)``   → delete
        """
