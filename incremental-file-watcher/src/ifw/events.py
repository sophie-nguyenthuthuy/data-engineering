"""File-event types.

A :class:`FileEvent` is the watcher's atomic unit of work. Every
backend (event-driven S3-SQS, polling, in-memory) emits the same shape
so the downstream :class:`Runner` doesn't care where the event came
from.
"""

from __future__ import annotations

from dataclasses import dataclass
from enum import Enum


class EventKind(str, Enum):
    """Lifecycle action that produced the event."""

    CREATED = "created"
    MODIFIED = "modified"
    DELETED = "deleted"


@dataclass(frozen=True, slots=True)
class FileEvent:
    """A single observation of an object on the watched store.

    Fields:
      * ``bucket`` — logical bucket (or root prefix) the file lives in.
      * ``key`` — object key relative to ``bucket``.
      * ``size`` — bytes; ``0`` is allowed for empty files.
      * ``last_modified_ms`` — server-side mtime in UTC ms-since-epoch.
      * ``etag`` — content fingerprint (S3 ETag or content-hash).
      * ``kind`` — :class:`EventKind`.
    """

    bucket: str
    key: str
    size: int
    last_modified_ms: int
    etag: str
    kind: EventKind = EventKind.CREATED

    def __post_init__(self) -> None:
        if not self.bucket:
            raise ValueError("bucket must be non-empty")
        if not self.key:
            raise ValueError("key must be non-empty")
        if self.size < 0:
            raise ValueError("size must be ≥ 0")
        if self.last_modified_ms < 0:
            raise ValueError("last_modified_ms must be ≥ 0")
        if not self.etag:
            raise ValueError("etag must be non-empty")

    def dedupe_key(self) -> str:
        """Stable identity used by the manifest."""
        return f"{self.bucket}/{self.key}#{self.etag}"


__all__ = ["EventKind", "FileEvent"]
