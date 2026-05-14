"""Polling backend — list the prefix and diff against a cursor.

For S3/MinIO deployments that don't (or can't) expose event
notifications, polling is the fallback: list the prefix at a regular
interval and emit a CREATED/MODIFIED event for every object whose key
or ETag is new since the last poll.

The actual list call is parameterised by an injectable ``lister``
callable so tests can drive the backend deterministically.
"""

from __future__ import annotations

from collections.abc import Callable, Iterator
from dataclasses import dataclass, field

from ifw.backends.base import Backend
from ifw.events import EventKind, FileEvent

#: Lister returns ``[(key, size, last_modified_ms, etag), ...]``.
Lister = Callable[[str], list[tuple[str, int, int, str]]]


@dataclass
class PollingBackend(Backend):
    """Stateful polling diff against a list-objects callable."""

    bucket: str
    lister: Lister
    kind: str = "polling"
    _seen: dict[str, str] = field(default_factory=dict, repr=False)

    def __post_init__(self) -> None:
        if not self.bucket:
            raise ValueError("bucket must be non-empty")

    def poll(self) -> Iterator[FileEvent]:
        for key, size, last_modified_ms, etag in self.lister(self.bucket):
            prev = self._seen.get(key)
            if prev == etag:
                continue
            kind = EventKind.CREATED if prev is None else EventKind.MODIFIED
            self._seen[key] = etag
            yield FileEvent(
                bucket=self.bucket,
                key=key,
                size=size,
                last_modified_ms=last_modified_ms,
                etag=etag,
                kind=kind,
            )


__all__ = ["Lister", "PollingBackend"]
