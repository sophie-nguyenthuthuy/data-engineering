"""Dedupe by manifest key.

The deduplicator maintains an in-memory set of dedupe keys; the
:class:`Runner` rebuilds it from the manifest on startup and inserts
into both the set + the manifest on every successful processing.
Re-running the watcher on the same backend is therefore a no-op for
events the manifest has already absorbed.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from ifw.events import FileEvent
    from ifw.manifest import Manifest


@dataclass
class Deduplicator:
    """In-memory dedupe key set, optionally hydrated from a manifest."""

    seen: set[str] = field(default_factory=set)

    @classmethod
    def from_manifest(cls, manifest: Manifest) -> Deduplicator:
        return cls(seen=set(manifest.keys()))

    def is_new(self, event: FileEvent) -> bool:
        return event.dedupe_key() not in self.seen

    def remember(self, event: FileEvent) -> None:
        self.seen.add(event.dedupe_key())


__all__ = ["Deduplicator"]
