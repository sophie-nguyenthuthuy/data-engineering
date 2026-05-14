"""Pre-loaded in-memory event source — primarily for tests."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import TYPE_CHECKING

from ifw.backends.base import Backend

if TYPE_CHECKING:
    from collections.abc import Iterator

    from ifw.events import FileEvent


@dataclass
class InMemoryBackend(Backend):
    """Drains a buffered list of events on each :meth:`poll`."""

    events: list[FileEvent] = field(default_factory=list)
    kind: str = "inmemory"

    def push(self, event: FileEvent) -> None:
        self.events.append(event)

    def poll(self) -> Iterator[FileEvent]:
        # Drain into a tuple so concurrent push() during yield is fine.
        snapshot = tuple(self.events)
        self.events.clear()
        yield from snapshot


__all__ = ["InMemoryBackend"]
