"""Append-only "cold" history tier.

The production analogue is partitioned Parquet on object storage; this
implementation keeps a list per ``(entity, feature)`` and is thread-safe.
The cold tier never evicts, so the resolver can fall back to it when the
hot tier has rotated past the snapshot the caller cares about.
"""

from __future__ import annotations

import threading
from collections import defaultdict
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any

from cfs.store.version import Version

if TYPE_CHECKING:
    from cfs.clock.vector_clock import VectorClock


@dataclass
class ColdStore:
    """Thread-safe append-only versioned history."""

    _lock: threading.RLock = field(default_factory=threading.RLock, repr=False)
    _data: dict[tuple[str, str], list[Version]] = field(
        default_factory=lambda: defaultdict(list), repr=False
    )

    def write(self, entity: str, feature: str, value: Any, clock: VectorClock, wall: float) -> None:
        if not entity or not feature:
            raise ValueError("entity and feature must be non-empty")
        with self._lock:
            self._data[(entity, feature)].append(Version(value=value, clock=dict(clock), wall=wall))

    def versions(self, entity: str, feature: str) -> list[Version]:
        with self._lock:
            return list(self._data.get((entity, feature), []))

    def n_entries(self) -> int:
        with self._lock:
            return sum(len(v) for v in self._data.values())


__all__ = ["ColdStore"]
