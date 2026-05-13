"""In-memory bounded-history "hot" store.

Holds the last ``k`` :class:`Version` records per ``(entity, feature)``.
Also tracks the highest vector clock observed for each entity, which the
resolver consults as the *target snapshot*.

The store is safe for concurrent writers and readers — every public
method takes an :class:`~threading.RLock` so a reader can chain a
:meth:`entity_clock` lookup with multiple :meth:`versions` calls without
the snapshot moving under it.
"""

from __future__ import annotations

import threading
from collections import defaultdict
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any

from cfs.clock.vector_clock import pointwise_max
from cfs.store.version import Version

if TYPE_CHECKING:
    from cfs.clock.vector_clock import VectorClock


@dataclass
class HotStore:
    """Thread-safe bounded-version online tier."""

    k: int = 5
    _lock: threading.RLock = field(default_factory=threading.RLock, repr=False)
    _data: dict[tuple[str, str], list[Version]] = field(
        default_factory=lambda: defaultdict(list), repr=False
    )
    _entity_clocks: dict[str, dict[str, int]] = field(default_factory=dict, repr=False)

    def __post_init__(self) -> None:
        if self.k < 1:
            raise ValueError("k must be ≥ 1")

    # ------------------------------------------------------------------ write

    def write(self, entity: str, feature: str, value: Any, clock: VectorClock, wall: float) -> None:
        """Append ``value`` tagged with ``clock`` / ``wall``; evict beyond k."""
        if not entity or not feature:
            raise ValueError("entity and feature must be non-empty")
        with self._lock:
            key = (entity, feature)
            versions = self._data[key]
            versions.append(Version(value=value, clock=dict(clock), wall=wall))
            versions.sort(key=lambda v: v.wall)
            if len(versions) > self.k:
                del versions[: len(versions) - self.k]
            # Bump the entity-level clock to the pointwise max.
            self._entity_clocks[entity] = pointwise_max(self._entity_clocks.get(entity, {}), clock)

    # ------------------------------------------------------------------- read

    def entity_clock(self, entity: str) -> dict[str, int]:
        with self._lock:
            return dict(self._entity_clocks.get(entity, {}))

    def versions(self, entity: str, feature: str) -> list[Version]:
        with self._lock:
            return list(self._data.get((entity, feature), []))

    def n_entries(self) -> int:
        with self._lock:
            return sum(len(v) for v in self._data.values())


__all__ = ["HotStore"]
