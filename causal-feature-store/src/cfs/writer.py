"""Thread-safe writer that maintains per-entity vector clocks.

Each call to :meth:`Writer.write` bumps the entity's counter for the
named ``component`` (e.g. the producing service) and then fans the
``(entity, feature, value)`` record out to both the hot and cold tiers
tagged with the new clock. Wall time is taken from ``time.time()`` if
the caller does not supply one — useful for tests.
"""

from __future__ import annotations

import threading
import time
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any

from cfs.clock.vector_clock import bump

if TYPE_CHECKING:
    from cfs.store.cold import ColdStore
    from cfs.store.hot import HotStore


@dataclass
class Writer:
    """Single-region writer with a per-entity vector clock."""

    hot: HotStore
    cold: ColdStore
    _lock: threading.RLock = field(default_factory=threading.RLock, repr=False)
    _entity_clocks: dict[str, dict[str, int]] = field(default_factory=dict, repr=False)

    def write(
        self,
        entity: str,
        component: str,
        feature: str,
        value: Any,
        wall: float | None = None,
    ) -> dict[str, int]:
        """Persist ``value`` and return the post-bump entity clock."""
        if not entity:
            raise ValueError("entity must be non-empty")
        if not component:
            raise ValueError("component must be non-empty")
        if not feature:
            raise ValueError("feature must be non-empty")
        actual_wall = time.time() if wall is None else wall
        with self._lock:
            new_clock = bump(self._entity_clocks.get(entity, {}), component)
            self._entity_clocks[entity] = new_clock
            self.hot.write(entity, feature, value, clock=new_clock, wall=actual_wall)
            self.cold.write(entity, feature, value, clock=new_clock, wall=actual_wall)
            return dict(new_clock)

    def current_clock(self, entity: str) -> dict[str, int]:
        with self._lock:
            return dict(self._entity_clocks.get(entity, {}))


__all__ = ["Writer"]
