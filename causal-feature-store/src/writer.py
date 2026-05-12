"""Writer: bumps entity clock per component, writes to hot + cold."""
from __future__ import annotations

import time
from dataclasses import dataclass, field

from .store import HotStore, ColdStore
from .vector_clock import bump


@dataclass
class Writer:
    hot: HotStore
    cold: ColdStore
    # entity → per-component counter
    _entity_clocks: dict = field(default_factory=dict)

    def write(self, entity: str, component: str, feature: str, value, wall: float | None = None):
        """Write a feature value produced by `component`."""
        wall = wall or time.time()
        cur = self._entity_clocks.get(entity, {})
        new = bump(cur, component)
        self._entity_clocks[entity] = new
        self.hot.write(entity, feature, value, clock=new, wall=wall)
        self.cold.write(entity, feature, value, clock=new, wall=wall)
        return dict(new)


__all__ = ["Writer"]
