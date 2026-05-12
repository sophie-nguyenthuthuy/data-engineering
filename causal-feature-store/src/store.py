"""Versioned online (hot) and offline (cold) feature stores.

Each store keeps multiple versions per (entity, feature), tagged with the
vector clock at the time of write. The hot store keeps the last K versions;
the cold store is append-only.
"""
from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass, field
from typing import Optional


@dataclass
class Version:
    value: object
    clock: dict       # vector clock at write
    wall: float       # for tie-breaking


@dataclass
class HotStore:
    """In-memory; bounded ring buffer of last K versions per (entity, feature)."""
    k: int = 5
    _data: dict = field(default_factory=lambda: defaultdict(list))
    # entity_clocks: latest clock seen per entity
    _entity_clocks: dict = field(default_factory=dict)

    def write(self, entity: str, feature: str, value, clock: dict, wall: float) -> None:
        key = (entity, feature)
        versions = self._data[key]
        versions.append(Version(value=value, clock=dict(clock), wall=wall))
        # Keep last K (most recent by wall time)
        versions.sort(key=lambda v: v.wall)
        if len(versions) > self.k:
            del versions[: len(versions) - self.k]
        # Update entity clock = pointwise max
        ec = self._entity_clocks.get(entity, {})
        new_ec = dict(ec)
        for c, v in clock.items():
            new_ec[c] = max(new_ec.get(c, 0), v)
        self._entity_clocks[entity] = new_ec

    def entity_clock(self, entity: str) -> dict:
        return dict(self._entity_clocks.get(entity, {}))

    def versions(self, entity: str, feature: str) -> list[Version]:
        return list(self._data.get((entity, feature), []))


@dataclass
class ColdStore:
    """Append-only history. In practice this is partitioned Parquet."""
    _data: dict = field(default_factory=lambda: defaultdict(list))

    def write(self, entity: str, feature: str, value, clock: dict, wall: float) -> None:
        self._data[(entity, feature)].append(
            Version(value=value, clock=dict(clock), wall=wall)
        )

    def versions(self, entity: str, feature: str) -> list[Version]:
        return list(self._data.get((entity, feature), []))


__all__ = ["Version", "HotStore", "ColdStore"]
