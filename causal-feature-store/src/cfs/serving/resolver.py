"""Causally consistent feature-vector resolver.

For each request the resolver:

  1. Reads the entity's current vector clock ``target`` from the hot tier.
  2. For every requested feature, finds the latest stored :class:`Version`
     whose write clock is ``≤ target`` (falling back to cold history if
     the hot tier no longer holds it).
  3. Returns the union of those values plus ``chosen_clock`` — the
     pointwise max of the per-feature clocks. By construction every
     returned feature was written at a clock ``≤ chosen_clock``, so the
     resulting vector is a single causally consistent snapshot.

A feature with *no* stored version dominated by ``target`` is reported
in :attr:`ResolvedVector.missing`; the caller decides whether to retry
or fall back to a default.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any

from cfs.clock.vector_clock import dominates, pointwise_max

if TYPE_CHECKING:
    from cfs.store.cold import ColdStore
    from cfs.store.hot import HotStore
    from cfs.store.version import Version


@dataclass(frozen=True, slots=True)
class ResolvedVector:
    """Resolver output: the snapshot and any unresolvable features."""

    features: dict[str, Any]
    chosen_clock: dict[str, int]
    missing: list[str] = field(default_factory=list)

    def is_complete(self) -> bool:
        return not self.missing


@dataclass
class Resolver:
    """Reads ``HotStore`` (and optionally ``ColdStore``) to assemble snapshots."""

    hot: HotStore
    cold: ColdStore | None = None

    def _best_version(self, versions: list[Version], target: dict[str, int]) -> Version | None:
        """Pick the latest (by wall time) version whose clock ``≤ target``."""
        candidates = [v for v in versions if dominates(target, v.clock)]
        if not candidates:
            return None
        return max(candidates, key=lambda v: v.wall)

    def get(self, entity: str, features: list[str]) -> ResolvedVector:
        if not entity:
            raise ValueError("entity must be non-empty")
        if not features:
            return ResolvedVector(features={}, chosen_clock={}, missing=[])
        target = self.hot.entity_clock(entity)
        chosen: dict[str, Version] = {}
        missing: list[str] = []
        for f in features:
            versions = self.hot.versions(entity, f)
            if self.cold is not None:
                versions = versions + self.cold.versions(entity, f)
            v = self._best_version(versions, target)
            if v is None:
                missing.append(f)
            else:
                chosen[f] = v
        chosen_clock = pointwise_max(*(v.clock for v in chosen.values())) if chosen else {}
        return ResolvedVector(
            features={f: v.value for f, v in chosen.items()},
            chosen_clock=chosen_clock,
            missing=missing,
        )

    def verify(self, entity: str, rv: ResolvedVector) -> bool:
        """Check that every returned value really comes from a clock ≤ chosen_clock."""
        for f in rv.features:
            versions = self.hot.versions(entity, f)
            if self.cold is not None:
                versions = versions + self.cold.versions(entity, f)
            matches = [
                v
                for v in versions
                if v.value == rv.features[f] and dominates(rv.chosen_clock, v.clock)
            ]
            if not matches:
                return False
        return True


__all__ = ["ResolvedVector", "Resolver"]
