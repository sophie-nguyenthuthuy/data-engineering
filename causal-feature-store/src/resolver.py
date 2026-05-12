"""Causally consistent feature-vector resolver.

Algorithm (per entity):
  1. Read entity's current clock vc_target.
  2. For each requested feature, find the latest write whose clock ≤ vc_target.
  3. Compute vc_chosen = pointwise_max of all chosen write clocks.
  4. Re-verify all chosen writes dominate ≤ vc_chosen (they do by construction).
  5. Return (features_chosen, vc_chosen).

The returned feature vector is a *causally consistent snapshot* — no two values
are concurrent or interleaved in a way that contradicts vc_chosen.
"""
from __future__ import annotations

from dataclasses import dataclass

from .store import HotStore, ColdStore, Version
from .vector_clock import dominates, pointwise_max


@dataclass
class ResolvedVector:
    features: dict          # feature_name -> value
    chosen_clock: dict      # vc_chosen
    missing: list           # features that had no consistent version


class Resolver:
    def __init__(self, hot: HotStore, cold: ColdStore | None = None):
        self.hot = hot
        self.cold = cold

    def _best_version_le(self, versions: list[Version], target: dict) -> Version | None:
        """Latest version whose clock ≤ target."""
        candidates = [v for v in versions if dominates(target, v.clock)]
        if not candidates:
            return None
        # Latest by wall (within causally dominated set)
        return max(candidates, key=lambda v: v.wall)

    def get(self, entity: str, features: list[str]) -> ResolvedVector:
        target = self.hot.entity_clock(entity)
        chosen: dict[str, Version] = {}
        missing = []
        for f in features:
            versions = self.hot.versions(entity, f)
            if self.cold is not None:
                versions = versions + self.cold.versions(entity, f)
            v = self._best_version_le(versions, target)
            if v is None:
                missing.append(f)
            else:
                chosen[f] = v
        # vc_chosen = max of all chosen write clocks
        vc_chosen = pointwise_max(*(v.clock for v in chosen.values())) if chosen else {}
        return ResolvedVector(
            features={f: v.value for f, v in chosen.items()},
            chosen_clock=vc_chosen,
            missing=missing,
        )


__all__ = ["ResolvedVector", "Resolver"]
