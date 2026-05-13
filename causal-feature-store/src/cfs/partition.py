"""Network-partition simulator for Jepsen-style consistency tests.

In a real deployment two writers behind different partitions don't see
each other's clock bumps. We model that by giving each side its own
private :class:`Writer` against shared :class:`HotStore` /
:class:`ColdStore` instances, but artificially *zeroing-out* the
counters of components the other side cannot observe at write time.

After the partition heals, both sides' clocks are surfaced via the
shared entity clock in the hot store, and the resolver picks the most
recent causally consistent snapshot across both partitions.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any

from cfs.serving.resolver import Resolver
from cfs.store.cold import ColdStore
from cfs.store.hot import HotStore
from cfs.writer import Writer

if TYPE_CHECKING:
    from cfs.serving.resolver import ResolvedVector


@dataclass
class PartitionScenario:
    """Two writers ("a"/"b") that diverge, then heal.

    ``writers`` is a mapping from side label to :class:`Writer`. Both
    writers share the same backing hot + cold stores; what makes them
    "partitioned" is that the bumps from one side are recorded against
    different component names, so neither pre-heal clock dominates the
    other.
    """

    hot: HotStore = field(default_factory=HotStore)
    cold: ColdStore = field(default_factory=ColdStore)
    writers: dict[str, Writer] = field(default_factory=dict)
    healed: bool = False

    def __post_init__(self) -> None:
        if not self.writers:
            self.writers = {
                "a": Writer(hot=self.hot, cold=self.cold),
                "b": Writer(hot=self.hot, cold=self.cold),
            }

    # ---------------------------------------------------------------- ops

    def write_on(
        self,
        side: str,
        entity: str,
        component: str,
        feature: str,
        value: Any,
        wall: float | None = None,
    ) -> dict[str, int]:
        """Write through one side's writer."""
        if side not in self.writers:
            raise ValueError(f"unknown side {side!r}; expected one of {list(self.writers)}")
        if not self.healed and component in self._foreign_components(side):
            raise ValueError(f"side {side!r} cannot write component {component!r} during partition")
        return self.writers[side].write(entity, component, feature, value, wall=wall)

    def heal(self) -> None:
        """Allow either side to write any component from now on."""
        self.healed = True

    def resolver(self) -> Resolver:
        return Resolver(hot=self.hot, cold=self.cold)

    def get(self, entity: str, features: list[str]) -> ResolvedVector:
        return self.resolver().get(entity, features)

    # --------------------------------------------------------------- helpers

    @staticmethod
    def _foreign_components(side: str) -> set[str]:
        """Component names the given side is forbidden from writing pre-heal."""
        if side == "a":
            return {"compB"}
        if side == "b":
            return {"compA"}
        return set()


__all__ = ["PartitionScenario"]
