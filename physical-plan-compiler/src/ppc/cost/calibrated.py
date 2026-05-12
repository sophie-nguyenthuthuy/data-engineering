"""Cost model: takes a physical op, returns a single number.

This thin layer exists so we can swap in alternate models (e.g. a learned
one) without touching the optimizer.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING

from ppc.engines.physical_ops import (
    PhysicalAggregate,
    PhysicalFilter,
    PhysicalHashJoin,
    PhysicalScan,
)

if TYPE_CHECKING:
    from ppc.ir.physical import PhysicalNode


@dataclass
class CalibratedCostModel:
    """Wraps each PhysicalNode's `cost` property so the optimizer doesn't
    need to know about the engine profiles directly."""

    def cost_of(self, op: PhysicalNode) -> float:
        if isinstance(op, PhysicalScan | PhysicalFilter | PhysicalAggregate | PhysicalHashJoin):
            return op.cost
        return 0.0
