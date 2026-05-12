"""Physical operators: realised forms of logical ops on a specific engine.

A PhysicalNode carries:
  - a reference to its logical group (for memoization / explain)
  - the engine it executes on
  - the input properties it requires (engine, partitioning, sort)
  - the output properties it delivers
  - per-engine codegen hooks (handled in `ppc.codegen.*`)
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import TYPE_CHECKING, ClassVar

if TYPE_CHECKING:
    from ppc.ir.logical import LogicalNode
    from ppc.ir.schema import Schema


@dataclass(frozen=True, slots=True)
class PhysicalProperties:
    """Properties tracked by the optimizer for plan matching.

    engine:        which runtime owns the output data
    partitioning:  partition keys, or "single" / "any"
    sort_order:    columns the data is sorted by (in order)
    """

    engine: str = "any"
    partitioning: tuple[str, ...] = ()
    sort_order: tuple[str, ...] = ()

    def satisfies(self, required: PhysicalProperties) -> bool:
        if required.engine != "any" and required.engine != self.engine:
            return False
        if required.partitioning and required.partitioning != self.partitioning:
            return False
        if required.sort_order:
            # delivered must start with the required sort prefix
            if len(self.sort_order) < len(required.sort_order):
                return False
            for a, b in zip(self.sort_order, required.sort_order, strict=False):
                if a != b:
                    return False
        return True


class PhysicalNode:
    """Base class. Subclasses are frozen dataclasses."""

    kind: ClassVar[str] = ""

    @property
    def schema(self) -> Schema:  # pragma: no cover
        raise NotImplementedError

    @property
    def engine(self) -> str:  # pragma: no cover
        raise NotImplementedError

    @property
    def cost(self) -> float:
        return 0.0

    @property
    def children(self) -> tuple[PhysicalNode, ...]:
        return ()

    @property
    def delivered_properties(self) -> PhysicalProperties:
        return PhysicalProperties(engine=self.engine)

    def explain(self, indent: int = 0) -> str:
        prefix = "  " * indent
        body = self._explain_self()
        out = [f"{prefix}{body}"]
        for c in self.children:
            out.append(c.explain(indent + 1))
        return "\n".join(out)

    def _explain_self(self) -> str:  # pragma: no cover
        return self.__class__.__name__


@dataclass(frozen=True, slots=True)
class PhysicalPlan:
    """The optimizer's output: a fully-realised plan, ready for codegen.

    Carries the root PhysicalNode and the total estimated cost (sum across
    the tree) for reporting.
    """

    root: PhysicalNode
    total_cost: float
    estimated_bytes: float
    logical: LogicalNode = field(repr=False)

    def explain(self) -> str:
        return self.root.explain()
