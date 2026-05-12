"""Physical properties propagated during search.

A property describes a `where this data is` / `what shape this data is in`
aspect of an operator's output. The optimizer:

  1. Computes the *required* properties for each parent operator.
  2. Computes the *delivered* properties of each child plan.
  3. Inserts enforcer operators (e.g. ExchangeOp, ConversionOp) where needed.
"""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True, slots=True)
class PhysicalProperties:
    engine: str = "any"                       # "spark" | "dbt" | "duckdb" | "flink" | "any"
    partitioning: tuple[str, ...] = ()        # hash-partition keys; () = single
    sort_order: tuple[str, ...] = ()          # sorted-by columns

    @classmethod
    def any(cls) -> PhysicalProperties:
        return cls()

    @classmethod
    def on(cls, engine: str) -> PhysicalProperties:
        return cls(engine=engine)

    def satisfies(self, required: PhysicalProperties) -> bool:
        """Does this set of *delivered* properties satisfy a *required* set?"""
        if required.engine != "any" and required.engine != self.engine:
            return False
        if required.partitioning and required.partitioning != self.partitioning:
            return False
        if required.sort_order:
            n = len(required.sort_order)
            if len(self.sort_order) < n:
                return False
            if self.sort_order[:n] != required.sort_order:
                return False
        return True

    def with_engine(self, engine: str) -> PhysicalProperties:
        return PhysicalProperties(engine=engine, partitioning=self.partitioning,
                                  sort_order=self.sort_order)
