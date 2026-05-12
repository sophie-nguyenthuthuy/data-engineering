"""Concrete physical operators.

Each carries its engine, schemas, and a `cost` derived from the engine
profile. Children are kept as a tuple of PhysicalNode (set by the optimizer
at materialization time via `with_children`).
"""

from __future__ import annotations

from dataclasses import dataclass, field, replace
from typing import ClassVar

from ppc.engines.base import ENGINE_PROFILES, EngineOp
from ppc.ir.expr import Expr
from ppc.ir.logical import AggFunc
from ppc.ir.physical import PhysicalNode, PhysicalProperties
from ppc.ir.schema import Schema
from ppc.ir.expr import ColumnRef


# ---------------------------------------------------------------------------
# Selectivity helpers (used by cost model + cardinality estimate)
# ---------------------------------------------------------------------------


def estimate_selectivity(predicate: Expr, schema: Schema) -> float:
    """Cheap heuristic. For real systems we'd consult histograms."""
    from ppc.ir.expr import BinaryOp, ColumnRef, Literal

    if isinstance(predicate, BinaryOp):
        op = predicate.op
        # column = literal → 1/NDV
        if op == "=":
            if isinstance(predicate.left, ColumnRef) and isinstance(predicate.right, Literal):
                ndv = schema[predicate.left.name].stats.ndv
                if ndv:
                    return 1.0 / ndv
            return 0.1
        if op in {"<", "<=", ">", ">="}:
            return 0.33
        if op in {"!="}:
            if isinstance(predicate.left, ColumnRef):
                ndv = schema[predicate.left.name].stats.ndv
                if ndv:
                    return 1.0 - (1.0 / ndv)
            return 0.9
        if op == "AND":
            return estimate_selectivity(predicate.left, schema) * \
                   estimate_selectivity(predicate.right, schema)
        if op == "OR":
            a = estimate_selectivity(predicate.left, schema)
            b = estimate_selectivity(predicate.right, schema)
            return min(1.0, a + b - a * b)
    return 0.1


# ---------------------------------------------------------------------------
# Scan
# ---------------------------------------------------------------------------


@dataclass(frozen=True, slots=True)
class PhysicalScan(PhysicalNode, EngineOp):
    engine: str = "spark"  # type: ignore[assignment]
    table: str = ""
    schema: Schema = field(default_factory=lambda: Schema(columns=()))
    kind: ClassVar[str] = "scan"

    @property
    def bytes_out(self) -> float:
        return self.schema.bytes_estimate()

    @property
    def cost(self) -> float:
        prof = ENGINE_PROFILES[self.engine]
        return prof.setup_cost + prof.cost_with_memory(self.bytes_out, prof.per_byte_scan)

    @property
    def delivered_properties(self) -> PhysicalProperties:
        return PhysicalProperties(engine=self.engine)

    def with_children(self, _: tuple[PhysicalNode, ...]) -> PhysicalScan:
        return self

    def _explain_self(self) -> str:
        return f"{self.engine}.Scan({self.table}, rows={self.schema.rows}, bytes≈{self.bytes_out:,.0f})"


# ---------------------------------------------------------------------------
# Filter
# ---------------------------------------------------------------------------


@dataclass(frozen=True, slots=True)
class PhysicalFilter(PhysicalNode, EngineOp):
    engine: str = "spark"  # type: ignore[assignment]
    predicate: Expr = field(default_factory=lambda: None)  # type: ignore[assignment]
    schema: Schema = field(default_factory=lambda: Schema(columns=()))
    _children: tuple[PhysicalNode, ...] = ()
    kind: ClassVar[str] = "filter"

    @property
    def children(self) -> tuple[PhysicalNode, ...]:
        return self._children

    @property
    def bytes_in(self) -> float:
        if not self._children:
            return self.schema.bytes_estimate()
        return self._children[0].schema.bytes_estimate()  # type: ignore[attr-defined]

    @property
    def bytes_out(self) -> float:
        sel = estimate_selectivity(self.predicate, self.schema)
        return self.bytes_in * sel

    @property
    def cost(self) -> float:
        prof = ENGINE_PROFILES[self.engine]
        return prof.setup_cost + prof.cost_with_memory(self.bytes_in, prof.per_byte_filter)

    @property
    def delivered_properties(self) -> PhysicalProperties:
        return PhysicalProperties(engine=self.engine)

    def with_children(self, children: tuple[PhysicalNode, ...]) -> PhysicalFilter:
        return replace(self, _children=children)

    def _explain_self(self) -> str:
        return f"{self.engine}.Filter({self.predicate}, bytes≈{self.bytes_out:,.0f})"


# ---------------------------------------------------------------------------
# Aggregate
# ---------------------------------------------------------------------------


@dataclass(frozen=True, slots=True)
class PhysicalAggregate(PhysicalNode, EngineOp):
    engine: str = "spark"  # type: ignore[assignment]
    group_by: tuple[ColumnRef, ...] = ()
    aggregates: tuple[AggFunc, ...] = ()
    child_schema: Schema = field(default_factory=lambda: Schema(columns=()))
    output_schema: Schema = field(default_factory=lambda: Schema(columns=()))
    _children: tuple[PhysicalNode, ...] = ()
    kind: ClassVar[str] = "aggregate"

    @property
    def schema(self) -> Schema:
        return self.output_schema

    @property
    def children(self) -> tuple[PhysicalNode, ...]:
        return self._children

    @property
    def bytes_in(self) -> float:
        return self.child_schema.bytes_estimate()

    @property
    def bytes_out(self) -> float:
        return self.output_schema.bytes_estimate()

    @property
    def cost(self) -> float:
        prof = ENGINE_PROFILES[self.engine]
        return prof.setup_cost + prof.cost_with_memory(self.bytes_in, prof.per_byte_agg)

    @property
    def delivered_properties(self) -> PhysicalProperties:
        return PhysicalProperties(engine=self.engine,
                                  partitioning=tuple(g.name for g in self.group_by))

    def with_children(self, children: tuple[PhysicalNode, ...]) -> PhysicalAggregate:
        return replace(self, _children=children)

    def _explain_self(self) -> str:
        gb = ",".join(g.name for g in self.group_by) or "()"
        aggs = ",".join(repr(a) for a in self.aggregates)
        return f"{self.engine}.HashAgg(group_by=[{gb}], aggs=[{aggs}], bytes≈{self.bytes_out:,.0f})"


# ---------------------------------------------------------------------------
# Hash Join
# ---------------------------------------------------------------------------


@dataclass(frozen=True, slots=True)
class PhysicalHashJoin(PhysicalNode, EngineOp):
    engine: str = "spark"  # type: ignore[assignment]
    on: Expr = field(default_factory=lambda: None)  # type: ignore[assignment]
    join_type: str = "INNER"
    left_schema: Schema = field(default_factory=lambda: Schema(columns=()))
    right_schema: Schema = field(default_factory=lambda: Schema(columns=()))
    output_schema: Schema = field(default_factory=lambda: Schema(columns=()))
    _children: tuple[PhysicalNode, ...] = ()
    kind: ClassVar[str] = "hashjoin"

    @property
    def schema(self) -> Schema:
        return self.output_schema

    @property
    def children(self) -> tuple[PhysicalNode, ...]:
        return self._children

    @property
    def bytes_in(self) -> float:
        return self.left_schema.bytes_estimate() + self.right_schema.bytes_estimate()

    @property
    def bytes_out(self) -> float:
        return self.output_schema.bytes_estimate()

    @property
    def cost(self) -> float:
        prof = ENGINE_PROFILES[self.engine]
        # Building hash table on smaller side; probe other side
        return prof.setup_cost + prof.cost_with_memory(self.bytes_in, prof.per_byte_join)

    @property
    def delivered_properties(self) -> PhysicalProperties:
        return PhysicalProperties(engine=self.engine)

    def with_children(self, children: tuple[PhysicalNode, ...]) -> PhysicalHashJoin:
        return replace(self, _children=children)

    def _explain_self(self) -> str:
        return f"{self.engine}.HashJoin({self.on}, bytes≈{self.bytes_out:,.0f})"


# ---------------------------------------------------------------------------
# Cross-engine conversion (Exchange)
# ---------------------------------------------------------------------------


@dataclass(frozen=True, slots=True)
class PhysicalConversion(PhysicalNode):
    src_engine: str
    dst_engine: str
    child: PhysicalNode = field(default_factory=lambda: None)  # type: ignore[assignment]
    bytes_in: float = 0.0
    kind: ClassVar[str] = "conversion"

    @property
    def engine(self) -> str:
        return self.dst_engine

    @property
    def schema(self) -> Schema:
        return self.child.schema  # type: ignore[attr-defined]

    @property
    def bytes_out(self) -> float:
        return self.bytes_in

    @property
    def children(self) -> tuple[PhysicalNode, ...]:
        return (self.child,)

    @property
    def cost(self) -> float:
        from ppc.engines.conversions import default_conversion_registry

        return default_conversion_registry().cost(self.src_engine, self.dst_engine, self.bytes_in)

    @property
    def delivered_properties(self) -> PhysicalProperties:
        return PhysicalProperties(engine=self.dst_engine)

    def _explain_self(self) -> str:
        return f"Convert({self.src_engine} → {self.dst_engine}, bytes≈{self.bytes_in:,.0f})"
