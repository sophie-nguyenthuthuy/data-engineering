"""Logical relational-algebra nodes.

Logical nodes describe *what* to compute, not *how*. They carry:
  - an output schema (derived from children + operator semantics)
  - a deterministic structural hash, for memoization keying

They are immutable.
"""

from __future__ import annotations

from dataclasses import dataclass
from functools import cached_property
from typing import ClassVar

from ppc.ir.expr import ColumnRef, Expr
from ppc.ir.schema import Column, Schema


class LogicalNode:
    """Base class. Subclasses are frozen dataclasses."""

    kind: ClassVar[str] = ""

    @property
    def schema(self) -> Schema:  # pragma: no cover - abstract
        raise NotImplementedError

    @property
    def children(self) -> tuple[LogicalNode, ...]:  # pragma: no cover
        raise NotImplementedError

    def replace_children(self, new_children: tuple[LogicalNode, ...]) -> LogicalNode:
        """Return a copy with replaced children."""
        raise NotImplementedError

    # Tree-as-string useful for repr / debugging
    def explain(self, indent: int = 0) -> str:
        prefix = "  " * indent
        body = self._explain_self()
        out = [f"{prefix}{body}"]
        for c in self.children:
            out.append(c.explain(indent + 1))
        return "\n".join(out)

    def _explain_self(self) -> str:  # pragma: no cover
        return self.__class__.__name__


# ---------------------------------------------------------------------------
# Leaves
# ---------------------------------------------------------------------------


@dataclass(frozen=True, slots=True, eq=True)
class LogicalScan(LogicalNode):
    """Read a base table."""

    kind: ClassVar[str] = "scan"
    table: str
    table_schema: Schema

    @property
    def schema(self) -> Schema:
        return self.table_schema

    @property
    def children(self) -> tuple[LogicalNode, ...]:
        return ()

    def replace_children(self, new_children: tuple[LogicalNode, ...]) -> LogicalNode:
        if new_children:
            raise ValueError("LogicalScan has no children")
        return self

    def _explain_self(self) -> str:
        return f"Scan(table={self.table!r}, rows={self.table_schema.rows})"


# ---------------------------------------------------------------------------
# Filter
# ---------------------------------------------------------------------------


@dataclass(frozen=True, slots=True, eq=True)
class LogicalFilter(LogicalNode):
    kind: ClassVar[str] = "filter"
    child: LogicalNode
    predicate: Expr

    @property
    def schema(self) -> Schema:
        # Filter doesn't change the schema (just reduces row count)
        return self.child.schema

    @property
    def children(self) -> tuple[LogicalNode, ...]:
        return (self.child,)

    def replace_children(self, new_children: tuple[LogicalNode, ...]) -> LogicalNode:
        (c,) = new_children
        return LogicalFilter(child=c, predicate=self.predicate)

    def _explain_self(self) -> str:
        return f"Filter(predicate={self.predicate})"


# ---------------------------------------------------------------------------
# Aggregate
# ---------------------------------------------------------------------------


@dataclass(frozen=True, slots=True, eq=True)
class AggFunc:
    """A single aggregate function call: COUNT, SUM, AVG, MIN, MAX."""

    func: str               # "COUNT" | "SUM" | "AVG" | "MIN" | "MAX"
    arg: Expr | None        # None for COUNT(*)
    alias: str              # output column name

    def __repr__(self) -> str:
        a = "*" if self.arg is None else repr(self.arg)
        return f"{self.func}({a}) AS {self.alias}"


@dataclass(frozen=True, slots=True, eq=True)
class LogicalAggregate(LogicalNode):
    kind: ClassVar[str] = "aggregate"
    child: LogicalNode
    group_by: tuple[ColumnRef, ...]
    aggregates: tuple[AggFunc, ...]

    @cached_property
    def schema(self) -> Schema:  # type: ignore[override]
        cols: list[Column] = []
        for g in self.group_by:
            cols.append(self.child.schema[g.name])
        for a in self.aggregates:
            # COUNT → INT64; SUM/MIN/MAX → arg's dtype; AVG → DOUBLE
            from ppc.ir.types import DOUBLE, INT64

            if a.func == "COUNT":
                cols.append(Column(name=a.alias, dtype=INT64))
            elif a.func == "AVG":
                cols.append(Column(name=a.alias, dtype=DOUBLE))
            else:
                if a.arg is None:
                    raise ValueError(f"{a.func} requires an argument")
                cols.append(Column(name=a.alias, dtype=a.arg.dtype))
        rows: int | None = None
        # Estimate: number of groups
        if self.group_by:
            ndvs = []
            for g in self.group_by:
                stats = self.child.schema[g.name].stats
                if stats.ndv is not None:
                    ndvs.append(stats.ndv)
            if ndvs:
                # cap at child row count
                child_rows = self.child.schema.rows
                est = 1
                for x in ndvs:
                    est *= x
                if child_rows is not None:
                    est = min(est, child_rows)
                rows = est
        else:
            rows = 1
        return Schema(columns=tuple(cols), rows=rows)

    @property
    def children(self) -> tuple[LogicalNode, ...]:
        return (self.child,)

    def replace_children(self, new_children: tuple[LogicalNode, ...]) -> LogicalNode:
        (c,) = new_children
        return LogicalAggregate(child=c, group_by=self.group_by, aggregates=self.aggregates)

    def _explain_self(self) -> str:
        gb = ",".join(g.name for g in self.group_by) or "()"
        aggs = ",".join(repr(a) for a in self.aggregates)
        return f"Aggregate(group_by=[{gb}], aggs=[{aggs}])"


# ---------------------------------------------------------------------------
# Join
# ---------------------------------------------------------------------------


@dataclass(frozen=True, slots=True, eq=True)
class LogicalJoin(LogicalNode):
    kind: ClassVar[str] = "join"
    left: LogicalNode
    right: LogicalNode
    on: Expr                # join condition; INNER for now
    join_type: str = "INNER"

    @cached_property
    def schema(self) -> Schema:  # type: ignore[override]
        merged = self.left.schema.union(self.right.schema)
        # Row estimate: cross product * predicate selectivity (default 1 / max NDV)
        left_rows = self.left.schema.rows
        right_rows = self.right.schema.rows
        if left_rows is None or right_rows is None:
            return merged
        # Pick the larger of the join-key NDVs as denominator (the standard
        # "1/max(NDV_L, NDV_R)" estimator).
        from ppc.ir.expr import BinaryOp, ColumnRef as CR

        denom = 1
        if isinstance(self.on, BinaryOp) and self.on.op == "=":
            if isinstance(self.on.left, CR) and isinstance(self.on.right, CR):
                # Columns may live on either side (commutativity may have run).
                ndvs: list[int] = []
                for col in (self.on.left, self.on.right):
                    for side in (self.left.schema, self.right.schema):
                        try:
                            s = side[col.name].stats
                        except KeyError:
                            continue
                        if s.ndv is not None:
                            ndvs.append(s.ndv)
                        break
                if ndvs:
                    denom = max(ndvs)
        est = (left_rows * right_rows) // max(denom, 1)
        return Schema(columns=merged.columns, rows=est)

    @property
    def children(self) -> tuple[LogicalNode, ...]:
        return (self.left, self.right)

    def replace_children(self, new_children: tuple[LogicalNode, ...]) -> LogicalNode:
        l, r = new_children
        return LogicalJoin(left=l, right=r, on=self.on, join_type=self.join_type)

    def _explain_self(self) -> str:
        return f"Join(type={self.join_type}, on={self.on})"
