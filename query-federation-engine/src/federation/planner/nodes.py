"""Logical query plan nodes."""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum
from typing import Any


class JoinType(str, Enum):
    INNER = "INNER"
    LEFT = "LEFT"
    RIGHT = "RIGHT"
    FULL = "FULL"
    CROSS = "CROSS"


@dataclass
class PlanNode:
    """Base class for all plan nodes."""
    estimated_rows: int = 0
    estimated_cost: float = 0.0

    def children(self) -> list["PlanNode"]:
        return []


@dataclass
class TableScan(PlanNode):
    """Reads rows from a single data source table."""
    source: str = ""          # e.g. "postgres"
    table: str = ""           # e.g. "orders"
    alias: str = ""
    projected_columns: list[str] = field(default_factory=list)
    pushed_predicates: list[Any] = field(default_factory=list)   # sqlglot expressions

    @property
    def qualified_name(self) -> str:
        return f"{self.source}.{self.table}"


@dataclass
class Filter(PlanNode):
    """Applies a predicate that couldn't be pushed down."""
    child: PlanNode = field(default_factory=lambda: PlanNode())
    predicate: Any = None  # sqlglot expression

    def children(self) -> list[PlanNode]:
        return [self.child]


@dataclass
class Project(PlanNode):
    """Selects a subset of columns."""
    child: PlanNode = field(default_factory=lambda: PlanNode())
    columns: list[Any] = field(default_factory=list)  # sqlglot expressions
    output_names: list[str] = field(default_factory=list)

    def children(self) -> list[PlanNode]:
        return [self.child]


@dataclass
class Join(PlanNode):
    """Joins two relations in the federation engine."""
    left: PlanNode = field(default_factory=lambda: PlanNode())
    right: PlanNode = field(default_factory=lambda: PlanNode())
    join_type: JoinType = JoinType.INNER
    condition: Any = None       # sqlglot expression
    left_keys: list[str] = field(default_factory=list)
    right_keys: list[str] = field(default_factory=list)

    def children(self) -> list[PlanNode]:
        return [self.left, self.right]


@dataclass
class Aggregate(PlanNode):
    """GROUP BY + aggregation functions."""
    child: PlanNode = field(default_factory=lambda: PlanNode())
    group_keys: list[Any] = field(default_factory=list)
    aggregates: list[Any] = field(default_factory=list)

    def children(self) -> list[PlanNode]:
        return [self.child]


@dataclass
class Sort(PlanNode):
    """ORDER BY."""
    child: PlanNode = field(default_factory=lambda: PlanNode())
    order_exprs: list[Any] = field(default_factory=list)

    def children(self) -> list[PlanNode]:
        return [self.child]


@dataclass
class Limit(PlanNode):
    """LIMIT / OFFSET."""
    child: PlanNode = field(default_factory=lambda: PlanNode())
    count: int = 0
    offset: int = 0

    def children(self) -> list[PlanNode]:
        return [self.child]


def explain_plan(node: PlanNode, indent: int = 0) -> str:
    """Return a human-readable plan tree."""
    prefix = "  " * indent
    lines: list[str] = []

    match node:
        case TableScan():
            preds = len(node.pushed_predicates)
            cols = ", ".join(node.projected_columns) or "*"
            lines.append(
                f"{prefix}TableScan [{node.qualified_name}"
                f"{' AS ' + node.alias if node.alias else ''}]"
                f"  cols=[{cols}]  pushed_predicates={preds}"
                f"  est_rows={node.estimated_rows:,}"
            )
        case Filter():
            lines.append(f"{prefix}Filter  predicate={node.predicate}  est_rows={node.estimated_rows:,}")
        case Project():
            names = ", ".join(node.output_names) or "…"
            lines.append(f"{prefix}Project  [{names}]")
        case Join():
            lines.append(
                f"{prefix}Join [{node.join_type.value}]"
                f"  on={node.left_keys}={node.right_keys}"
                f"  est_rows={node.estimated_rows:,}  cost={node.estimated_cost:.1f}"
            )
        case Aggregate():
            lines.append(f"{prefix}Aggregate  group_by={[str(k) for k in node.group_keys]}")
        case Sort():
            lines.append(f"{prefix}Sort")
        case Limit():
            lines.append(f"{prefix}Limit  count={node.count}  offset={node.offset}")
        case _:
            lines.append(f"{prefix}{type(node).__name__}")

    for child in node.children():
        lines.append(explain_plan(child, indent + 1))

    return "\n".join(lines)
