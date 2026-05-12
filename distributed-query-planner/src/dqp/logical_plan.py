"""Logical query plan nodes."""
from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import Any, List, Optional

from dqp.predicate import Predicate


class PlanNode(ABC):
    """Abstract base for all logical plan nodes."""

    @property
    @abstractmethod
    def children(self) -> List[PlanNode]:
        """Return direct child nodes."""
        ...


@dataclass
class ScanNode(PlanNode):
    """Full table scan — no predicate applied at engine level."""

    table_name: str
    engine_name: str
    columns: List[str]

    @property
    def children(self) -> List[PlanNode]:
        return []

    def __repr__(self) -> str:
        return (
            f"ScanNode(table={self.table_name!r}, engine={self.engine_name!r}, "
            f"columns={self.columns})"
        )


@dataclass
class FilterNode(PlanNode):
    """Applies a predicate above a child plan node."""

    child: PlanNode
    predicate: Predicate

    @property
    def children(self) -> List[PlanNode]:
        return [self.child]

    def __repr__(self) -> str:
        return f"FilterNode(predicate={self.predicate!r})"


@dataclass
class ProjectNode(PlanNode):
    """Projects a subset of columns."""

    child: PlanNode
    columns: List[str]

    @property
    def children(self) -> List[PlanNode]:
        return [self.child]

    def __repr__(self) -> str:
        return f"ProjectNode(columns={self.columns})"


@dataclass
class JoinNode(PlanNode):
    """Logical join of two plan nodes."""

    left: PlanNode
    right: PlanNode
    condition: Predicate
    join_type: str = "inner"  # inner, left, right, full

    @property
    def children(self) -> List[PlanNode]:
        return [self.left, self.right]

    def __repr__(self) -> str:
        return f"JoinNode(type={self.join_type!r}, condition={self.condition!r})"


@dataclass
class AggregateNode(PlanNode):
    """Aggregation with optional grouping."""

    child: PlanNode
    group_by: List[str]
    aggregates: List[Any]  # List of aggregate expressions (engine-specific)

    @property
    def children(self) -> List[PlanNode]:
        return [self.child]

    def __repr__(self) -> str:
        return f"AggregateNode(group_by={self.group_by}, aggs={self.aggregates})"


@dataclass
class PushedScanNode(PlanNode):
    """Scan where some predicates have been pushed into the engine.

    *pushed_predicates* are applied natively by the engine.
    *residual_predicates* must still be applied in Python after the scan.
    """

    table_name: str
    engine_name: str
    pushed_predicates: List[Predicate]
    residual_predicates: List[Predicate]
    columns: List[str]

    @property
    def children(self) -> List[PlanNode]:
        return []

    def __repr__(self) -> str:
        return (
            f"PushedScanNode(table={self.table_name!r}, engine={self.engine_name!r}, "
            f"pushed={len(self.pushed_predicates)}, residual={len(self.residual_predicates)}, "
            f"columns={self.columns})"
        )


# ---------------------------------------------------------------------------
# Plan pretty-printer
# ---------------------------------------------------------------------------


def plan_to_str(node: PlanNode, indent: int = 0) -> str:
    """Return a human-readable multi-line representation of a plan tree."""
    prefix = "  " * indent
    lines = [f"{prefix}{node!r}"]
    for child in node.children:
        lines.append(plan_to_str(child, indent + 1))
    return "\n".join(lines)
