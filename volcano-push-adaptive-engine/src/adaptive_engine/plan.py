"""Logical query plan node hierarchy."""
from __future__ import annotations
from dataclasses import dataclass, field
from typing import Optional
from .expressions import Expr


@dataclass
class PlanNode:
    """Base class for all query plan nodes."""
    estimated_rows: int = 0
    node_id: str = ""  # filled by optimizer

    def children(self) -> list["PlanNode"]:
        return []

    def __repr__(self) -> str:
        return f"{type(self).__name__}(est={self.estimated_rows})"


# ------------------------------------------------------------------
# Leaf nodes
# ------------------------------------------------------------------

@dataclass
class ScanNode(PlanNode):
    table: str = ""

    def __repr__(self) -> str:
        return f"Scan({self.table}, est={self.estimated_rows})"


# ------------------------------------------------------------------
# Unary nodes
# ------------------------------------------------------------------

@dataclass
class FilterNode(PlanNode):
    child: Optional[PlanNode] = None
    predicate: Optional[Expr] = None
    selectivity: float = 0.5

    def children(self) -> list[PlanNode]:
        return [self.child] if self.child else []

    def __repr__(self) -> str:
        return f"Filter({self.predicate}, sel={self.selectivity:.2f}, est={self.estimated_rows})"


@dataclass
class ProjectNode(PlanNode):
    child: Optional[PlanNode] = None
    columns: list[str] = field(default_factory=list)

    def children(self) -> list[PlanNode]:
        return [self.child] if self.child else []

    def __repr__(self) -> str:
        return f"Project({self.columns}, est={self.estimated_rows})"


@dataclass
class AggregateNode(PlanNode):
    child: Optional[PlanNode] = None
    group_by: list[str] = field(default_factory=list)
    # (output_col, func, input_col) — func in {count, sum, avg, min, max}
    aggregates: list[tuple[str, str, str]] = field(default_factory=list)

    def children(self) -> list[PlanNode]:
        return [self.child] if self.child else []

    def __repr__(self) -> str:
        aggs = ", ".join(f"{f}({c})->{o}" for o, f, c in self.aggregates)
        return f"Aggregate(by={self.group_by}, {aggs}, est={self.estimated_rows})"


@dataclass
class SortNode(PlanNode):
    child: Optional[PlanNode] = None
    # (column, ascending)
    order_by: list[tuple[str, bool]] = field(default_factory=list)

    def children(self) -> list[PlanNode]:
        return [self.child] if self.child else []

    def __repr__(self) -> str:
        keys = ", ".join(f"{c} {'ASC' if a else 'DESC'}" for c, a in self.order_by)
        return f"Sort({keys}, est={self.estimated_rows})"


@dataclass
class LimitNode(PlanNode):
    child: Optional[PlanNode] = None
    limit: int = 0
    offset: int = 0

    def children(self) -> list[PlanNode]:
        return [self.child] if self.child else []

    def __repr__(self) -> str:
        return f"Limit({self.limit}, offset={self.offset})"


# ------------------------------------------------------------------
# Binary / join nodes
# ------------------------------------------------------------------

@dataclass
class HashJoinNode(PlanNode):
    """Build hash table from right child, probe with left child."""
    left: Optional[PlanNode] = None   # probe side
    right: Optional[PlanNode] = None  # build side
    left_key: str = ""
    right_key: str = ""
    join_type: str = "inner"  # inner | left | right | full

    def children(self) -> list[PlanNode]:
        return [c for c in (self.left, self.right) if c]

    def __repr__(self) -> str:
        return (
            f"HashJoin({self.join_type.upper()}, "
            f"{self.left_key}={self.right_key}, est={self.estimated_rows})"
        )


@dataclass
class NestedLoopJoinNode(PlanNode):
    """Fallback O(n²) join for small inputs or inequality predicates."""
    left: Optional[PlanNode] = None
    right: Optional[PlanNode] = None
    predicate: Optional[Expr] = None

    def children(self) -> list[PlanNode]:
        return [c for c in (self.left, self.right) if c]

    def __repr__(self) -> str:
        return f"NLJoin({self.predicate}, est={self.estimated_rows})"


# ------------------------------------------------------------------
# Internal: materialized buffer node (used by adaptive engine)
# ------------------------------------------------------------------

@dataclass
class BufferNode(PlanNode):
    """Wraps pre-materialized rows; inserted by adaptive engine when a
    subtree is replaced by push-based execution."""
    rows: list[dict] = field(default_factory=list)
    source_repr: str = ""

    def children(self) -> list[PlanNode]:
        return []

    def __repr__(self) -> str:
        return f"Buffer({len(self.rows)} rows from {self.source_repr})"


# ------------------------------------------------------------------
# Visitor helpers
# ------------------------------------------------------------------

def walk(node: PlanNode):
    """Pre-order traversal of the plan tree."""
    yield node
    for child in node.children():
        yield from walk(child)


def plan_repr(node: PlanNode, indent: int = 0) -> str:
    lines = [" " * indent + repr(node)]
    for child in node.children():
        lines.append(plan_repr(child, indent + 2))
    return "\n".join(lines)
