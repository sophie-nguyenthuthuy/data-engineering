"""Query plan node data structures."""
from __future__ import annotations
from dataclasses import dataclass, field
from typing import Optional
import math


OPERATOR_TYPES = [
    "Seq Scan", "Index Scan", "Index Only Scan", "Bitmap Heap Scan",
    "Bitmap Index Scan", "Hash Join", "Merge Join", "Nested Loop",
    "Hash", "Sort", "Aggregate", "Group", "Gather", "Gather Merge",
    "Materialize", "Memoize", "Subquery Scan", "Result", "Limit",
    "Unique", "SetOp", "Append", "MergeAppend", "Unknown",
]
OPERATOR_INDEX = {op: i for i, op in enumerate(OPERATOR_TYPES)}

JOIN_TYPES = ["Inner", "Left", "Right", "Full", "Semi", "Anti", "Unknown"]
SCAN_DIRECTIONS = ["Forward", "Backward", "NoMovement", "Unknown"]


@dataclass
class PlanNode:
    node_type: str
    estimated_rows: float
    estimated_width: int
    estimated_cost_startup: float
    estimated_cost_total: float

    # Populated after EXPLAIN ANALYZE
    actual_rows: Optional[float] = None
    actual_loops: int = 1

    # Relational context
    relation_name: Optional[str] = None
    alias: Optional[str] = None
    index_name: Optional[str] = None
    join_type: Optional[str] = None
    hash_cond: Optional[str] = None
    merge_cond: Optional[str] = None
    join_filter: Optional[str] = None
    filter: Optional[str] = None
    output: list[str] = field(default_factory=list)

    children: list[PlanNode] = field(default_factory=list)

    # Set during tree traversal
    node_id: int = -1
    depth: int = 0

    @property
    def actual_rows_total(self) -> Optional[float]:
        if self.actual_rows is None:
            return None
        return self.actual_rows * self.actual_loops

    @property
    def q_error(self) -> Optional[float]:
        """Max(est/act, act/est) — q-error, 1.0 = perfect."""
        if self.actual_rows is None or self.actual_rows_total is None:
            return None
        act = max(self.actual_rows_total, 1.0)
        est = max(self.estimated_rows, 1.0)
        return max(est / act, act / est)

    @property
    def is_over_estimated(self) -> bool:
        if self.actual_rows_total is None:
            return False
        return self.estimated_rows > self.actual_rows_total * 100

    @property
    def is_under_estimated(self) -> bool:
        if self.actual_rows_total is None:
            return False
        return self.actual_rows_total > self.estimated_rows * 100

    @property
    def cardinality_error_ratio(self) -> Optional[float]:
        if self.actual_rows_total is None:
            return None
        act = max(self.actual_rows_total, 1.0)
        est = max(self.estimated_rows, 1.0)
        return est / act  # > 1 means over-estimated, < 1 means under-estimated

    def operator_one_hot(self) -> list[int]:
        vec = [0] * len(OPERATOR_TYPES)
        idx = OPERATOR_INDEX.get(self.node_type, OPERATOR_INDEX["Unknown"])
        vec[idx] = 1
        return vec

    def log_estimated_rows(self) -> float:
        return math.log(max(self.estimated_rows, 1.0))

    def log_estimated_cost(self) -> float:
        return math.log(max(self.estimated_cost_total, 1.0))

    def __repr__(self) -> str:
        act = f", actual={self.actual_rows_total:.0f}" if self.actual_rows_total is not None else ""
        qe = f", q-err={self.q_error:.1f}x" if self.q_error is not None else ""
        return (
            f"PlanNode({self.node_type}, est={self.estimated_rows:.0f}"
            f"{act}{qe}, depth={self.depth})"
        )

    def all_nodes(self) -> list[PlanNode]:
        nodes = [self]
        for child in self.children:
            nodes.extend(child.all_nodes())
        return nodes
