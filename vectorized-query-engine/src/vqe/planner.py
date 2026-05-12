"""Logical plan → Physical operator tree (planner)."""
from __future__ import annotations

from typing import List

from .expressions import BinaryExpr, ColumnRef, Expr
from .logical_plan import (
    Aggregate,
    Filter,
    Join,
    Limit,
    LogicalPlan,
    Project,
    Scan,
    Sort,
)
from .physical_plan import (
    FilterOp,
    HashAggOp,
    HashJoinOp,
    LimitOp,
    PhysicalOp,
    ProjectOp,
    SequentialScan,
    SortOp,
)


class Planner:
    def plan(self, node: LogicalPlan) -> PhysicalOp:
        if isinstance(node, Scan):
            return SequentialScan(
                table_name=node.table,
                output_cols=node.columns,
                predicates=node.pushed_predicates,
            )

        if isinstance(node, Filter):
            child = self.plan(node.child)
            return FilterOp(child, node.predicate)

        if isinstance(node, Project):
            child = self.plan(node.child)
            return ProjectOp(child, node.exprs, node.aliases)

        if isinstance(node, Aggregate):
            child = self.plan(node.child)
            return HashAggOp(child, node.group_by, node.aggregates)

        if isinstance(node, Sort):
            child = self.plan(node.child)
            return SortOp(child, node.keys, node.ascending)

        if isinstance(node, Limit):
            child = self.plan(node.child)
            return LimitOp(child, node.n, node.offset)

        if isinstance(node, Join):
            left = self.plan(node.left)
            right = self.plan(node.right)
            left_keys, right_keys = _extract_join_keys(node.condition)
            return HashJoinOp(left, right, left_keys, right_keys, node.join_type)

        raise NotImplementedError(f"No physical plan for {type(node).__name__}")


def _extract_join_keys(cond: Expr) -> tuple[List[str], List[str]]:
    """
    Extract equi-join key pairs from a join condition.
    Supports: a.x = b.y  AND  a.z = b.w  (conjuncts of equalities).
    Returns (left_keys, right_keys).
    """
    from .expressions import split_conjuncts

    left_keys: List[str] = []
    right_keys: List[str] = []

    for c in split_conjuncts(cond):
        if isinstance(c, BinaryExpr) and c.op == "=":
            if isinstance(c.left, ColumnRef) and isinstance(c.right, ColumnRef):
                left_keys.append(c.left.name)
                right_keys.append(c.right.name)

    if not left_keys:
        raise ValueError(
            f"Only equi-join conditions (col = col) are supported, got: {cond!r}"
        )
    return left_keys, right_keys
