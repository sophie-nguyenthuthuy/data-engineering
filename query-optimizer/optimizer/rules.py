"""
Transformation and implementation rules for the Cascades optimizer.

Transformation rules  (logical → logical):
  - CommuteJoin    : A ⋈ B  →  B ⋈ A
  - AssociateLeft  : (A ⋈ B) ⋈ C  →  A ⋈ (B ⋈ C)
  - AssociateRight : A ⋈ (B ⋈ C)  →  (A ⋈ B) ⋈ C

Implementation rules  (logical → physical):
  - LogicalJoin → HashJoin
  - LogicalJoin → MergeJoin
  - LogicalJoin → NestedLoopJoin
  - Scan         → SeqScan

Each rule's `apply` method returns a list of new (expr, tables) pairs to
insert into the memo, or an empty list if the rule does not match.
"""
from __future__ import annotations
from typing import TYPE_CHECKING, List, Optional, Tuple

from optimizer.expressions import (
    LogicalExpr, PhysicalExpr,
    Scan, LogicalJoin, PhysicalScan, PhysicalJoin, PhysicalOp, Predicate,
)

if TYPE_CHECKING:
    from optimizer.memo import Memo, Group


# ---------------------------------------------------------------------------
# Base class
# ---------------------------------------------------------------------------

class Rule:
    name: str = "Rule"

    def matches(self, expr: LogicalExpr, memo: "Memo") -> bool:
        raise NotImplementedError

    def apply(self, expr: LogicalExpr, group: "Group", memo: "Memo") -> List:
        """Return list of (new_expr, tables_frozenset) tuples."""
        raise NotImplementedError


# ---------------------------------------------------------------------------
# Transformation rules
# ---------------------------------------------------------------------------

class CommuteJoin(Rule):
    """A ⋈ B  →  B ⋈ A"""
    name = "CommuteJoin"

    def matches(self, expr, memo):
        return isinstance(expr, LogicalJoin)

    def apply(self, expr: LogicalJoin, group, memo):
        flipped_preds = tuple(p.flipped() for p in expr.predicates)
        new_expr = LogicalJoin(
            left_group=expr.right_group,
            right_group=expr.left_group,
            predicates=flipped_preds,
            join_type=expr.join_type,
        )
        left_g = memo.get_group(expr.left_group)
        right_g = memo.get_group(expr.right_group)
        new_tables = right_g.tables | left_g.tables
        return [(new_expr, new_tables)]


class AssociateLeft(Rule):
    """(A ⋈ B) ⋈ C  →  A ⋈ (B ⋈ C)"""
    name = "AssociateLeft"

    def matches(self, expr, memo):
        if not isinstance(expr, LogicalJoin):
            return False
        left_g = memo.get_group(expr.left_group)
        return any(isinstance(e, LogicalJoin) for e in left_g.logical_exprs)

    def apply(self, expr: LogicalJoin, group, memo):
        left_g = memo.get_group(expr.left_group)
        right_c = expr.right_group  # C
        right_g_c = memo.get_group(right_c)

        results = []
        for inner in left_g.logical_exprs:
            if not isinstance(inner, LogicalJoin):
                continue
            # inner is A ⋈ B
            a_gid = inner.left_group
            b_gid = inner.right_group
            a_g = memo.get_group(a_gid)
            b_g = memo.get_group(b_gid)

            # Build new B ⋈ C group
            bc_tables = b_g.tables | right_g_c.tables
            bc_preds = tuple(
                p for p in (expr.predicates + inner.predicates)
                if p.left_table in bc_tables and p.right_table in bc_tables
            )
            bc_expr = LogicalJoin(b_gid, right_c, bc_preds)

            # A ⋈ (B ⋈ C)
            ac_tables = a_g.tables | bc_tables
            ac_preds = tuple(
                p for p in (expr.predicates + inner.predicates)
                if p.left_table in ac_tables and p.right_table in ac_tables
                and p not in bc_preds
            )
            results.append(("two_level", bc_expr, bc_tables, ac_preds, a_gid, ac_tables))

        return results  # handled specially by optimizer


class AssociateRight(Rule):
    """A ⋈ (B ⋈ C)  →  (A ⋈ B) ⋈ C"""
    name = "AssociateRight"

    def matches(self, expr, memo):
        if not isinstance(expr, LogicalJoin):
            return False
        right_g = memo.get_group(expr.right_group)
        return any(isinstance(e, LogicalJoin) for e in right_g.logical_exprs)

    def apply(self, expr: LogicalJoin, group, memo):
        left_a = expr.left_group
        right_g = memo.get_group(expr.right_group)
        a_g = memo.get_group(left_a)

        results = []
        for inner in right_g.logical_exprs:
            if not isinstance(inner, LogicalJoin):
                continue
            b_gid = inner.left_group
            c_gid = inner.right_group
            b_g = memo.get_group(b_gid)
            c_g = memo.get_group(c_gid)

            # (A ⋈ B)
            ab_tables = a_g.tables | b_g.tables
            ab_preds = tuple(
                p for p in (expr.predicates + inner.predicates)
                if p.left_table in ab_tables and p.right_table in ab_tables
            )
            ab_expr = LogicalJoin(left_a, b_gid, ab_preds)

            # (A ⋈ B) ⋈ C
            abc_tables = ab_tables | c_g.tables
            abc_preds = tuple(
                p for p in (expr.predicates + inner.predicates)
                if p not in ab_preds
                and p.left_table in abc_tables and p.right_table in abc_tables
            )
            results.append(("two_level", ab_expr, ab_tables, abc_preds, c_gid, abc_tables))

        return results


# ---------------------------------------------------------------------------
# Implementation rules
# ---------------------------------------------------------------------------

class ImplementScan(Rule):
    name = "ImplementScan"

    def matches(self, expr, memo):
        return isinstance(expr, Scan)

    def apply(self, expr: Scan, group, memo):
        return [PhysicalScan(expr.table)]


class ImplementHashJoin(Rule):
    name = "ImplementHashJoin"

    def matches(self, expr, memo):
        return isinstance(expr, LogicalJoin)

    def apply(self, expr: LogicalJoin, group, memo):
        return [PhysicalJoin(expr.left_group, expr.right_group,
                             PhysicalOp.HASH_JOIN, expr.predicates)]


class ImplementMergeJoin(Rule):
    name = "ImplementMergeJoin"

    def matches(self, expr, memo):
        # Merge join requires at least one equi-join predicate
        return isinstance(expr, LogicalJoin) and len(expr.predicates) > 0

    def apply(self, expr: LogicalJoin, group, memo):
        return [PhysicalJoin(expr.left_group, expr.right_group,
                             PhysicalOp.MERGE_JOIN, expr.predicates)]


class ImplementNestedLoop(Rule):
    name = "ImplementNestedLoop"

    def matches(self, expr, memo):
        return isinstance(expr, LogicalJoin)

    def apply(self, expr: LogicalJoin, group, memo):
        return [PhysicalJoin(expr.left_group, expr.right_group,
                             PhysicalOp.NESTED_LOOP, expr.predicates)]


# ---------------------------------------------------------------------------
# Rule sets
# ---------------------------------------------------------------------------

TRANSFORMATION_RULES: List[Rule] = [
    CommuteJoin(),
    AssociateLeft(),
    AssociateRight(),
]

IMPLEMENTATION_RULES: List[Rule] = [
    ImplementScan(),
    ImplementHashJoin(),
    ImplementMergeJoin(),
    ImplementNestedLoop(),
]
