"""Query optimizer: predicate pushdown, projection pushdown, constant folding."""
from __future__ import annotations

from typing import List, Optional, Set

from .expressions import (
    AggExpr,
    BinaryExpr,
    ColumnRef,
    Expr,
    Literal,
    conjuncts_to_expr,
    split_conjuncts,
)
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


# ---------------------------------------------------------------------------
# Rule: constant folding (evaluate constant binary expressions at plan time)
# ---------------------------------------------------------------------------

def fold_constants(expr: Expr) -> Expr:
    if isinstance(expr, BinaryExpr):
        left = fold_constants(expr.left)
        right = fold_constants(expr.right)
        if isinstance(left, Literal) and isinstance(right, Literal):
            lv, rv = left.value, right.value
            op = expr.op
            try:
                if op == "+":
                    return Literal(lv + rv)
                if op == "-":
                    return Literal(lv - rv)
                if op == "*":
                    return Literal(lv * rv)
                if op == "/" and rv != 0:
                    return Literal(lv / rv)
                if op == "=":
                    return Literal(lv == rv)
                if op == "!=":
                    return Literal(lv != rv)
                if op == "<":
                    return Literal(lv < rv)
                if op == "<=":
                    return Literal(lv <= rv)
                if op == ">":
                    return Literal(lv > rv)
                if op == ">=":
                    return Literal(lv >= rv)
                if op == "AND":
                    return Literal(bool(lv) and bool(rv))
                if op == "OR":
                    return Literal(bool(lv) or bool(rv))
            except Exception:
                pass
        return BinaryExpr(expr.op, left, right)
    return expr


# ---------------------------------------------------------------------------
# Rule: predicate pushdown
# Push Filter nodes as far toward the leaves as possible.
# ---------------------------------------------------------------------------

def _pushdown_filter(plan: LogicalPlan, predicates: List[Expr]) -> LogicalPlan:
    if isinstance(plan, Scan):
        plan.pushed_predicates.extend(predicates)
        return plan

    if isinstance(plan, Filter):
        # Combine and recurse
        all_preds = split_conjuncts(plan.predicate) + predicates
        new_child = _pushdown_filter(plan.child, all_preds)
        return new_child

    if isinstance(plan, Project):
        # Can push predicates that only reference columns available below the projection
        below_cols = set(plan.child.schema_names()) if plan.child.schema_names() else None
        pushable = []
        remaining = []
        for p in predicates:
            used = p.columns_used()
            # Check if all used cols are ColumnRefs that are available below
            if below_cols is None or used.issubset(below_cols):
                pushable.append(p)
            else:
                remaining.append(p)
        new_child = _pushdown_filter(plan.child, pushable)
        plan.child = new_child
        if remaining:
            merged = conjuncts_to_expr(remaining)
            return Filter(plan, merged)
        return plan

    if isinstance(plan, Aggregate):
        # Cannot push predicates through aggregate (they reference post-agg columns)
        # But can push predicates on group-by keys through to child
        key_cols = {repr(e) for e in plan.group_by if isinstance(e, ColumnRef)}
        pushable = []
        remaining = []
        for p in predicates:
            used = p.columns_used()
            if used.issubset(key_cols):
                pushable.append(p)
            else:
                remaining.append(p)
        if pushable:
            plan.child = _pushdown_filter(plan.child, pushable)
        if remaining:
            merged = conjuncts_to_expr(remaining)
            return Filter(plan, merged)
        return plan

    if isinstance(plan, Join):
        left_cols = set(plan.left.schema_names()) if plan.left.schema_names() else None
        right_cols = set(plan.right.schema_names()) if plan.right.schema_names() else None
        push_left = []
        push_right = []
        remaining = []
        for p in predicates:
            used = p.columns_used()
            if left_cols is not None and used.issubset(left_cols):
                push_left.append(p)
            elif right_cols is not None and used.issubset(right_cols):
                push_right.append(p)
            else:
                remaining.append(p)
        plan.left = _pushdown_filter(plan.left, push_left)
        plan.right = _pushdown_filter(plan.right, push_right)
        if remaining:
            merged = conjuncts_to_expr(remaining)
            return Filter(plan, merged)
        return plan

    if isinstance(plan, (Sort, Limit)):
        plan.child = _pushdown_filter(plan.child, predicates)
        return plan

    # Default: cannot push, wrap in Filter
    if predicates:
        merged = conjuncts_to_expr(predicates)
        return Filter(plan, merged)
    return plan


# ---------------------------------------------------------------------------
# Rule: projection pushdown
# Track which columns are actually needed and prune Scan columns.
# ---------------------------------------------------------------------------

def _collect_needed_cols(plan: LogicalPlan, needed: Optional[Set[str]] = None) -> LogicalPlan:
    """Bottom-up pass; annotates Scan nodes with the minimal column set."""
    if isinstance(plan, Scan):
        if needed is not None:
            # Keep only needed columns; if empty, keep all (e.g. COUNT(*))
            if needed:
                # Intersect with actual schema (caller can't know what the table has)
                plan.columns = sorted(needed)
        return plan

    if isinstance(plan, Filter):
        pred_cols = plan.predicate.columns_used()
        child_needed = (needed | pred_cols) if needed is not None else None
        plan.child = _collect_needed_cols(plan.child, child_needed)
        return plan

    if isinstance(plan, Project):
        expr_cols: Set[str] = set()
        for e in plan.exprs:
            expr_cols |= e.columns_used()
        child_needed = expr_cols  # project defines what it needs
        plan.child = _collect_needed_cols(plan.child, child_needed)
        return plan

    if isinstance(plan, Aggregate):
        agg_cols: Set[str] = set()
        for e in plan.group_by:
            agg_cols |= e.columns_used()
        for a in plan.aggregates:
            agg_cols |= a.columns_used()
        plan.child = _collect_needed_cols(plan.child, agg_cols)
        return plan

    if isinstance(plan, Sort):
        sort_cols: Set[str] = set()
        for k in plan.keys:
            sort_cols |= k.columns_used()
        child_needed = (needed | sort_cols) if needed is not None else sort_cols
        plan.child = _collect_needed_cols(plan.child, child_needed)
        return plan

    if isinstance(plan, Limit):
        plan.child = _collect_needed_cols(plan.child, needed)
        return plan

    if isinstance(plan, Join):
        join_cols = plan.condition.columns_used()
        child_needed = (needed | join_cols) if needed is not None else join_cols
        # Rough split — each side gets all needed (can be refined)
        plan.left = _collect_needed_cols(plan.left, child_needed)
        plan.right = _collect_needed_cols(plan.right, child_needed)
        return plan

    return plan


# ---------------------------------------------------------------------------
# Top-level optimizer
# ---------------------------------------------------------------------------

class Optimizer:
    def optimize(self, plan: LogicalPlan) -> LogicalPlan:
        # 1. Constant folding on all predicates
        plan = _fold_plan_constants(plan)
        # 2. Predicate pushdown
        plan = _pushdown_filter(plan, [])
        # 3. Projection pushdown
        plan = _collect_needed_cols(plan)
        return plan


def _fold_plan_constants(plan: LogicalPlan) -> LogicalPlan:
    if isinstance(plan, Filter):
        plan.predicate = fold_constants(plan.predicate)
        plan.child = _fold_plan_constants(plan.child)
    elif isinstance(plan, Project):
        plan.exprs = [fold_constants(e) for e in plan.exprs]
        plan.child = _fold_plan_constants(plan.child)
    elif isinstance(plan, Aggregate):
        plan.group_by = [fold_constants(e) for e in plan.group_by]
        plan.child = _fold_plan_constants(plan.child)
    elif isinstance(plan, Sort):
        plan.keys = [fold_constants(k) for k in plan.keys]
        plan.child = _fold_plan_constants(plan.child)
    elif isinstance(plan, Join):
        plan.condition = fold_constants(plan.condition)
        plan.left = _fold_plan_constants(plan.left)
        plan.right = _fold_plan_constants(plan.right)
    elif isinstance(plan, (Limit,)):
        plan.child = _fold_plan_constants(plan.child)
    return plan
