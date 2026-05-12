"""SQL-to-plan compiler.

Converts a SqlQuery AST (from the parser) into a physical plan tree
(PlanNode hierarchy) using the catalog for table metadata and selectivity
estimation.

Algorithm
---------
1. Start with ScanNode for the FROM table.
2. Apply each JOIN as a HashJoinNode (probe = current plan, build = joined table).
3. Wrap in a FilterNode if there is a WHERE clause.
4. Wrap in AggregateNode if GROUP BY or aggregate functions in SELECT.
5. Wrap in a FilterNode for HAVING (post-aggregate filter).
6. Wrap in SortNode for ORDER BY.
7. Wrap in LimitNode for LIMIT/OFFSET.
8. Wrap in ProjectNode for the SELECT column list (outermost).
"""
from __future__ import annotations
from typing import Any

from ..catalog import Catalog
from ..expressions import (
    AndExpr, BinOp, ColRef, Expr, Literal, NotExpr, OrExpr,
)
from ..plan import (
    AggregateNode, FilterNode, HashJoinNode, LimitNode,
    PlanNode, ProjectNode, ScanNode, SortNode,
)
from .parser import (
    ParseError, Parser, SqlAgg, SqlBinOp, SqlColRef, SqlExpr,
    SqlIsNull, SqlLiteral, SqlQuery, SqlUnaryOp,
)


class PlanError(Exception):
    pass


class Planner:
    """Compiles a SQL string or SqlQuery into a PlanNode tree."""

    def __init__(self, catalog: Catalog) -> None:
        self.catalog = catalog

    # ------------------------------------------------------------------
    # Entry points
    # ------------------------------------------------------------------

    def plan_sql(self, sql: str) -> PlanNode:
        query = Parser.parse(sql)
        return self.plan(query)

    def plan(self, q: SqlQuery) -> PlanNode:
        # Build an alias map: alias → real table name
        alias_map: dict[str, str] = {}
        if q.from_alias:
            alias_map[q.from_alias] = q.from_table
        for j in q.joins:
            if j.alias:
                alias_map[j.alias] = j.table

        node: PlanNode = ScanNode(table=q.from_table)

        # --- JOINs ---
        for join in q.joins:
            on_expr = join.condition
            left_key, right_key = _extract_equi_keys(on_expr, alias_map)
            node = HashJoinNode(
                left=node,
                right=ScanNode(table=join.table),
                left_key=left_key,
                right_key=right_key,
                join_type=join.join_type,
            )

        # --- WHERE ---
        if q.where is not None:
            pred = _to_expr(q.where)
            sel = _estimate_selectivity(q.where, q.from_table, self.catalog)
            node = FilterNode(child=node, predicate=pred, selectivity=sel)

        # --- GROUP BY + aggregates ---
        agg_funcs = _collect_aggs(q.select)
        if q.group_by or agg_funcs:
            group_cols = [_col_name(g) for g in q.group_by]
            agg_specs: list[tuple[str, str, str]] = []
            for item in q.select:
                if isinstance(item.expr, SqlAgg):
                    out_col = item.alias or _agg_alias(item.expr)
                    func = item.expr.func
                    in_col = _col_name(item.expr.arg) if item.expr.arg else "*"
                    agg_specs.append((out_col, func, in_col))
            node = AggregateNode(
                child=node,
                group_by=group_cols,
                aggregates=agg_specs,
            )

        # --- HAVING ---
        if q.having is not None:
            pred = _to_expr(q.having)
            node = FilterNode(child=node, predicate=pred, selectivity=0.5)

        # --- ORDER BY ---
        if q.order_by:
            order = [(_col_name(expr), asc) for expr, asc in q.order_by]
            node = SortNode(child=node, order_by=order)

        # --- LIMIT / OFFSET ---
        if q.limit is not None:
            node = LimitNode(child=node, limit=q.limit, offset=q.offset)

        # --- PROJECT ---
        cols = _select_columns(q.select, self.catalog, q.from_table, q.joins)
        if cols is not None:  # None means SELECT *
            node = ProjectNode(child=node, columns=cols)

        return node


# ------------------------------------------------------------------
# Key extraction for equi-joins
# ------------------------------------------------------------------

def _extract_equi_keys(
    on_expr: SqlExpr,
    alias_map: dict[str, str],
) -> tuple[str, str]:
    """Pull (left_key, right_key) from a simple ON col = col condition."""
    if isinstance(on_expr, SqlBinOp) and on_expr.op == "=":
        left, right = on_expr.left, on_expr.right
        if isinstance(left, SqlColRef) and isinstance(right, SqlColRef):
            return left.name, right.name
    raise PlanError(
        f"JOIN ON clause must be a simple equality (col = col), got: {on_expr}"
    )


# ------------------------------------------------------------------
# SELECT column resolution
# ------------------------------------------------------------------

def _select_columns(
    items: list,
    catalog: Catalog,
    from_table: str,
    joins: list,
) -> list[str] | None:
    """Return projected column list, or None for SELECT *."""
    all_star = all(isinstance(i.expr, SqlLiteral) and i.expr.value == "*" for i in items)
    if all_star:
        return None

    cols: list[str] = []
    for item in items:
        if isinstance(item.expr, SqlLiteral) and item.expr.value == "*":
            # Expand * into all columns from all tables
            for tname in [from_table] + [j.table for j in joins]:
                try:
                    for c in catalog.stats(tname).columns:
                        if c.name not in cols:
                            cols.append(c.name)
                except KeyError:
                    pass
        elif isinstance(item.expr, SqlAgg):
            out = item.alias or _agg_alias(item.expr)
            cols.append(out)
        elif isinstance(item.expr, SqlColRef):
            name = item.alias or item.expr.name
            cols.append(name)
        else:
            # Computed expression — need an alias
            name = item.alias or f"col{len(cols)}"
            cols.append(name)
    return cols


# ------------------------------------------------------------------
# Expression translation
# ------------------------------------------------------------------

def _to_expr(sql_expr: SqlExpr) -> Expr:
    """Translate a SqlExpr to the engine's Expr hierarchy."""
    match sql_expr:
        case SqlLiteral(value=v):
            return Literal(v)
        case SqlColRef(name=n):
            return ColRef(n)
        case SqlBinOp(left=l, op="AND", right=r):
            return AndExpr(_to_expr(l), _to_expr(r))
        case SqlBinOp(left=l, op="OR", right=r):
            return OrExpr(_to_expr(l), _to_expr(r))
        case SqlBinOp(left=l, op=op, right=r):
            return BinOp(_to_expr(l), op, _to_expr(r))
        case SqlUnaryOp(op="NOT", operand=o):
            return NotExpr(_to_expr(o))
        case SqlIsNull(expr=e, negated=neg):
            # Translate IS NULL as col = None (approximate)
            inner = BinOp(_to_expr(e), "=", Literal(None))
            return NotExpr(inner) if neg else inner
        case SqlAgg():
            raise PlanError("Aggregate functions cannot appear in WHERE/ON predicates")
        case _:
            raise PlanError(f"Cannot translate expression: {sql_expr!r}")


# ------------------------------------------------------------------
# Selectivity estimation from SQL predicates
# ------------------------------------------------------------------

def _estimate_selectivity(
    sql_expr: SqlExpr,
    table: str,
    catalog: Catalog,
    depth: int = 0,
) -> float:
    if depth > 10:
        return 0.5
    match sql_expr:
        case SqlBinOp(left=SqlColRef(name=col), op=op, right=SqlLiteral(value=val)):
            try:
                cs = catalog.stats(table).column(col)
                if cs:
                    return cs.selectivity_for_op(op, val)
            except KeyError:
                pass
            return 0.3
        case SqlBinOp(op="AND", left=l, right=r):
            sl = _estimate_selectivity(l, table, catalog, depth + 1)
            sr = _estimate_selectivity(r, table, catalog, depth + 1)
            return sl * sr
        case SqlBinOp(op="OR", left=l, right=r):
            sl = _estimate_selectivity(l, table, catalog, depth + 1)
            sr = _estimate_selectivity(r, table, catalog, depth + 1)
            return sl + sr - sl * sr
        case SqlUnaryOp(op="NOT", operand=o):
            return 1.0 - _estimate_selectivity(o, table, catalog, depth + 1)
        case SqlIsNull():
            return 0.05
        case _:
            return 0.5


# ------------------------------------------------------------------
# Helpers
# ------------------------------------------------------------------

def _col_name(expr: SqlExpr | None) -> str:
    if expr is None:
        return "*"
    if isinstance(expr, SqlColRef):
        return expr.name
    raise PlanError(f"Expected a column reference, got: {expr!r}")


def _agg_alias(agg: SqlAgg) -> str:
    arg = _col_name(agg.arg) if agg.arg else "*"
    return f"{agg.func}_{arg}"


def _collect_aggs(items: list) -> list[SqlAgg]:
    return [i.expr for i in items if isinstance(i.expr, SqlAgg)]
