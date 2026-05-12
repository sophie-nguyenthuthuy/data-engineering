"""SQL -> Logical plan, via sqlglot.

Subset supported (TPC-H-shaped queries):
  - SELECT cols, agg(*), agg(col) [AS alias]
  - FROM table  (single table or N-way INNER JOIN)
  - WHERE conjunctive predicate (=, !=, <, <=, >, >=, AND, OR, NOT)
  - GROUP BY col [, col ...]
  - HAVING (treated as Filter on top of Aggregate)
  - ORDER BY / LIMIT are recorded for codegen but don't affect the logical plan

We deliberately punt on subqueries, UNION, window functions, complex CASE,
correlated WHERE etc. — they belong in a richer frontend.
"""

from __future__ import annotations

from typing import Any

import sqlglot
from sqlglot import expressions as sg

from ppc.frontend.catalog import Catalog
from ppc.ir.expr import (
    AND,
    NOT,
    OR,
    BinaryOp,
    ColumnRef,
    Expr,
    Literal,
    UnaryOp,
    column_from_schema,
    lit,
)
from ppc.ir.logical import (
    AggFunc,
    LogicalAggregate,
    LogicalFilter,
    LogicalJoin,
    LogicalNode,
    LogicalScan,
)
from ppc.ir.schema import Schema


class SqlParseError(Exception):
    """Raised when SQL parses but lies outside the supported subset."""


def sql_to_logical(sql: str, catalog: Catalog, dialect: str = "duckdb") -> LogicalNode:
    """Parse `sql` into a logical plan against `catalog`."""
    parsed = sqlglot.parse_one(sql, read=dialect)
    if not isinstance(parsed, sg.Select):
        raise SqlParseError(f"only SELECT supported, got {type(parsed).__name__}")
    return _select_to_logical(parsed, catalog)


def compile_sql(sql: str, catalog: Catalog, dialect: str = "duckdb") -> Any:
    """One-shot compile: SQL -> PhysicalPlan via the default Optimizer.

    Re-exported through ppc.__init__.
    """
    from ppc.cascades.optimizer import Optimizer

    logical = sql_to_logical(sql, catalog, dialect)
    return Optimizer(catalog).optimize(logical)


# ---------------------------------------------------------------------------
# SELECT compilation
# ---------------------------------------------------------------------------


def _select_to_logical(node: sg.Select, catalog: Catalog) -> LogicalNode:
    # 1. FROM (possibly with JOINs)
    plan, name_scope = _from_clause(node, catalog)

    # 2. WHERE
    where = node.args.get("where")
    if isinstance(where, sg.Where):
        pred = _expr_to_ir(where.this, name_scope)
        plan = LogicalFilter(child=plan, predicate=pred)

    # 3. GROUP BY / HAVING / aggregates in SELECT
    group = node.args.get("group")
    has_agg = any(_contains_agg(p) for p in node.expressions)
    if group or has_agg:
        plan = _build_aggregate(node, plan, name_scope)
        # After aggregate, name_scope changes — but we don't support
        # post-aggregate WHERE other than HAVING.
        having = node.args.get("having")
        if isinstance(having, sg.Having):
            # After aggregate, the schema has group cols + agg aliases.
            scope_after = _schema_to_scope(plan.schema)
            hp = _expr_to_ir(having.this, scope_after)
            plan = LogicalFilter(child=plan, predicate=hp)

    return plan


def _from_clause(
    node: sg.Select, catalog: Catalog
) -> tuple[LogicalNode, dict[str, Schema]]:
    """Build the FROM/JOIN sub-tree, return (plan, name -> schema)."""
    # sqlglot ≥25 names this `from_` to avoid Python keyword clash
    frm = node.args.get("from_") or node.args.get("from")
    if not isinstance(frm, sg.From):
        raise SqlParseError("missing FROM clause")

    table = frm.this
    if not isinstance(table, sg.Table):
        raise SqlParseError(f"FROM must be a table, got {type(table).__name__}")
    tname = table.name
    alias = table.alias_or_name
    if tname not in catalog:
        raise SqlParseError(f"unknown table: {tname}")
    schema = catalog.get(tname)
    plan: LogicalNode = LogicalScan(table=tname, table_schema=schema)
    scope: dict[str, Schema] = {alias: schema}

    # JOINs
    for j in node.args.get("joins") or []:
        if not isinstance(j, sg.Join):
            continue
        jtype = (j.args.get("kind") or "INNER").upper()
        if jtype not in ("INNER", ""):
            raise SqlParseError(f"only INNER JOIN supported (got {jtype})")
        right_tbl = j.this
        if not isinstance(right_tbl, sg.Table):
            raise SqlParseError("JOIN target must be a table")
        rname = right_tbl.name
        ralias = right_tbl.alias_or_name
        if rname not in catalog:
            raise SqlParseError(f"unknown table: {rname}")
        rschema = catalog.get(rname)
        right_plan = LogicalScan(table=rname, table_schema=rschema)
        scope[ralias] = rschema
        on = j.args.get("on")
        if on is None:
            raise SqlParseError("INNER JOIN requires ON")
        cond = _expr_to_ir(on, scope)
        plan = LogicalJoin(left=plan, right=right_plan, on=cond, join_type="INNER")
    return plan, scope


def _build_aggregate(
    select: sg.Select, plan: LogicalNode, scope: dict[str, Schema]
) -> LogicalNode:
    """Construct LogicalAggregate from GROUP BY + agg functions in SELECT."""
    group_cols: list[ColumnRef] = []
    aggs: list[AggFunc] = []

    grp = select.args.get("group")
    if isinstance(grp, sg.Group):
        for g in grp.expressions:
            ir = _expr_to_ir(g, scope)
            if not isinstance(ir, ColumnRef):
                raise SqlParseError(
                    f"GROUP BY supports column refs only, got {type(ir).__name__}"
                )
            group_cols.append(ir)

    for proj in select.expressions:
        # AGG functions show up as Count/Sum/Avg/Min/Max
        actual = proj.unalias() if isinstance(proj, sg.Alias) else proj
        alias = proj.alias_or_name
        if isinstance(actual, sg.Count):
            arg = actual.this
            if isinstance(arg, sg.Star):
                aggs.append(AggFunc(func="COUNT", arg=None, alias=alias))
            else:
                aggs.append(AggFunc(func="COUNT", arg=_expr_to_ir(arg, scope), alias=alias))
        elif isinstance(actual, sg.Sum):
            aggs.append(AggFunc(func="SUM", arg=_expr_to_ir(actual.this, scope), alias=alias))
        elif isinstance(actual, sg.Avg):
            aggs.append(AggFunc(func="AVG", arg=_expr_to_ir(actual.this, scope), alias=alias))
        elif isinstance(actual, sg.Min):
            aggs.append(AggFunc(func="MIN", arg=_expr_to_ir(actual.this, scope), alias=alias))
        elif isinstance(actual, sg.Max):
            aggs.append(AggFunc(func="MAX", arg=_expr_to_ir(actual.this, scope), alias=alias))
        else:
            # Non-aggregate column — must be in GROUP BY
            ir = _expr_to_ir(actual, scope)
            if isinstance(ir, ColumnRef) and any(g.name == ir.name for g in group_cols):
                continue
            raise SqlParseError(
                f"non-aggregate projection {actual} not in GROUP BY"
            )
    return LogicalAggregate(child=plan, group_by=tuple(group_cols), aggregates=tuple(aggs))


# ---------------------------------------------------------------------------
# Expression compilation
# ---------------------------------------------------------------------------


def _expr_to_ir(node: sg.Expression, scope: dict[str, Schema]) -> Expr:
    if isinstance(node, sg.Column):
        return _resolve_column(node, scope)
    if isinstance(node, sg.Literal):
        if node.is_string:
            return lit(node.this)
        # numeric
        try:
            i = int(node.this)
            return lit(i)
        except ValueError:
            return lit(float(node.this))
    if isinstance(node, sg.Boolean):
        return lit(bool(node.this))
    if isinstance(node, sg.Null):
        return lit(None)
    if isinstance(node, sg.Paren):
        return _expr_to_ir(node.this, scope)
    if isinstance(node, sg.Not):
        return NOT(_expr_to_ir(node.this, scope))
    if isinstance(node, sg.And):
        return AND(_expr_to_ir(node.this, scope), _expr_to_ir(node.expression, scope))
    if isinstance(node, sg.Or):
        return OR(_expr_to_ir(node.this, scope), _expr_to_ir(node.expression, scope))

    op_map = {
        sg.EQ: "=",
        sg.NEQ: "!=",
        sg.LT: "<",
        sg.LTE: "<=",
        sg.GT: ">",
        sg.GTE: ">=",
        sg.Add: "+",
        sg.Sub: "-",
        sg.Mul: "*",
        sg.Div: "/",
        sg.Mod: "%",
    }
    for cls, op_name in op_map.items():
        if isinstance(node, cls):
            l = _expr_to_ir(node.this, scope)
            r = _expr_to_ir(node.expression, scope)
            return BinaryOp(op=op_name, left=l, right=r)

    raise SqlParseError(f"unsupported expression: {type(node).__name__} ({node})")


def _resolve_column(node: sg.Column, scope: dict[str, Schema]) -> ColumnRef:
    name = node.name
    table = node.table  # qualifier (may be "")
    # If qualified: scope[table].column
    if table:
        if table not in scope:
            raise SqlParseError(f"unknown table alias in column ref: {table}.{name}")
        return column_from_schema(name, scope[table])
    # Unqualified: search all schemas in scope
    matches: list[Schema] = []
    for s in scope.values():
        if any(c.name == name for c in s.columns):
            matches.append(s)
    if not matches:
        raise SqlParseError(f"unknown column: {name}")
    if len(matches) > 1:
        raise SqlParseError(f"ambiguous column: {name}")
    return column_from_schema(name, matches[0])


def _schema_to_scope(schema: Schema) -> dict[str, Schema]:
    return {"": schema}


def _contains_agg(node: sg.Expression) -> bool:
    for n in node.walk():
        # sqlglot 30: walk() yields nodes directly
        target = n if isinstance(n, sg.Expression) else (n[0] if isinstance(n, tuple) else None)
        if target is None:
            continue
        if isinstance(target, sg.AggFunc) or isinstance(
            target, (sg.Count, sg.Sum, sg.Avg, sg.Min, sg.Max)
        ):
            return True
    return False
