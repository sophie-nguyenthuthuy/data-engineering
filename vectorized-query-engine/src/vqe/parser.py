"""SQL → LogicalPlan via sqlglot AST walking."""
from __future__ import annotations

from typing import Optional

import pyarrow as pa
import sqlglot
import sqlglot.expressions as sge

from .expressions import (
    AggExpr,
    BetweenExpr,
    BinaryExpr,
    CastExpr,
    ColumnRef,
    Expr,
    InExpr,
    IsNullExpr,
    Literal,
    UnaryExpr,
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
# Expression conversion
# ---------------------------------------------------------------------------

def _conv_expr(node: sge.Expression) -> Expr:
    if isinstance(node, sge.Star):
        return ColumnRef("*")

    if isinstance(node, sge.Column):
        return ColumnRef(node.name)

    if isinstance(node, sge.Literal):
        if node.is_string:
            return Literal(node.this)
        v = node.this
        try:
            v = int(v)
        except (ValueError, TypeError):
            try:
                v = float(v)
            except (ValueError, TypeError):
                pass
        return Literal(v)

    if isinstance(node, sge.Neg):
        return UnaryExpr("-", _conv_expr(node.this))

    if isinstance(node, sge.Not):
        return UnaryExpr("NOT", _conv_expr(node.this))

    if isinstance(node, sge.Paren):
        return _conv_expr(node.this)

    if isinstance(node, sge.Alias):
        return _conv_expr(node.this)

    # Boolean binary
    if isinstance(node, sge.And):
        return BinaryExpr("AND", _conv_expr(node.left), _conv_expr(node.right))
    if isinstance(node, sge.Or):
        return BinaryExpr("OR", _conv_expr(node.left), _conv_expr(node.right))

    # Comparison
    _cmp_map: dict = {
        sge.EQ:  "=",
        sge.NEQ: "!=",
        sge.LT:  "<",
        sge.LTE: "<=",
        sge.GT:  ">",
        sge.GTE: ">=",
    }
    for cls, op in _cmp_map.items():
        if isinstance(node, cls):
            return BinaryExpr(op, _conv_expr(node.left), _conv_expr(node.right))

    # Arithmetic
    _arith_map: dict = {
        sge.Add: "+",
        sge.Sub: "-",
        sge.Mul: "*",
        sge.Div: "/",
    }
    for cls, op in _arith_map.items():
        if isinstance(node, cls):
            return BinaryExpr(op, _conv_expr(node.left), _conv_expr(node.right))

    if isinstance(node, sge.Like):
        return BinaryExpr("LIKE", _conv_expr(node.this), _conv_expr(node.expression))

    if isinstance(node, sge.Between):
        return BetweenExpr(
            _conv_expr(node.this),
            _conv_expr(node.args["low"]),
            _conv_expr(node.args["high"]),
        )

    if isinstance(node, sge.In):
        values = []
        for e in node.expressions:
            converted = _conv_expr(e)
            if isinstance(converted, Literal):
                values.append(converted.value)
        return InExpr(_conv_expr(node.this), values)

    if isinstance(node, sge.Is):
        inner = _conv_expr(node.this)
        negated = isinstance(node.expression, sge.Not) or isinstance(node.expression, sge.Not)
        return IsNullExpr(inner, negated=negated)

    if isinstance(node, sge.Cast):
        inner = _conv_expr(node.this)
        to_type = _conv_pa_type(node.to)
        return CastExpr(inner, to_type)

    # Aggregates
    _agg_map: dict = {
        sge.Sum: "sum",
        sge.Avg: "avg",
        sge.Min: "min",
        sge.Max: "max",
    }
    for cls, func in _agg_map.items():
        if isinstance(node, cls):
            return AggExpr(func, _conv_expr(node.this))

    if isinstance(node, sge.Count):
        inner = node.this
        if inner is None or isinstance(inner, sge.Star):
            return AggExpr("count_star")
        return AggExpr("count", _conv_expr(inner))

    if isinstance(node, sge.Anonymous):
        name = node.this.upper()
        if name in ("COUNT", "SUM", "AVG", "MIN", "MAX"):
            args = node.expressions
            if name == "COUNT" and (not args or isinstance(args[0], sge.Star)):
                return AggExpr("count_star")
            return AggExpr(name.lower(), _conv_expr(args[0]) if args else None)

    raise NotImplementedError(f"Unsupported expression: {type(node).__name__}: {node}")


def _conv_pa_type(node: sge.DataType) -> pa.DataType:
    t = node.this
    if t in (sge.DataType.Type.INT, sge.DataType.Type.INT4):
        return pa.int32()
    if t in (sge.DataType.Type.BIGINT, sge.DataType.Type.INT8):
        return pa.int64()
    if t in (sge.DataType.Type.FLOAT, sge.DataType.Type.REAL):
        return pa.float32()
    if t in (sge.DataType.Type.DOUBLE, sge.DataType.Type.FLOAT8):
        return pa.float64()
    if t == sge.DataType.Type.TEXT:
        return pa.string()
    if t == sge.DataType.Type.DATE:
        return pa.date32()
    return pa.string()


# ---------------------------------------------------------------------------
# SELECT item helpers
# ---------------------------------------------------------------------------

class _SelectItem:
    """Parsed output of a single SELECT item."""
    def __init__(self, expr: Expr, alias: Optional[str]):
        self.expr = expr
        self.alias = alias
        self.is_star = isinstance(expr, ColumnRef) and expr.name == "*"
        self.is_agg = isinstance(expr, AggExpr)


def _parse_select_items(items) -> list[_SelectItem]:
    result = []
    for item in items:
        if isinstance(item, sge.Star):
            result.append(_SelectItem(ColumnRef("*"), None))
            continue
        alias = item.alias if isinstance(item, sge.Alias) else None
        expr = _conv_expr(item)
        # Fix alias on AggExpr
        if isinstance(expr, AggExpr) and alias:
            expr.alias = alias
        result.append(_SelectItem(expr, alias))
    return result


# ---------------------------------------------------------------------------
# Statement → LogicalPlan
# ---------------------------------------------------------------------------

def parse(sql: str) -> LogicalPlan:
    statements = sqlglot.parse(sql, read="duckdb")
    if not statements:
        raise ValueError("No statements found")
    stmt = statements[0]
    if not isinstance(stmt, sge.Select):
        raise NotImplementedError(f"Only SELECT is supported, got {type(stmt).__name__}")
    return _build_select(stmt)


def _build_select(stmt: sge.Select) -> LogicalPlan:
    # --- FROM / JOIN ---
    plan = _build_from(stmt)

    # --- WHERE ---
    where = stmt.find(sge.Where)
    if where:
        pred = _conv_expr(where.this)
        plan = Filter(plan, pred)

    # --- SELECT list ---
    select_items = _parse_select_items(stmt.expressions)

    # Short-circuit: SELECT *
    is_star_only = len(select_items) == 1 and select_items[0].is_star

    # Separate regular vs aggregate expressions
    regular: list[_SelectItem] = []
    aggs: list[_SelectItem] = []
    for si in select_items:
        if si.is_agg:
            aggs.append(si)
        elif not si.is_star:
            regular.append(si)

    # --- GROUP BY ---
    group_node = stmt.find(sge.Group)
    group_by: list[Expr] = []
    if group_node:
        group_by = [_conv_expr(e) for e in group_node.expressions]

    has_agg = bool(aggs)

    if has_agg or group_by:
        agg_exprs: list[AggExpr] = [si.expr for si in aggs]  # type: ignore[assignment]

        # Assign names to aggregates
        for i, agg in enumerate(agg_exprs):
            if not agg.alias:
                agg.alias = agg.output_name

        plan = Aggregate(plan, group_by, agg_exprs)

        # HAVING
        having = stmt.find(sge.Having)
        if having:
            plan = Filter(plan, _conv_expr(having.this))

        # Final projection: group keys + aggregate outputs
        proj_exprs: list[Expr] = list(group_by) + [ColumnRef(a.output_name) for a in agg_exprs]
        proj_aliases: list[Optional[str]] = (
            [None] * len(group_by) + [a.alias for a in agg_exprs]
        )
        plan = Project(plan, proj_exprs, proj_aliases)

    elif not is_star_only:
        # Pure projection (no aggregates)
        exprs = [si.expr for si in regular]
        aliases = [si.alias for si in regular]
        if exprs:
            plan = Project(plan, exprs, aliases)

    # --- ORDER BY ---
    order = stmt.find(sge.Order)
    if order:
        keys = []
        ascending = []
        for o in order.expressions:
            if isinstance(o, sge.Ordered):
                keys.append(_conv_expr(o.this))
                ascending.append(not o.args.get("desc", False))
            else:
                keys.append(_conv_expr(o))
                ascending.append(True)
        plan = Sort(plan, keys, ascending)

    # --- LIMIT / OFFSET ---
    limit_node = stmt.find(sge.Limit)
    if limit_node:
        # sqlglot stores the limit value in `expression`, not `this`
        limit_val = limit_node.args.get("expression") or limit_node.this
        n = int(limit_val.this)
        offset_node = stmt.find(sge.Offset)
        if offset_node:
            off_val = offset_node.args.get("expression") or offset_node.this
            offset = int(off_val.this)
        else:
            offset = 0
        plan = Limit(plan, n, offset)

    return plan


def _build_from(stmt: sge.Select) -> LogicalPlan:
    from_node = stmt.find(sge.From)
    if not from_node:
        raise ValueError("SELECT without FROM is not supported")

    source = from_node.this
    plan = _source_to_plan(source)

    for join in stmt.find_all(sge.Join):
        right = _source_to_plan(join.this)
        on_clause = join.args.get("on")
        if on_clause is None:
            raise ValueError("Only JOIN ... ON is supported")
        cond = _conv_expr(on_clause)
        side = join.args.get("side", "")
        jtype = "LEFT" if str(side).upper() == "LEFT" else "INNER"
        plan = Join(plan, right, cond, jtype)

    return plan


def _source_to_plan(source: sge.Expression) -> LogicalPlan:
    if isinstance(source, sge.Table):
        return Scan(source.name.lower())
    if isinstance(source, sge.Subquery):
        inner = source.this
        if isinstance(inner, sge.Select):
            return _build_select(inner)
    raise NotImplementedError(f"Unsupported FROM source: {type(source).__name__}")
