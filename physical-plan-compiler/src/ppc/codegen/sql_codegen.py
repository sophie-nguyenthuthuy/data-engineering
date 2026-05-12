"""Generic SQL renderer for any subtree on a SQL-speaking engine.

Used by DuckDB / dbt code generators to emit the SQL body. Each engine has
its own dialect quirks but the structural emission is identical.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from ppc.engines.physical_ops import (
    PhysicalAggregate,
    PhysicalConversion,
    PhysicalFilter,
    PhysicalHashJoin,
    PhysicalScan,
)
from ppc.ir.expr import BinaryOp, ColumnRef, Expr, Literal, UnaryOp

if TYPE_CHECKING:
    from ppc.ir.physical import PhysicalNode

_SQL_OP = {
    "=": "=", "!=": "<>", "<": "<", "<=": "<=", ">": ">", ">=": ">=",
    "+": "+", "-": "-", "*": "*", "/": "/", "%": "%",
    "AND": "AND", "OR": "OR",
}


def render_expr(e: Expr) -> str:
    if isinstance(e, Literal):
        v = e.value
        if v is None:
            return "NULL"
        if isinstance(v, str):
            return "'" + v.replace("'", "''") + "'"
        if isinstance(v, bool):
            return "TRUE" if v else "FALSE"
        return str(v)
    if isinstance(e, ColumnRef):
        return e.name
    if isinstance(e, BinaryOp):
        return f"({render_expr(e.left)} {_SQL_OP[e.op]} {render_expr(e.right)})"
    if isinstance(e, UnaryOp):
        if e.op == "NOT":
            return f"NOT ({render_expr(e.operand)})"
        return f"({e.op}{render_expr(e.operand)})"
    raise NotImplementedError(f"render_expr: {type(e).__name__}")


def emit_sql(node: PhysicalNode, dialect: str = "duckdb") -> str:
    """Render a physical sub-tree as a SQL query (one SELECT)."""
    if isinstance(node, PhysicalScan):
        return f"SELECT * FROM {node.table}"

    if isinstance(node, PhysicalFilter):
        (child,) = node.children
        inner = emit_sql(child, dialect=dialect)
        return f"SELECT * FROM ({inner}) AS _t WHERE {render_expr(node.predicate)}"

    if isinstance(node, PhysicalAggregate):
        (child,) = node.children
        inner = emit_sql(child, dialect=dialect)
        select_cols = []
        for g in node.group_by:
            select_cols.append(g.name)
        for a in node.aggregates:
            arg = "*" if a.arg is None else render_expr(a.arg)
            select_cols.append(f"{a.func}({arg}) AS {a.alias}")
        groupby = "GROUP BY " + ", ".join(g.name for g in node.group_by) if node.group_by else ""
        return f"SELECT {', '.join(select_cols)} FROM ({inner}) AS _t {groupby}".rstrip()

    if isinstance(node, PhysicalHashJoin):
        left, right = node.children
        l_sql = emit_sql(left, dialect=dialect)
        r_sql = emit_sql(right, dialect=dialect)
        return (
            f"SELECT * FROM ({l_sql}) AS _l "
            f"{node.join_type} JOIN ({r_sql}) AS _r "
            f"ON {render_expr(node.on)}"
        )

    if isinstance(node, PhysicalConversion):
        # On a SQL engine: assume the conversion materialised a table; the
        # caller is responsible for naming it. Here we just emit the child.
        return emit_sql(node.child, dialect=dialect)

    raise NotImplementedError(f"emit_sql: {type(node).__name__}")
