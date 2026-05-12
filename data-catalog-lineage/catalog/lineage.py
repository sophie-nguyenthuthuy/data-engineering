"""
Column-level lineage extraction using sqlglot.
Parses SQL INSERT/CREATE TABLE AS SELECT/MERGE statements
and returns (source_column, target_column) pairs.
"""
from __future__ import annotations
import re
import sqlglot
from sqlglot import exp
from sqlglot.lineage import lineage as sqlglot_lineage


def _strip_quotes(name: str) -> str:
    return name.strip('"').strip("'").strip("`")


def _fqn(table: str, column: str, schema: str | None = None) -> str:
    if schema:
        return f"{schema}.{table}.{column}"
    return f"{table}.{column}"


def extract_lineage(sql: str, dialect: str = "sqlite") -> list[dict]:
    """
    Parse a SQL statement and return column-level lineage edges.

    Returns a list of dicts:
        {"source": "schema.table.column", "target": "schema.table.column", "transform": str}
    """
    edges: list[dict] = []

    try:
        statements = sqlglot.parse(sql, dialect=dialect, error_level=sqlglot.ErrorLevel.IGNORE)
    except Exception:
        return edges

    for stmt in statements:
        if stmt is None:
            continue

        # Determine the target table
        target_table: str | None = None
        target_schema: str | None = None
        explicit_targets: list[str] = []

        if isinstance(stmt, exp.Create):
            this = stmt.args.get("this")
            if isinstance(this, exp.Table):
                target_table = _strip_quotes(this.name)
                if this.args.get("db"):
                    target_schema = _strip_quotes(str(this.args["db"]))
        elif isinstance(stmt, exp.Insert):
            this = stmt.args.get("this")
            # sqlglot >=v26: INSERT INTO t (c1,c2) → Schema node wrapping Table
            if isinstance(this, exp.Schema):
                tbl = this.args.get("this")
                if isinstance(tbl, exp.Table):
                    target_table = _strip_quotes(tbl.name)
                    if tbl.args.get("db"):
                        target_schema = _strip_quotes(str(tbl.args["db"]))
                # Column list lives inside Schema.expressions
                col_exprs = this.args.get("expressions") or []
                explicit_targets = [_strip_quotes(c.name) for c in col_exprs]
            elif isinstance(this, exp.Table):
                target_table = _strip_quotes(this.name)
                if this.args.get("db"):
                    target_schema = _strip_quotes(str(this.args["db"]))
                # Older fallback: explicit columns as top-level arg
                cols_node = stmt.args.get("columns")
                if cols_node:
                    explicit_targets = [_strip_quotes(c.name) for c in cols_node]
        elif isinstance(stmt, exp.Select):
            # bare SELECT → skip, no target
            continue

        if target_table is None:
            continue

        # Pull the SELECT part
        select_expr: exp.Select | None = None
        if isinstance(stmt, (exp.Create, exp.Insert)):
            select_expr = stmt.find(exp.Select)

        if select_expr is None:
            continue

        for i, sel in enumerate(select_expr.expressions):
            # Resolve target column name
            if explicit_targets and i < len(explicit_targets):
                tgt_col = explicit_targets[i]
            elif isinstance(sel, exp.Alias):
                tgt_col = _strip_quotes(sel.alias)
            elif isinstance(sel, exp.Column):
                tgt_col = _strip_quotes(sel.name)
            else:
                tgt_col = f"col_{i}"

            target_fqn = _fqn(target_table, tgt_col, target_schema)
            transform = sel.sql(dialect=dialect)

            # Walk expression to find all source columns
            source_cols = _extract_source_columns(sel, select_expr)

            if source_cols:
                for src in source_cols:
                    edges.append({"source": src, "target": target_fqn, "transform": transform})
            else:
                # Literal or computed with no traceable source — record as self-ref
                edges.append({"source": None, "target": target_fqn, "transform": transform})

    return edges


def _extract_source_columns(expr: exp.Expression, select_ctx: exp.Select) -> list[str]:
    """Walk an expression and collect qualified source column FQNs."""
    sources: dict[str, str] = {}  # alias → table name

    for frm in select_ctx.find_all(exp.From, exp.Join):
        tbl = frm.find(exp.Table)
        if tbl:
            tname = _strip_quotes(tbl.name)
            schema = _strip_quotes(str(tbl.args["db"])) if tbl.args.get("db") else None
            alias = tbl.alias or tname
            sources[_strip_quotes(alias)] = (schema, tname)

    result: list[str] = []
    for col in expr.find_all(exp.Column):
        col_name = _strip_quotes(col.name)
        tbl_ref = col.table
        if tbl_ref:
            tbl_ref = _strip_quotes(tbl_ref)
            resolved = sources.get(tbl_ref, (None, tbl_ref))
        else:
            # Unqualified — take first FROM table
            if sources:
                resolved = next(iter(sources.values()))
            else:
                resolved = (None, "unknown")

        schema_p, table_p = resolved
        result.append(_fqn(table_p, col_name, schema_p))

    return result


def parse_column_refs(fqn: str) -> tuple[str | None, str | None, str]:
    """Split 'schema.table.column' or 'table.column' into parts."""
    parts = fqn.split(".")
    if len(parts) == 3:
        return parts[0], parts[1], parts[2]
    elif len(parts) == 2:
        return None, parts[0], parts[1]
    return None, None, fqn
