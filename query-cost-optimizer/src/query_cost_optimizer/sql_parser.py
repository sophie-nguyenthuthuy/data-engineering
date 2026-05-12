"""SQL parsing helpers using sqlglot."""

from __future__ import annotations

import logging
from functools import lru_cache

logger = logging.getLogger(__name__)

try:
    import sqlglot
    import sqlglot.expressions as exp
    _SQLGLOT_AVAILABLE = True
except ImportError:
    _SQLGLOT_AVAILABLE = False


def _parse(sql: str):
    """Return parsed AST or None."""
    if not _SQLGLOT_AVAILABLE or not sql:
        return None
    try:
        return sqlglot.parse_one(sql, error_level=sqlglot.ErrorLevel.IGNORE)
    except Exception:
        return None


def extract_filter_columns(sql: str, table_hint: str = "") -> list[str]:
    """Return column names used in WHERE / HAVING clauses."""
    ast = _parse(sql)
    if ast is None:
        return _regex_fallback_where(sql)
    cols: list[str] = []
    for node in ast.find_all(exp.Where, exp.Having):
        for col in node.find_all(exp.Column):
            name = col.name
            if name and name not in cols:
                cols.append(name)
    return cols


def extract_join_columns(sql: str, table_hint: str = "") -> list[str]:
    """Return column names used in JOIN conditions."""
    ast = _parse(sql)
    if ast is None:
        return []
    cols: list[str] = []
    for join in ast.find_all(exp.Join):
        for col in join.find_all(exp.Column):
            name = col.name
            if name and name not in cols:
                cols.append(name)
    return cols


def extract_group_by_columns(sql: str) -> list[str]:
    """Return column names used in GROUP BY."""
    ast = _parse(sql)
    if ast is None:
        return []
    cols: list[str] = []
    for node in ast.find_all(exp.Group):
        for col in node.find_all(exp.Column):
            name = col.name
            if name and name not in cols:
                cols.append(name)
    return cols


def extract_select_star(sql: str) -> bool:
    """Return True if the query contains a SELECT *."""
    ast = _parse(sql)
    if ast is None:
        return "select *" in sql.lower()
    return bool(ast.find(exp.Star))


def extract_referenced_tables(sql: str) -> list[str]:
    """Return fully-qualified table names referenced in the query."""
    ast = _parse(sql)
    if ast is None:
        return []
    tables: list[str] = []
    for tbl in ast.find_all(exp.Table):
        parts = [p for p in [tbl.catalog, tbl.db, tbl.name] if p]
        full = ".".join(parts)
        if full and full not in tables:
            tables.append(full)
    return tables


def detect_expensive_patterns(sql: str) -> list[str]:
    """Return a list of expensive-pattern names found in sql."""
    patterns: list[str] = []
    sql_lower = sql.lower()

    if extract_select_star(sql):
        patterns.append("select_star")

    ast = _parse(sql)
    if ast is not None:
        # CROSS JOIN
        for join in ast.find_all(exp.Join):
            if join.args.get("kind") and str(join.args["kind"]).upper() == "CROSS":
                patterns.append("cross_join")
                break

        # Non-sargable: functions on filter columns  e.g. WHERE UPPER(col) = ...
        for where in ast.find_all(exp.Where):
            for func in where.find_all(exp.Anonymous, exp.Upper, exp.Lower, exp.Cast, exp.TryCast):
                patterns.append("non_sargable_filter")
                break

        # Nested subquery in SELECT list
        for sel in ast.find_all(exp.Select):
            for expr in sel.expressions:
                if isinstance(expr, exp.Subquery):
                    patterns.append("scalar_subquery_in_select")
                    break

        # DISTINCT without ORDER BY / GROUP BY — often a lazy dedup
        for sel in ast.find_all(exp.Select):
            if sel.args.get("distinct") and not ast.find(exp.Group) and not ast.find(exp.Order):
                patterns.append("unnecessary_distinct")
                break

    # ORDER BY without LIMIT (full sort, no use)
    if ast is not None:
        if ast.find(exp.Order) and not ast.find(exp.Limit):
            patterns.append("order_without_limit")

    return list(set(patterns))


# ---------------------------------------------------------------------------
# Fallback (no sqlglot)
# ---------------------------------------------------------------------------

def _regex_fallback_where(sql: str) -> list[str]:
    import re
    matches = re.findall(r"WHERE\s+(\w+)\s*[=<>!]", sql, re.IGNORECASE)
    return list(set(matches))
