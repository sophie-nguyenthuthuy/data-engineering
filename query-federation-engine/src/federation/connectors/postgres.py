"""PostgreSQL connector — translates pushed predicates into SQL WHERE clauses."""

from __future__ import annotations

from typing import Any

import pandas as pd
import sqlglot
import sqlglot.expressions as exp

from .base import BaseConnector, ConnectorResult


class PostgresConnector(BaseConnector):
    """
    Uses psycopg2 for real connections.
    Falls back to an in-memory DataFrame if `_mock_data` is set (for tests).
    """

    def __init__(self) -> None:
        self._mock_data: dict[str, pd.DataFrame] = {}

    def set_mock(self, table: str, df: pd.DataFrame) -> None:
        self._mock_data[table] = df

    def fetch(
        self,
        table: str,
        columns: list[str],
        predicates: list[exp.Expression],
        limit: int | None = None,
        connection_params: dict[str, Any] | None = None,
    ) -> ConnectorResult:
        if self._mock_data:
            return self._fetch_mock(table, columns, predicates, limit)
        return self._fetch_real(table, columns, predicates, limit, connection_params or {})

    def estimate_rows(
        self,
        table: str,
        predicates: list[exp.Expression],
        connection_params: dict[str, Any] | None = None,
    ) -> int:
        if table in self._mock_data:
            return len(self._mock_data[table])
        return 100_000

    # ------------------------------------------------------------------ #

    def _fetch_real(
        self,
        table: str,
        columns: list[str],
        predicates: list[exp.Expression],
        limit: int | None,
        params: dict[str, Any],
    ) -> ConnectorResult:
        import psycopg2
        import psycopg2.extras

        col_list = ", ".join(f'"{c}"' for c in columns) if columns else "*"
        where_sql, values = self._predicates_to_sql(predicates)
        query = f"SELECT {col_list} FROM {table}"
        if where_sql:
            query += f" WHERE {where_sql}"
        if limit:
            query += f" LIMIT {limit}"

        dsn = params.get("dsn") or (
            f"host={params.get('host','localhost')} "
            f"port={params.get('port',5432)} "
            f"dbname={params['dbname']} "
            f"user={params.get('user','postgres')} "
            f"password={params.get('password','')}"
        )
        conn = psycopg2.connect(dsn)
        try:
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                cur.execute(query, values)
                rows = cur.fetchall()
        finally:
            conn.close()

        df = pd.DataFrame(rows)
        return ConnectorResult(
            data=df,
            rows_scanned=len(df),
            rows_returned=len(df),
            source="postgres",
            table=table,
        )

    def _fetch_mock(
        self,
        table: str,
        columns: list[str],
        predicates: list[exp.Expression],
        limit: int | None,
    ) -> ConnectorResult:
        df = self._mock_data.get(table, pd.DataFrame()).copy()
        df = _apply_predicates(df, predicates)
        if columns:
            available = [c for c in columns if c in df.columns]
            if available:
                df = df[available]
        if limit:
            df = df.head(limit)
        return ConnectorResult(
            data=df.reset_index(drop=True),
            rows_scanned=len(self._mock_data.get(table, df)),
            rows_returned=len(df),
            source="postgres",
            table=table,
        )

    def _predicates_to_sql(
        self, predicates: list[exp.Expression]
    ) -> tuple[str, list[Any]]:
        """Translate sqlglot expressions to a parameterised SQL fragment."""
        if not predicates:
            return "", []
        parts, values = [], []
        for pred in predicates:
            sql, vals = _expr_to_sql(pred)
            parts.append(sql)
            values.extend(vals)
        return " AND ".join(parts), values


# --------------------------------------------------------------------------- #
# Helpers shared by mock + real path                                           #
# --------------------------------------------------------------------------- #

def _expr_to_sql(expr: exp.Expression) -> tuple[str, list[Any]]:
    """Return (sql_fragment, [bind_values]) for a single predicate."""
    match expr:
        case exp.EQ(this=col, expression=val) if isinstance(col, exp.Column):
            return f'"{col.name}" = %s', [_lit(val)]
        case exp.NEQ(this=col, expression=val) if isinstance(col, exp.Column):
            return f'"{col.name}" != %s', [_lit(val)]
        case exp.GT(this=col, expression=val) if isinstance(col, exp.Column):
            return f'"{col.name}" > %s', [_lit(val)]
        case exp.GTE(this=col, expression=val) if isinstance(col, exp.Column):
            return f'"{col.name}" >= %s', [_lit(val)]
        case exp.LT(this=col, expression=val) if isinstance(col, exp.Column):
            return f'"{col.name}" < %s', [_lit(val)]
        case exp.LTE(this=col, expression=val) if isinstance(col, exp.Column):
            return f'"{col.name}" <= %s', [_lit(val)]
        case exp.Like(this=col, expression=val) if isinstance(col, exp.Column):
            return f'"{col.name}" LIKE %s', [_lit(val)]
        case exp.In(this=col, expressions=vals) if isinstance(col, exp.Column):
            placeholders = ", ".join(["%s"] * len(vals))
            return f'"{col.name}" IN ({placeholders})', [_lit(v) for v in vals]
        case exp.Is(this=col, expression=exp.Null()) if isinstance(col, exp.Column):
            return f'"{col.name}" IS NULL', []
        case _:
            # Fall back to sqlglot SQL generation
            return expr.sql(dialect="postgres"), []


def _lit(expr: exp.Expression) -> Any:
    if isinstance(expr, exp.Literal):
        return expr.to_py()
    return str(expr)


def _apply_predicates(df: pd.DataFrame, predicates: list[exp.Expression]) -> pd.DataFrame:
    """Apply predicates to an in-memory DataFrame (mock mode)."""
    for pred in predicates:
        mask = _eval_predicate(df, pred)
        if mask is not None:
            df = df[mask]
    return df


def _eval_predicate(df: pd.DataFrame, expr: exp.Expression):
    """Return a boolean Series or None."""
    import pandas as pd

    def col(e: exp.Expression):
        name = e.name if isinstance(e, exp.Column) else str(e)
        return df[name] if name in df.columns else None

    def lit(e: exp.Expression):
        return _lit(e)

    match expr:
        case exp.EQ(this=c, expression=v) if isinstance(c, exp.Column):
            s = col(c)
            return s == lit(v) if s is not None else None
        case exp.NEQ(this=c, expression=v) if isinstance(c, exp.Column):
            s = col(c)
            return s != lit(v) if s is not None else None
        case exp.GT(this=c, expression=v) if isinstance(c, exp.Column):
            s = col(c)
            return s > lit(v) if s is not None else None
        case exp.GTE(this=c, expression=v) if isinstance(c, exp.Column):
            s = col(c)
            return s >= lit(v) if s is not None else None
        case exp.LT(this=c, expression=v) if isinstance(c, exp.Column):
            s = col(c)
            return s < lit(v) if s is not None else None
        case exp.LTE(this=c, expression=v) if isinstance(c, exp.Column):
            s = col(c)
            return s <= lit(v) if s is not None else None
        case exp.Like(this=c, expression=v) if isinstance(c, exp.Column):
            s = col(c)
            pattern = str(lit(v)).replace("%", ".*").replace("_", ".")
            return s.str.match(pattern) if s is not None else None
        case exp.In(this=c, expressions=vals) if isinstance(c, exp.Column):
            s = col(c)
            return s.isin([lit(v) for v in vals]) if s is not None else None
        case exp.And(this=left, expression=right):
            lm = _eval_predicate(df, left)
            rm = _eval_predicate(df, right)
            if lm is not None and rm is not None:
                return lm & rm
        case exp.Or(this=left, expression=right):
            lm = _eval_predicate(df, left)
            rm = _eval_predicate(df, right)
            if lm is not None and rm is not None:
                return lm | rm
        case exp.Not(this=inner):
            m = _eval_predicate(df, inner)
            return ~m if m is not None else None
    return None
