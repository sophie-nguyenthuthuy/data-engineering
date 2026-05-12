"""PostgreSQL engine: translates predicates to SQL WHERE clause fragments."""
from __future__ import annotations

import datetime
from typing import Any, Dict, List, Optional, Set

from dqp.engines.base import EngineBase, EngineCapability, PushdownResult
from dqp.predicate import (
    AndPredicate,
    BetweenPredicate,
    ComparisonOp,
    ComparisonPredicate,
    InPredicate,
    IsNullPredicate,
    LikePredicate,
    Literal,
    NotPredicate,
    OrPredicate,
    Predicate,
)

_OP_SQL: Dict[ComparisonOp, str] = {
    ComparisonOp.EQ: "=",
    ComparisonOp.NEQ: "<>",
    ComparisonOp.LT: "<",
    ComparisonOp.LTE: "<=",
    ComparisonOp.GT: ">",
    ComparisonOp.GTE: ">=",
}


def _quote_identifier(name: str) -> str:
    """Double-quote a Postgres identifier to preserve case and escape quotes."""
    return '"' + name.replace('"', '""') + '"'


def format_value(lit: Literal) -> str:
    """Format a Literal as a proper SQL literal string."""
    val = lit.value
    dtype = lit.dtype

    if dtype == "null" or val is None:
        return "NULL"

    if dtype == "bool":
        return "TRUE" if val else "FALSE"

    if dtype == "int":
        return str(int(val))

    if dtype == "float":
        return repr(float(val))

    if dtype == "str":
        # Escape single quotes by doubling them
        escaped = str(val).replace("'", "''")
        return f"'{escaped}'"

    if dtype == "date":
        if isinstance(val, datetime.date):
            return f"DATE '{val.isoformat()}'"
        return f"DATE '{val}'"

    if dtype == "datetime":
        if isinstance(val, datetime.datetime):
            return f"TIMESTAMP '{val.isoformat()}'"
        return f"TIMESTAMP '{val}'"

    # Fallback: treat as string
    escaped = str(val).replace("'", "''")
    return f"'{escaped}'"


class PostgresEngine(EngineBase):
    """Generates SQL WHERE clause fragments and full SELECT statements for PostgreSQL."""

    def __init__(self, conn_string: Optional[str] = None) -> None:
        """*conn_string* is a libpq connection string; may be None for plan-only usage."""
        self._conn_string = conn_string

    @property
    def name(self) -> str:
        return "postgres"

    @property
    def capabilities(self) -> Set[EngineCapability]:
        return {
            EngineCapability.COMPARISON,
            EngineCapability.IN,
            EngineCapability.BETWEEN,
            EngineCapability.LIKE,
            EngineCapability.IS_NULL,
            EngineCapability.AND,
            EngineCapability.OR,
            EngineCapability.NOT,
        }

    def translate_predicate(self, pred: Predicate) -> str:
        """Translate a predicate to a SQL WHERE clause fragment."""

        if isinstance(pred, ComparisonPredicate):
            col = _quote_identifier(pred.column.column)
            op = _OP_SQL[pred.op]
            val = format_value(pred.value)
            return f"{col} {op} {val}"

        if isinstance(pred, InPredicate):
            col = _quote_identifier(pred.column.column)
            vals = ", ".join(format_value(v) for v in pred.values)
            kw = "NOT IN" if pred.negated else "IN"
            return f"{col} {kw} ({vals})"

        if isinstance(pred, BetweenPredicate):
            col = _quote_identifier(pred.column.column)
            lo = format_value(pred.low)
            hi = format_value(pred.high)
            kw = "NOT BETWEEN" if pred.negated else "BETWEEN"
            return f"{col} {kw} {lo} AND {hi}"

        if isinstance(pred, LikePredicate):
            col = _quote_identifier(pred.column.column)
            escaped_pattern = pred.pattern.replace("'", "''")
            kw = "NOT LIKE" if pred.negated else "LIKE"
            return f"{col} {kw} '{escaped_pattern}'"

        if isinstance(pred, IsNullPredicate):
            col = _quote_identifier(pred.column.column)
            return f"{col} IS NOT NULL" if pred.negated else f"{col} IS NULL"

        if isinstance(pred, AndPredicate):
            parts = [self.translate_predicate(p) for p in pred.predicates]
            return "(" + " AND ".join(parts) + ")"

        if isinstance(pred, OrPredicate):
            parts = [self.translate_predicate(p) for p in pred.predicates]
            return "(" + " OR ".join(parts) + ")"

        if isinstance(pred, NotPredicate):
            inner = self.translate_predicate(pred.predicate)
            return f"NOT ({inner})"

        raise ValueError(f"Unsupported predicate type: {type(pred).__name__}")

    def build_select_sql(
        self,
        table_name: str,
        pushed_result: PushdownResult,
        columns: List[str],
        schema: str = "public",
    ) -> str:
        """Build a complete SELECT statement from a pushdown result."""
        if columns:
            col_list = ", ".join(_quote_identifier(c) for c in columns)
        else:
            col_list = "*"

        qualified = f"{_quote_identifier(schema)}.{_quote_identifier(table_name)}"
        sql = f"SELECT {col_list} FROM {qualified}"

        where_fragment: Optional[str] = pushed_result.native_filter
        if where_fragment:
            sql += f" WHERE {where_fragment}"

        return sql

    def partial_index_hint(
        self, pred: Predicate, available_indexes: List[Dict[str, Any]]
    ) -> Optional[str]:
        """Return the name of a partial index whose predicate matches *pred*, if any.

        *available_indexes* is a list of dicts with keys:
            - name (str): index name
            - predicate (str): the partial index WHERE clause, e.g. "status = 'active'"
            - columns (List[str]): indexed columns

        This is a best-effort heuristic: we match the SQL translation of *pred* against
        the stored index predicate string.
        """
        try:
            pred_sql = self.translate_predicate(pred).lower().strip()
        except Exception:
            return None

        # Also build an unquoted version for matching against plain-text index predicates
        pred_sql_unquoted = pred_sql.replace('"', "")

        for idx in available_indexes:
            idx_pred = idx.get("predicate", "").lower().strip()
            if not idx_pred:
                continue
            # Match against both the quoted and unquoted form of the translated predicate
            if idx_pred in pred_sql or idx_pred in pred_sql_unquoted:
                return idx.get("name")
        return None

    def execute_scan(
        self, table_name: str, pushed_result: PushdownResult, columns: List[str]
    ) -> Any:
        """Execute a SELECT against PostgreSQL."""
        if self._conn_string is None:
            raise RuntimeError("PostgresEngine requires a conn_string to execute scans")
        try:
            import psycopg2
            import psycopg2.extras
        except ImportError as exc:
            raise ImportError(
                "psycopg2-binary is required; install with: pip install psycopg2-binary"
            ) from exc

        sql = self.build_select_sql(table_name, pushed_result, columns)
        conn = psycopg2.connect(self._conn_string)
        try:
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                cur.execute(sql)
                return [dict(row) for row in cur.fetchall()]
        finally:
            conn.close()
