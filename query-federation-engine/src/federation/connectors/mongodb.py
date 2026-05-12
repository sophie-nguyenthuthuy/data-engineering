"""MongoDB connector — translates pushed predicates into MQL filter documents."""

from __future__ import annotations

from typing import Any

import pandas as pd
import sqlglot.expressions as exp

from .base import BaseConnector, ConnectorResult


class MongoDBConnector(BaseConnector):

    def __init__(self) -> None:
        self._mock_data: dict[str, pd.DataFrame] = {}

    def set_mock(self, collection: str, df: pd.DataFrame) -> None:
        self._mock_data[collection] = df

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
        return 50_000

    # ------------------------------------------------------------------ #

    def _fetch_real(
        self,
        table: str,
        columns: list[str],
        predicates: list[exp.Expression],
        limit: int | None,
        params: dict[str, Any],
    ) -> ConnectorResult:
        from pymongo import MongoClient

        uri = params.get("uri", "mongodb://localhost:27017")
        dbname = params.get("database", "test")
        client = MongoClient(uri)
        db = client[dbname]
        collection = db[table]

        mongo_filter = self._predicates_to_mql(predicates)
        projection = {c: 1 for c in columns} if columns else {}
        if projection:
            projection["_id"] = 0

        cursor = collection.find(mongo_filter, projection)
        if limit:
            cursor = cursor.limit(limit)

        rows = list(cursor)
        client.close()

        df = pd.DataFrame(rows)
        return ConnectorResult(
            data=df,
            rows_scanned=len(df),
            rows_returned=len(df),
            source="mongodb",
            table=table,
        )

    def _fetch_mock(
        self,
        table: str,
        columns: list[str],
        predicates: list[exp.Expression],
        limit: int | None,
    ) -> ConnectorResult:
        from .postgres import _apply_predicates

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
            source="mongodb",
            table=table,
        )

    def _predicates_to_mql(self, predicates: list[exp.Expression]) -> dict[str, Any]:
        """Convert sqlglot predicates to a MongoDB query filter document."""
        if not predicates:
            return {}
        filters: list[dict] = [_expr_to_mql(p) for p in predicates]
        if len(filters) == 1:
            return filters[0]
        return {"$and": filters}


def _expr_to_mql(expr: exp.Expression) -> dict[str, Any]:
    match expr:
        case exp.EQ(this=col, expression=val) if isinstance(col, exp.Column):
            return {col.name: {"$eq": _lit(val)}}
        case exp.NEQ(this=col, expression=val) if isinstance(col, exp.Column):
            return {col.name: {"$ne": _lit(val)}}
        case exp.GT(this=col, expression=val) if isinstance(col, exp.Column):
            return {col.name: {"$gt": _lit(val)}}
        case exp.GTE(this=col, expression=val) if isinstance(col, exp.Column):
            return {col.name: {"$gte": _lit(val)}}
        case exp.LT(this=col, expression=val) if isinstance(col, exp.Column):
            return {col.name: {"$lt": _lit(val)}}
        case exp.LTE(this=col, expression=val) if isinstance(col, exp.Column):
            return {col.name: {"$lte": _lit(val)}}
        case exp.Like(this=col, expression=val) if isinstance(col, exp.Column):
            pattern = str(_lit(val)).replace("%", ".*").replace("_", ".")
            return {col.name: {"$regex": pattern}}
        case exp.In(this=col, expressions=vals) if isinstance(col, exp.Column):
            return {col.name: {"$in": [_lit(v) for v in vals]}}
        case exp.Is(this=col, expression=exp.Null()) if isinstance(col, exp.Column):
            return {col.name: {"$exists": False}}
        case exp.And(this=left, expression=right):
            return {"$and": [_expr_to_mql(left), _expr_to_mql(right)]}
        case exp.Or(this=left, expression=right):
            return {"$or": [_expr_to_mql(left), _expr_to_mql(right)]}
        case exp.Not(this=inner):
            return {"$nor": [_expr_to_mql(inner)]}
        case _:
            return {}


def _lit(expr: exp.Expression) -> Any:
    if isinstance(expr, exp.Literal):
        return expr.to_py()
    return str(expr)
