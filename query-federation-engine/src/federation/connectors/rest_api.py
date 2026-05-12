"""REST API connector — maps SQL predicates to query-string parameters."""

from __future__ import annotations

from typing import Any

import pandas as pd
import sqlglot.expressions as exp

from .base import BaseConnector, ConnectorResult


class RestApiConnector(BaseConnector):
    """
    Fetches data from a REST endpoint that returns JSON (array of objects).

    Predicate pushdown strategy
    ---------------------------
    Simple equality / comparison predicates on columns whose names appear in
    ``param_map`` (connection config) are forwarded as query-string parameters.
    Any predicates that cannot be forwarded are applied in-process after fetch.

    Example connection config::

        {
          "base_url": "https://api.example.com/v1",
          "endpoint": "/orders",
          "param_map": {"status": "status", "user_id": "user_id"},
          "headers": {"Authorization": "Bearer TOKEN"},
          "result_path": "data.items",   # dot-path into response JSON
          "page_size": 100,
          "page_param": "page",
          "limit_param": "limit"
        }
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
        return 10_000   # REST APIs are assumed small / paginated

    # ------------------------------------------------------------------ #

    def _fetch_real(
        self,
        table: str,
        columns: list[str],
        predicates: list[exp.Expression],
        limit: int | None,
        params: dict[str, Any],
    ) -> ConnectorResult:
        import httpx

        base_url = params.get("base_url", "")
        endpoint = params.get("endpoint", f"/{table}")
        headers = params.get("headers", {})
        param_map: dict[str, str] = params.get("param_map", {})
        result_path: str = params.get("result_path", "")
        page_size: int = params.get("page_size", 100)
        page_param: str = params.get("page_param", "page")
        limit_param: str = params.get("limit_param", "limit")

        # Split predicates into pushable (equality on known params) vs. residual
        pushed_params, residual = self._split_predicates(predicates, param_map)

        url = base_url.rstrip("/") + "/" + endpoint.lstrip("/")
        query: dict[str, Any] = {**pushed_params, limit_param: page_size}

        all_rows: list[dict] = []
        page = 1
        with httpx.Client(headers=headers, timeout=30) as client:
            while True:
                query[page_param] = page
                resp = client.get(url, params=query)
                resp.raise_for_status()
                data = resp.json()
                rows = self._extract_result(data, result_path)
                if not rows:
                    break
                all_rows.extend(rows)
                if limit and len(all_rows) >= limit:
                    break
                if len(rows) < page_size:
                    break   # last page
                page += 1

        df = pd.DataFrame(all_rows)
        from .postgres import _apply_predicates
        df = _apply_predicates(df, residual)
        if columns:
            available = [c for c in columns if c in df.columns]
            if available:
                df = df[available]
        if limit:
            df = df.head(limit)

        return ConnectorResult(
            data=df.reset_index(drop=True),
            rows_scanned=len(all_rows),
            rows_returned=len(df),
            source="rest_api",
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
        original_len = len(df)
        df = _apply_predicates(df, predicates)
        if columns:
            available = [c for c in columns if c in df.columns]
            if available:
                df = df[available]
        if limit:
            df = df.head(limit)
        return ConnectorResult(
            data=df.reset_index(drop=True),
            rows_scanned=original_len,
            rows_returned=len(df),
            source="rest_api",
            table=table,
        )

    # ------------------------------------------------------------------ #
    # Helpers                                                              #
    # ------------------------------------------------------------------ #

    def _split_predicates(
        self,
        predicates: list[exp.Expression],
        param_map: dict[str, str],
    ) -> tuple[dict[str, Any], list[exp.Expression]]:
        """Return (query_params_dict, residual_predicates)."""
        pushed: dict[str, Any] = {}
        residual: list[exp.Expression] = []

        for pred in predicates:
            if isinstance(pred, exp.EQ) and isinstance(pred.this, exp.Column):
                col_name = pred.this.name
                if col_name in param_map:
                    pushed[param_map[col_name]] = _lit(pred.expression)
                    continue
            residual.append(pred)

        return pushed, residual

    @staticmethod
    def _extract_result(data: Any, result_path: str) -> list[dict]:
        """Navigate a dot-path like 'data.items' into the response JSON."""
        if not result_path:
            if isinstance(data, list):
                return data
            return [data] if isinstance(data, dict) else []

        for key in result_path.split("."):
            if isinstance(data, dict):
                data = data.get(key, [])
            else:
                return []
        return data if isinstance(data, list) else []


def _lit(expr: exp.Expression) -> Any:
    if isinstance(expr, exp.Literal):
        return expr.to_py()
    return str(expr)
