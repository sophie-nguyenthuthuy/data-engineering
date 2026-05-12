"""S3 / local Parquet connector — uses PyArrow for predicate & projection pushdown."""

from __future__ import annotations

from pathlib import Path
from typing import Any

import pandas as pd
import sqlglot.expressions as exp

from .base import BaseConnector, ConnectorResult


class S3ParquetConnector(BaseConnector):
    """
    Reads Parquet files from S3 (s3://bucket/prefix/) or local paths.

    Predicate pushdown is translated to PyArrow ``dataset.Scanner`` filters,
    which prune row-groups before any data is decoded.
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
        return 500_000   # Parquet tables tend to be large

    # ------------------------------------------------------------------ #

    def _fetch_real(
        self,
        table: str,
        columns: list[str],
        predicates: list[exp.Expression],
        limit: int | None,
        params: dict[str, Any],
    ) -> ConnectorResult:
        import pyarrow.dataset as ds

        path = params.get("path") or params.get("s3_path") or table
        filesystem = self._build_filesystem(params)

        dataset = ds.dataset(path, filesystem=filesystem, format="parquet")

        arrow_filter = _predicates_to_arrow(predicates)
        scanner = dataset.scanner(
            columns=columns or None,
            filter=arrow_filter,
        )
        table_arrow = scanner.to_table()

        df = table_arrow.to_pandas()
        if limit:
            df = df.head(limit)

        return ConnectorResult(
            data=df.reset_index(drop=True),
            rows_scanned=table_arrow.num_rows,
            rows_returned=len(df),
            source="s3_parquet",
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
            source="s3_parquet",
            table=table,
        )

    @staticmethod
    def _build_filesystem(params: dict[str, Any]):
        path = params.get("path", "")
        if path.startswith("s3://") or params.get("bucket"):
            import pyarrow.fs as pafs

            region = params.get("region", "us-east-1")
            access_key = params.get("access_key")
            secret_key = params.get("secret_key")
            if access_key and secret_key:
                return pafs.S3FileSystem(
                    access_key=access_key,
                    secret_key=secret_key,
                    region=region,
                )
            return pafs.S3FileSystem(region=region)
        return None   # local filesystem


# --------------------------------------------------------------------------- #
# PyArrow filter expression builder                                            #
# --------------------------------------------------------------------------- #

def _predicates_to_arrow(predicates: list[exp.Expression]):
    """Convert sqlglot predicates to a PyArrow compute expression."""
    if not predicates:
        return None
    try:
        import pyarrow.compute as pc

        parts = [_expr_to_arrow(p) for p in predicates]
        parts = [p for p in parts if p is not None]
        if not parts:
            return None
        result = parts[0]
        for part in parts[1:]:
            result = result & part
        return result
    except Exception:
        return None


def _expr_to_arrow(expr: exp.Expression):
    try:
        import pyarrow.compute as pc
        import pyarrow as pa

        def field(e): return pc.field(e.name)
        def lit(e): return _lit(e)

        match expr:
            case exp.EQ(this=c, expression=v) if isinstance(c, exp.Column):
                return field(c) == lit(v)
            case exp.NEQ(this=c, expression=v) if isinstance(c, exp.Column):
                return field(c) != lit(v)
            case exp.GT(this=c, expression=v) if isinstance(c, exp.Column):
                return field(c) > lit(v)
            case exp.GTE(this=c, expression=v) if isinstance(c, exp.Column):
                return field(c) >= lit(v)
            case exp.LT(this=c, expression=v) if isinstance(c, exp.Column):
                return field(c) < lit(v)
            case exp.LTE(this=c, expression=v) if isinstance(c, exp.Column):
                return field(c) <= lit(v)
            case exp.In(this=c, expressions=vals) if isinstance(c, exp.Column):
                return pc.is_in(field(c), pa.array([lit(v) for v in vals]))
            case exp.And(this=left, expression=right):
                l, r = _expr_to_arrow(left), _expr_to_arrow(right)
                return (l & r) if l is not None and r is not None else (l or r)
            case exp.Or(this=left, expression=right):
                l, r = _expr_to_arrow(left), _expr_to_arrow(right)
                return (l | r) if l is not None and r is not None else (l or r)
    except Exception:
        pass
    return None


def _lit(expr: exp.Expression) -> Any:
    if isinstance(expr, exp.Literal):
        return expr.to_py()
    return str(expr)
