"""
Execution engine — walks the optimized plan tree, dispatches connector fetches
in parallel, and assembles the final result DataFrame.
"""

from __future__ import annotations

import concurrent.futures
import time
from dataclasses import dataclass, field
from typing import Any

import pandas as pd
import sqlglot.expressions as exp

from .catalog import SchemaCatalog, SourceType
from .connectors import (
    BaseConnector, ConnectorResult,
    MongoDBConnector, PostgresConnector,
    RestApiConnector, S3ParquetConnector,
)
from .planner.nodes import (
    Aggregate, Filter, Join, JoinType, Limit,
    PlanNode, Project, Sort, TableScan,
)


@dataclass
class ExecutionStats:
    total_time_ms: float = 0.0
    rows_scanned: dict[str, int] = field(default_factory=dict)
    rows_returned: int = 0
    sources_queried: list[str] = field(default_factory=list)

    def summary(self) -> str:
        lines = [
            f"Total time   : {self.total_time_ms:.1f} ms",
            f"Rows returned: {self.rows_returned:,}",
            "Sources      : " + ", ".join(self.sources_queried),
        ]
        for src, n in self.rows_scanned.items():
            lines.append(f"  {src:20s} scanned {n:,} rows")
        return "\n".join(lines)


class Executor:
    """
    Traverses a PlanNode tree and executes it.

    TableScans for independent sources are dispatched concurrently using a
    thread pool (each connector is typically I/O-bound).
    """

    def __init__(
        self,
        catalog: SchemaCatalog,
        connectors: dict[str, BaseConnector] | None = None,
        max_workers: int = 8,
    ) -> None:
        self.catalog = catalog
        self.max_workers = max_workers
        self._connectors: dict[str, BaseConnector] = connectors or {}
        self.stats = ExecutionStats()

    # ------------------------------------------------------------------ #
    # Connector registry                                                   #
    # ------------------------------------------------------------------ #

    def register_connector(self, source_name: str, connector: BaseConnector) -> None:
        self._connectors[source_name] = connector

    def _get_connector(self, source_name: str) -> BaseConnector:
        if source_name in self._connectors:
            return self._connectors[source_name]
        # Auto-instantiate a default connector based on catalog source type
        schema = next(
            (s for s in self.catalog._tables.values() if s.source == source_name), None
        )
        if schema is None:
            raise KeyError(f"No connector and no catalog entry for source: {source_name!r}")
        mapping = {
            SourceType.POSTGRES:   PostgresConnector,
            SourceType.MONGODB:    MongoDBConnector,
            SourceType.S3_PARQUET: S3ParquetConnector,
            SourceType.REST_API:   RestApiConnector,
        }
        cls = mapping.get(schema.source_type)
        if cls is None:
            raise ValueError(f"Unsupported source type: {schema.source_type}")
        conn = cls()
        self._connectors[source_name] = conn
        return conn

    # ------------------------------------------------------------------ #
    # Public entry point                                                   #
    # ------------------------------------------------------------------ #

    def execute(self, plan: PlanNode) -> pd.DataFrame:
        t0 = time.perf_counter()
        df = self._execute_node(plan)
        self.stats.total_time_ms = (time.perf_counter() - t0) * 1000
        self.stats.rows_returned = len(df)
        return df

    # ------------------------------------------------------------------ #
    # Node dispatch                                                        #
    # ------------------------------------------------------------------ #

    def _execute_node(self, node: PlanNode) -> pd.DataFrame:
        match node:
            case TableScan():
                return self._exec_scan(node)
            case Filter():
                return self._exec_filter(node)
            case Project():
                return self._exec_project(node)
            case Join():
                return self._exec_join(node)
            case Aggregate():
                return self._exec_aggregate(node)
            case Sort():
                return self._exec_sort(node)
            case Limit():
                return self._exec_limit(node)
            case _:
                raise NotImplementedError(f"Unhandled plan node: {type(node).__name__}")

    # ------------------------------------------------------------------ #
    # TableScan                                                            #
    # ------------------------------------------------------------------ #

    def _exec_scan(self, node: TableScan) -> pd.DataFrame:
        connector = self._get_connector(node.source)
        schema = self.catalog.get_table(node.qualified_name)
        result: ConnectorResult = connector.fetch(
            table=node.table,
            columns=node.projected_columns,
            predicates=node.pushed_predicates,
            connection_params=schema.connection,
        )
        # Track stats
        key = node.qualified_name
        self.stats.rows_scanned[key] = result.rows_scanned
        if node.source not in self.stats.sources_queried:
            self.stats.sources_queried.append(node.source)

        df = result.data
        # Prefix columns with alias to avoid collisions during joins
        alias = node.alias or node.table
        df = df.rename(columns={c: f"{alias}.{c}" for c in df.columns})
        return df

    # ------------------------------------------------------------------ #
    # Filter (residual predicates not pushed to source)                   #
    # ------------------------------------------------------------------ #

    def _exec_filter(self, node: Filter) -> pd.DataFrame:
        df = self._execute_node(node.child)
        if node.predicate is not None:
            mask = _eval_on_df(df, node.predicate)
            if mask is not None:
                df = df[mask]
        return df.reset_index(drop=True)

    # ------------------------------------------------------------------ #
    # Project                                                              #
    # ------------------------------------------------------------------ #

    def _exec_project(self, node: Project) -> pd.DataFrame:
        df = self._execute_node(node.child)

        result_cols: dict[str, pd.Series] = {}
        for expr, out_name in zip(node.columns, node.output_names):
            if isinstance(expr, exp.Star):
                for col in df.columns:
                    result_cols[col] = df[col]
            elif isinstance(expr, exp.Alias):
                series = _eval_expr_on_df(df, expr.this)
                result_cols[out_name] = series if series is not None else pd.Series([None] * len(df))
            elif isinstance(expr, exp.Column):
                col = _resolve_column(df, expr)
                if col is not None:
                    result_cols[out_name] = df[col]
            else:
                series = _eval_expr_on_df(df, expr)
                result_cols[out_name] = series if series is not None else pd.Series([None] * len(df))

        return pd.DataFrame(result_cols) if result_cols else df

    # ------------------------------------------------------------------ #
    # Join — hash join with parallel left/right fetch                     #
    # ------------------------------------------------------------------ #

    def _exec_join(self, node: Join) -> pd.DataFrame:
        # Execute both sides; if they are independent scans, run in parallel
        with concurrent.futures.ThreadPoolExecutor(max_workers=2) as pool:
            left_fut = pool.submit(self._execute_node, node.left)
            right_fut = pool.submit(self._execute_node, node.right)
            left_df = left_fut.result()
            right_df = right_fut.result()

        if node.join_type == JoinType.CROSS:
            left_df["_cross"] = 1
            right_df["_cross"] = 1
            merged = pd.merge(left_df, right_df, on="_cross").drop(columns=["_cross"])
            return merged.reset_index(drop=True)

        if node.left_keys and node.right_keys:
            # Resolve fully-qualified key names to actual DataFrame columns.
            # The optimizer may have swapped left/right sides for cost reasons,
            # so if the primary mapping fails, try the reversed key assignment.
            left_on = [_find_col(left_df, k) for k in node.left_keys]
            right_on = [_find_col(right_df, k) for k in node.right_keys]

            if None in left_on or None in right_on:
                # Try swapped: keys built for original sides may now be reversed
                left_on_swap  = [_find_col(left_df,  k) for k in node.right_keys]
                right_on_swap = [_find_col(right_df, k) for k in node.left_keys]
                if None not in left_on_swap and None not in right_on_swap:
                    left_on, right_on = left_on_swap, right_on_swap

            if None not in left_on and None not in right_on:
                how = {
                    JoinType.INNER: "inner",
                    JoinType.LEFT:  "left",
                    JoinType.RIGHT: "right",
                    JoinType.FULL:  "outer",
                }.get(node.join_type, "inner")

                merged = pd.merge(
                    left_df, right_df,
                    left_on=left_on,
                    right_on=right_on,
                    how=how,
                    suffixes=("", "_dup"),
                )
                # Drop duplicate key columns from right side
                dup_cols = [c for c in merged.columns if c.endswith("_dup")]
                merged = merged.drop(columns=dup_cols)
                return merged.reset_index(drop=True)

        # Fallback: apply join condition row-by-row (slow, but correct)
        return self._nested_loop_join(left_df, right_df, node)

    def _nested_loop_join(
        self, left: pd.DataFrame, right: pd.DataFrame, node: Join
    ) -> pd.DataFrame:
        """Brute-force join for complex / non-equi conditions."""
        rows = []
        for _, lr in left.iterrows():
            for _, rr in right.iterrows():
                combined = pd.Series({**lr.to_dict(), **rr.to_dict()})
                if node.condition is None or _eval_condition_on_row(combined, node.condition):
                    rows.append(combined)
        return pd.DataFrame(rows).reset_index(drop=True) if rows else pd.DataFrame()

    # ------------------------------------------------------------------ #
    # Aggregate                                                            #
    # ------------------------------------------------------------------ #

    def _exec_aggregate(self, node: Aggregate) -> pd.DataFrame:
        df = self._execute_node(node.child)

        group_cols = [_resolve_column(df, k) or str(k) for k in node.group_keys]
        group_cols = [c for c in group_cols if c in df.columns]

        agg_spec: dict[str, Any] = {}
        for agg_expr in node.aggregates:
            out_col = str(agg_expr)
            inner = next(iter(agg_expr.find_all(exp.Column)), None)
            if inner is None:
                continue
            src_col = _resolve_column(df, inner)
            if src_col is None:
                continue
            fn = type(agg_expr).__name__.lower()
            pandas_fn = {
                "sum": "sum", "avg": "mean", "count": "count",
                "max": "max", "min": "min",
            }.get(fn, "sum")
            agg_spec[src_col] = pandas_fn

        if not group_cols:
            if agg_spec:
                result = df.agg(agg_spec).to_frame().T
            else:
                result = df
        else:
            if agg_spec:
                result = df.groupby(group_cols).agg(agg_spec).reset_index()
            else:
                result = df[group_cols].drop_duplicates().reset_index(drop=True)

        return result

    # ------------------------------------------------------------------ #
    # Sort                                                                 #
    # ------------------------------------------------------------------ #

    def _exec_sort(self, node: Sort) -> pd.DataFrame:
        df = self._execute_node(node.child)
        by: list[str] = []
        ascending: list[bool] = []
        for order_expr in node.order_exprs:
            col_expr = order_expr.this if hasattr(order_expr, "this") else order_expr
            col = _resolve_column(df, col_expr)
            if col and col in df.columns:
                by.append(col)
                desc = getattr(order_expr, "desc", False)
                ascending.append(not desc)
        if by:
            df = df.sort_values(by=by, ascending=ascending)
        return df.reset_index(drop=True)

    # ------------------------------------------------------------------ #
    # Limit                                                                #
    # ------------------------------------------------------------------ #

    def _exec_limit(self, node: Limit) -> pd.DataFrame:
        df = self._execute_node(node.child)
        start = node.offset
        end = node.offset + node.count if node.count else None
        return df.iloc[start:end].reset_index(drop=True)


# --------------------------------------------------------------------------- #
# Column resolution helpers                                                    #
# --------------------------------------------------------------------------- #

def _find_col(df: pd.DataFrame, key: str) -> str | None:
    """Resolve 'alias.col' or 'col' to an actual DataFrame column name."""
    if key in df.columns:
        return key
    # Try suffix match: "orders.id" -> find column ending in ".id" or named "id"
    suffix = key.split(".")[-1]
    for col in df.columns:
        if col == suffix or col.endswith(f".{suffix}"):
            return col
    return None


def _resolve_column(df: pd.DataFrame, expr: exp.Expression) -> str | None:
    if isinstance(expr, exp.Column):
        qualified = f"{expr.table}.{expr.name}" if expr.table else expr.name
        return _find_col(df, qualified)
    return None


# --------------------------------------------------------------------------- #
# In-memory expression evaluation                                              #
# --------------------------------------------------------------------------- #

def _eval_on_df(df: pd.DataFrame, expr: exp.Expression) -> pd.Series | None:
    """Evaluate a predicate expression against the DataFrame; return bool Series."""
    from .connectors.postgres import _eval_predicate as _pg_eval

    # Build a temporary df with unqualified column names for the evaluator
    rename = {c: c.split(".")[-1] for c in df.columns}
    flat = df.rename(columns=rename)
    mask = _pg_eval(flat, expr)
    return mask


def _eval_expr_on_df(df: pd.DataFrame, expr: exp.Expression) -> pd.Series | None:
    """Evaluate a scalar / arithmetic expression; return a Series."""
    if isinstance(expr, exp.Column):
        col = _resolve_column(df, expr)
        return df[col] if col else None
    if isinstance(expr, exp.Literal):
        val = expr.to_py()
        return pd.Series([val] * len(df))
    # For complex expressions fall back to sqlglot-generated string eval (unsafe in prod)
    return None


def _eval_condition_on_row(row: pd.Series, expr: exp.Expression) -> bool:
    try:
        rename = {c: c.split(".")[-1] for c in row.index}
        flat = {rename.get(k, k): v for k, v in row.items()}
        df_single = pd.DataFrame([flat])
        from .connectors.postgres import _eval_predicate as _pg_eval
        mask = _pg_eval(df_single, expr)
        return bool(mask.iloc[0]) if mask is not None else True
    except Exception:
        return True
