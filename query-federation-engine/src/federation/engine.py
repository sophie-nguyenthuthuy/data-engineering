"""
FederationEngine — the single public entry point.

Usage
-----
    engine = FederationEngine.from_yaml("config/catalog.yaml")
    df, stats = engine.query("SELECT u.name, o.total FROM postgres.orders o JOIN mongodb.users u ON o.user_id = u.id")
    print(df)
    print(stats.summary())
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

import pandas as pd

from .catalog import SchemaCatalog, SourceType, TableSchema, ColumnDef
from .connectors import (
    BaseConnector,
    MongoDBConnector, PostgresConnector,
    RestApiConnector, S3ParquetConnector,
)
from .executor import ExecutionStats, Executor
from .planner import CostBasedOptimizer, QueryPlanner, explain_plan
from .planner.nodes import PlanNode


_SOURCE_CONNECTOR_MAP = {
    SourceType.POSTGRES:   PostgresConnector,
    SourceType.MONGODB:    MongoDBConnector,
    SourceType.S3_PARQUET: S3ParquetConnector,
    SourceType.REST_API:   RestApiConnector,
}


class FederationEngine:
    """
    Orchestrates: parse → plan → optimize → execute.

    Parameters
    ----------
    catalog : SchemaCatalog
        Registered tables and source connections.
    connectors : dict[str, BaseConnector], optional
        Override connectors per source name (useful for testing / mocking).
    max_workers : int
        Thread-pool size for parallel source scans.
    """

    def __init__(
        self,
        catalog: SchemaCatalog,
        connectors: dict[str, BaseConnector] | None = None,
        max_workers: int = 8,
    ) -> None:
        self.catalog = catalog
        self._planner = QueryPlanner(catalog)
        self._optimizer = CostBasedOptimizer()
        self._executor = Executor(catalog, connectors, max_workers)

    # ------------------------------------------------------------------ #
    # Factory constructors                                                 #
    # ------------------------------------------------------------------ #

    @classmethod
    def from_yaml(cls, path: str | Path, **kwargs: Any) -> "FederationEngine":
        catalog = SchemaCatalog.from_yaml(path)
        return cls(catalog, **kwargs)

    @classmethod
    def from_catalog(cls, catalog: SchemaCatalog, **kwargs: Any) -> "FederationEngine":
        return cls(catalog, **kwargs)

    # ------------------------------------------------------------------ #
    # Core API                                                             #
    # ------------------------------------------------------------------ #

    def query(self, sql: str) -> tuple[pd.DataFrame, ExecutionStats]:
        """
        Execute a federated SQL query.

        Returns
        -------
        (DataFrame of results, ExecutionStats)
        """
        plan = self._build_plan(sql)
        df = self._executor.execute(plan)
        return df, self._executor.stats

    def explain(self, sql: str) -> str:
        """Return a human-readable query plan without executing it."""
        plan = self._build_plan(sql)
        return explain_plan(plan)

    def register_connector(self, source_name: str, connector: BaseConnector) -> None:
        """Override the connector used for a specific source (e.g. for testing)."""
        self._executor.register_connector(source_name, connector)

    # ------------------------------------------------------------------ #
    # Internal                                                             #
    # ------------------------------------------------------------------ #

    def _build_plan(self, sql: str) -> PlanNode:
        logical = self._planner.build(sql)
        optimized = self._optimizer.optimize(logical)
        return optimized

    # ------------------------------------------------------------------ #
    # Convenience: register an in-memory mock table                       #
    # ------------------------------------------------------------------ #

    def register_mock_table(
        self,
        source: str,
        table: str,
        source_type: SourceType,
        df: pd.DataFrame,
        connection: dict[str, Any] | None = None,
    ) -> None:
        """Register an in-memory DataFrame as a federated table (for demos/tests)."""
        columns = [
            ColumnDef(name=col, dtype=_pandas_dtype_to_str(df[col].dtype))
            for col in df.columns
        ]
        schema = TableSchema(
            source=source,
            table=table,
            source_type=source_type,
            columns=columns,
            estimated_rows=len(df),
            connection=connection or {},
        )
        # Register source if not present
        if source not in self.catalog._sources:
            self.catalog.register_source(source, source_type, connection or {})
        self.catalog.register_table(schema)

        # Wire up mock connector
        connector = self._executor._connectors.get(source)
        if connector is None:
            connector = _SOURCE_CONNECTOR_MAP[source_type]()
            self._executor.register_connector(source, connector)
        if hasattr(connector, "set_mock"):
            connector.set_mock(table, df)


def _pandas_dtype_to_str(dtype) -> str:
    name = str(dtype)
    if "int" in name:
        return "int"
    if "float" in name:
        return "float"
    if "bool" in name:
        return "bool"
    if "datetime" in name:
        return "timestamp"
    return "string"
