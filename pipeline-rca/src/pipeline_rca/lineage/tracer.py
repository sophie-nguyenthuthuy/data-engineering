"""
Lineage tracer: maps downstream metrics → upstream tables/columns.

Supports two lineage sources:
1. Static YAML config (simple, no external deps).
2. SQLLineage-based SQL parsing (optional; install sqllineage package).

The tracer returns the set of upstream tables/columns that should be
examined as candidate root causes for a given metric degradation.
"""

from __future__ import annotations

import logging
import re
from dataclasses import dataclass, field
from typing import Any

logger = logging.getLogger(__name__)

_SQLLINEAGE_AVAILABLE = False
try:
    import sqllineage  # noqa: F401
    _SQLLINEAGE_AVAILABLE = True
except ImportError:
    pass


@dataclass
class LineageNode:
    name: str                               # fully qualified: "schema.table" or "schema.table.column"
    kind: str = "table"                     # "table" | "column"
    upstream: list["LineageNode"] = field(default_factory=list)


@dataclass
class LineageGraph:
    """A lightweight DAG representing metric → tables → columns lineage."""

    metric_to_tables: dict[str, list[str]] = field(default_factory=dict)
    table_to_columns: dict[str, list[str]] = field(default_factory=dict)

    def upstream_tables(self, metric: str) -> list[str]:
        return self.metric_to_tables.get(metric, [])

    def upstream_columns(self, table: str) -> list[str]:
        return self.table_to_columns.get(table, [])

    def all_upstream_nodes(self, metric: str) -> list[str]:
        """Return all table + column nodes reachable from *metric*."""
        nodes: list[str] = []
        for table in self.upstream_tables(metric):
            nodes.append(table)
            nodes.extend(f"{table}.{col}" for col in self.upstream_columns(table))
        return nodes


class LineageTracer:
    """
    Builds and queries a lineage graph.

    Usage
    -----
    tracer = LineageTracer()
    tracer.register_metric("daily_active_users", upstream_tables=["user_events", "sessions"])
    tracer.register_table_columns("user_events", ["user_id", "event_type", "created_at"])
    graph = tracer.graph
    """

    def __init__(self) -> None:
        self._graph = LineageGraph()

    @property
    def graph(self) -> LineageGraph:
        return self._graph

    def register_metric(
        self, metric_name: str, upstream_tables: list[str]
    ) -> None:
        self._graph.metric_to_tables[metric_name] = list(upstream_tables)
        logger.debug("Registered lineage for metric %s → %s", metric_name, upstream_tables)

    def register_table_columns(self, table: str, columns: list[str]) -> None:
        self._graph.table_to_columns[table] = list(columns)

    def from_config(self, config: dict[str, Any]) -> None:
        """Populate from the 'lineage' section of the YAML config."""
        metrics = config.get("metrics", [])
        upstream_tables = config.get("lineage", {}).get("upstream_tables", [])

        for metric_cfg in metrics:
            name = metric_cfg["name"]
            sql = metric_cfg.get("query", "")
            tables = _extract_tables_from_sql(sql) if sql else []
            # Supplement with explicit upstream_tables list
            tables = list(dict.fromkeys(tables + upstream_tables))
            self.register_metric(name, tables)

    def from_sql(self, metric_name: str, sql: str) -> None:
        """Parse a SQL query and register lineage for *metric_name*."""
        if _SQLLINEAGE_AVAILABLE:
            tables = _sqllineage_tables(sql)
        else:
            tables = _extract_tables_from_sql(sql)
        self.register_metric(metric_name, tables)
        logger.info(
            "Parsed lineage for %s: %d upstream tables (%s)",
            metric_name,
            len(tables),
            "sqllineage" if _SQLLINEAGE_AVAILABLE else "regex",
        )

    def candidate_nodes(self, metric_name: str) -> list[str]:
        """Return all upstream nodes (tables + columns) for a metric."""
        return self._graph.all_upstream_nodes(metric_name)


# ------------------------------------------------------------------
# SQL parsing helpers
# ------------------------------------------------------------------

_FROM_PATTERN = re.compile(
    r"""(?:FROM|JOIN)\s+([`"\[]?[\w.]+[`"\]]?)""",
    re.IGNORECASE,
)


def _extract_tables_from_sql(sql: str) -> list[str]:
    """Very lightweight regex extraction; good enough for simple warehouse queries."""
    matches = _FROM_PATTERN.findall(sql)
    seen: dict[str, None] = {}
    for m in matches:
        clean = m.strip('`"[]{}')
        # Skip obvious non-tables: subquery aliases, CTEs with no dot
        if clean and not clean.upper() in ("SELECT", "WHERE", "GROUP", "ORDER"):
            seen[clean] = None
    return list(seen)


def _sqllineage_tables(sql: str) -> list[str]:
    """Use sqllineage for more accurate extraction when available."""
    from sqllineage.runner import LineageRunner  # type: ignore[import]

    runner = LineageRunner(sql)
    return [str(t) for t in runner.source_tables()]
