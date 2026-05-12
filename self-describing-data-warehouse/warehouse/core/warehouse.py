"""
SelfDescribingWarehouse — the top-level facade.
Combines the metadata registry, quality, freshness, usage, lineage,
incidents, and recommender into one coherent API.
"""

import sqlite3
import time
from pathlib import Path
from typing import Optional

from warehouse.schema.metadata_schema import METADATA_SCHEMA
from warehouse.core.registry import MetadataRegistry, TableMeta
from warehouse.core.lineage import LineageTracker
from warehouse.core.quality import QualityScorer
from warehouse.core.freshness import FreshnessMonitor
from warehouse.core.usage import UsageTracker
from warehouse.core.incidents import IncidentTracker
from warehouse.core.recommender import TableRecommender


class SelfDescribingWarehouse:
    """
    A SQLite-backed warehouse where every table is queryable both for its
    data and for its own metadata — lineage, quality, freshness, usage, and
    incident history.
    """

    def __init__(self, db_path: str = ":memory:"):
        self.db_path = db_path
        self.conn = sqlite3.connect(db_path, check_same_thread=False)
        self.conn.row_factory = sqlite3.Row
        self.conn.execute("PRAGMA journal_mode=WAL")
        self._bootstrap()

        self.registry  = MetadataRegistry(self.conn)
        self.lineage   = LineageTracker(self.conn)
        self.quality   = QualityScorer(self.conn)
        self.freshness = FreshnessMonitor(self.conn)
        self.usage     = UsageTracker(self.conn)
        self.incidents = IncidentTracker(self.conn)
        self.recommender = TableRecommender(self.conn)

    # ------------------------------------------------------------------ #
    #  Bootstrap                                                           #
    # ------------------------------------------------------------------ #

    def _bootstrap(self) -> None:
        for stmt in METADATA_SCHEMA.strip().split(";"):
            stmt = stmt.strip()
            if stmt:
                self.conn.execute(stmt)
        self.conn.commit()

    # ------------------------------------------------------------------ #
    #  Data operations (query tracking built-in)                          #
    # ------------------------------------------------------------------ #

    def execute(
        self,
        sql: str,
        params: tuple = (),
        user: str = "anonymous",
    ) -> list[dict]:
        """Run any SQL and automatically record usage for every table touched."""
        tables = self._extract_tables(sql)
        start = time.monotonic()
        rows = self.conn.execute(sql, params).fetchall()
        elapsed_ms = int((time.monotonic() - start) * 1000)
        for t in tables:
            if self._is_registered(t):
                self.usage.record(t, queried_by=user, query=sql, execution_ms=elapsed_ms)
        return [dict(r) for r in rows]

    def create_table(self, ddl: str) -> None:
        """Execute a CREATE TABLE statement."""
        self.conn.execute(ddl)
        self.conn.commit()

    def insert_many(self, table_name: str, rows: list[dict]) -> None:
        if not rows:
            return
        cols = list(rows[0].keys())
        placeholders = ", ".join("?" * len(cols))
        col_list = ", ".join(f"[{c}]" for c in cols)
        self.conn.executemany(
            f"INSERT INTO [{table_name}] ({col_list}) VALUES ({placeholders})",
            [tuple(r[c] for c in cols) for r in rows],
        )
        self.conn.commit()

    # ------------------------------------------------------------------ #
    #  Self-describing queries                                             #
    # ------------------------------------------------------------------ #

    def describe(self, table_name: str) -> dict:
        """Everything known about a table — metadata + latest quality/freshness/usage."""
        meta = self.registry.get_table(table_name)
        if not meta:
            return {"error": f"Table '{table_name}' is not registered."}
        meta["quality"]     = self.quality.latest(table_name)
        meta["freshness"]   = self.freshness.latest(table_name)
        meta["usage"]       = self.usage.stats(table_name)
        meta["top_users"]   = self.usage.top_users(table_name)
        meta["lineage"]     = {
            "upstream":   self.lineage.upstream(table_name),
            "downstream": self.lineage.downstream(table_name),
        }
        meta["last_incident"] = self.incidents.last_incident(table_name)
        meta["open_incidents"] = self.incidents.open_incidents(table_name)
        return meta

    def recommend(
        self,
        query: str,
        domain: Optional[str] = None,
        top_k: int = 5,
    ):
        """Answer: 'which table should I use for X?'"""
        return self.recommender.recommend(query, domain=domain, top_k=top_k)

    def catalog(self, domain: Optional[str] = None) -> list[dict]:
        """List all registered (non-deprecated) tables."""
        return self.registry.list_tables(domain=domain)

    def health_dashboard(self) -> list[dict]:
        """Return a per-table health summary across all registered tables."""
        tables = self.registry.list_tables(include_deprecated=False)
        rows = []
        for t in tables:
            name = t["table_name"]
            q = self.quality.latest(name)
            f = self.freshness.latest(name)
            u = self.usage.stats(name)
            open_inc = len(self.incidents.open_incidents(name))
            rows.append({
                "table_name":    name,
                "domain":        t["domain"],
                "quality_score": q["quality_score"] if q else None,
                "freshness_score": f["freshness_score"] if f else None,
                "total_queries": u.get("total_queries", 0),
                "last_queried":  u.get("last_queried_at"),
                "open_incidents": open_inc,
                "trend":         self.quality.trend(name),
            })
        rows.sort(key=lambda r: (r["quality_score"] or 0), reverse=True)
        return rows

    # ------------------------------------------------------------------ #
    #  Helpers                                                             #
    # ------------------------------------------------------------------ #

    def _extract_tables(self, sql: str) -> list[str]:
        import re
        tokens = re.findall(r"\b(?:FROM|JOIN|INTO|UPDATE)\s+\[?(\w+)\]?", sql, re.IGNORECASE)
        return list(set(tokens))

    def _is_registered(self, table_name: str) -> bool:
        row = self.conn.execute(
            "SELECT 1 FROM meta_tables WHERE table_name=?", (table_name,)
        ).fetchone()
        return row is not None

    def close(self) -> None:
        self.conn.close()
