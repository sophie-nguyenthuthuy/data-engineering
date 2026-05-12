"""
Usage tracker — records every query against the warehouse and aggregates
popularity stats for the recommender.
"""

import sqlite3
from datetime import datetime, timezone
from typing import Optional


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


class UsageTracker:
    def __init__(self, conn: sqlite3.Connection):
        self.conn = conn

    def record(
        self,
        table_name: str,
        queried_by: str = "anonymous",
        query: str = "",
        execution_ms: int = 0,
    ) -> None:
        self.conn.execute(
            """
            INSERT INTO meta_usage
                (table_name, queried_at, queried_by, query_preview, execution_ms)
            VALUES (?, ?, ?, ?, ?)
            """,
            (table_name, _now(), queried_by, query[:200], execution_ms),
        )
        self.conn.commit()

    def stats(self, table_name: str) -> dict:
        row = self.conn.execute(
            """
            SELECT
                COUNT(*) as total_queries,
                COUNT(DISTINCT queried_by) as unique_users,
                MAX(queried_at) as last_queried_at,
                AVG(execution_ms) as avg_execution_ms
            FROM meta_usage
            WHERE table_name=?
            """,
            (table_name,),
        ).fetchone()
        return dict(row) if row else {}

    def top_tables(self, limit: int = 10) -> list[dict]:
        """Tables ranked by query frequency."""
        rows = self.conn.execute(
            """
            SELECT table_name, COUNT(*) as query_count,
                   COUNT(DISTINCT queried_by) as unique_users,
                   MAX(queried_at) as last_queried_at
            FROM meta_usage
            GROUP BY table_name
            ORDER BY query_count DESC
            LIMIT ?
            """,
            (limit,),
        ).fetchall()
        return [dict(r) for r in rows]

    def top_users(self, table_name: str, limit: int = 5) -> list[dict]:
        rows = self.conn.execute(
            """
            SELECT queried_by, COUNT(*) as query_count
            FROM meta_usage
            WHERE table_name=?
            GROUP BY queried_by
            ORDER BY query_count DESC
            LIMIT ?
            """,
            (table_name, limit),
        ).fetchall()
        return [dict(r) for r in rows]

    def recent_queries(self, table_name: str, limit: int = 5) -> list[dict]:
        rows = self.conn.execute(
            """
            SELECT queried_at, queried_by, query_preview, execution_ms
            FROM meta_usage
            WHERE table_name=?
            ORDER BY queried_at DESC LIMIT ?
            """,
            (table_name, limit),
        ).fetchall()
        return [dict(r) for r in rows]

    def usage_score(self, table_name: str) -> float:
        """
        0-100 popularity score.  High frequency + recent = high score.
        """
        stats = self.stats(table_name)
        total = stats.get("total_queries") or 0
        last = stats.get("last_queried_at")

        frequency_score = min(100, total * 5)   # cap at 20 queries → 100

        if last:
            try:
                last_dt = datetime.fromisoformat(last)
                if last_dt.tzinfo is None:
                    last_dt = last_dt.replace(tzinfo=timezone.utc)
                days_ago = (datetime.now(timezone.utc) - last_dt).days
                recency_score = max(0, 100 - days_ago * 10)
            except ValueError:
                recency_score = 0.0
        else:
            recency_score = 0.0

        return round(frequency_score * 0.6 + recency_score * 0.4, 2)
