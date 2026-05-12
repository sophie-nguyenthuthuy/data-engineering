"""DuckDB store for enriched events.

dlt owns the raw tables (created under the `github` schema). We own the
`enriched` schema: one row per notification with AI-derived fields plus a
materialized view that joins raw + enriched for the UI/CLI.
"""

from __future__ import annotations

from contextlib import contextmanager
from pathlib import Path
from typing import Any

import duckdb

from streamdigest.config import settings

ENRICHED_DDL = """
CREATE SCHEMA IF NOT EXISTS enriched;

CREATE TABLE IF NOT EXISTS enriched.notifications (
    id               VARCHAR PRIMARY KEY,
    summary          VARCHAR,
    actions          JSON,
    priority         INTEGER,          -- 1 (low) .. 5 (critical)
    priority_reason  VARCHAR,
    sentiment        VARCHAR,          -- positive / neutral / negative / mixed
    model            VARCHAR,
    enriched_at      TIMESTAMP DEFAULT current_timestamp,
    input_hash       VARCHAR           -- hash of the inputs used; skip re-enrich if unchanged
);

CREATE INDEX IF NOT EXISTS idx_enriched_priority
    ON enriched.notifications(priority DESC, enriched_at DESC);
"""

DIGEST_VIEW_DDL = """
CREATE OR REPLACE VIEW enriched.digest AS
SELECT
    n.id,
    n.repo_full_name,
    n.subject_type,
    n.subject_title,
    n.reason,
    n.updated_at,
    n.unread,
    e.summary,
    e.actions,
    e.priority,
    e.priority_reason,
    e.sentiment,
    e.model,
    e.enriched_at
FROM github.notifications n
LEFT JOIN enriched.notifications e USING (id);
"""


class Store:
    def __init__(self, path: Path | str):
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)

    @contextmanager
    def connect(self) -> Any:
        conn = duckdb.connect(str(self.path))
        try:
            yield conn
        finally:
            conn.close()

    def init_schema(self) -> None:
        with self.connect() as conn:
            conn.execute(ENRICHED_DDL)

    def ensure_digest_view(self) -> None:
        """Create the digest view if the underlying `github.notifications` exists."""
        with self.connect() as conn:
            exists = conn.execute(
                """
                SELECT COUNT(*) FROM information_schema.tables
                WHERE table_schema = 'github' AND table_name = 'notifications'
                """
            ).fetchone()[0]
            if exists:
                conn.execute(DIGEST_VIEW_DDL)

    def unenriched_notifications(self, limit: int = 50) -> list[dict[str, Any]]:
        """Raw rows that haven't been enriched yet (or whose input_hash changed)."""
        with self.connect() as conn:
            rows = conn.execute(
                """
                SELECT n.id,
                       n.repo_full_name,
                       n.subject_type,
                       n.subject_title,
                       n.reason,
                       n.body
                FROM github.notifications n
                LEFT JOIN enriched.notifications e USING (id)
                WHERE e.id IS NULL
                ORDER BY n.updated_at DESC
                LIMIT ?
                """,
                [limit],
            ).fetchall()
            cols = ["id", "repo_full_name", "subject_type", "subject_title", "reason", "body"]
            return [dict(zip(cols, r, strict=True)) for r in rows]

    def upsert_enrichment(
        self,
        *,
        id: str,
        summary: str,
        actions: list[str],
        priority: int,
        priority_reason: str,
        sentiment: str,
        model: str,
        input_hash: str,
    ) -> None:
        import json

        with self.connect() as conn:
            conn.execute(
                """
                INSERT INTO enriched.notifications
                    (id, summary, actions, priority, priority_reason,
                     sentiment, model, input_hash)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT (id) DO UPDATE SET
                    summary         = EXCLUDED.summary,
                    actions         = EXCLUDED.actions,
                    priority        = EXCLUDED.priority,
                    priority_reason = EXCLUDED.priority_reason,
                    sentiment       = EXCLUDED.sentiment,
                    model           = EXCLUDED.model,
                    enriched_at     = current_timestamp,
                    input_hash      = EXCLUDED.input_hash
                """,
                [id, summary, json.dumps(actions), priority, priority_reason,
                 sentiment, model, input_hash],
            )

    def top_digest(self, limit: int = 20) -> list[dict[str, Any]]:
        with self.connect() as conn:
            rows = conn.execute(
                f"""
                SELECT id, repo_full_name, subject_type, subject_title,
                       priority, summary, actions, sentiment, updated_at
                FROM enriched.digest
                WHERE priority IS NOT NULL
                ORDER BY priority DESC, updated_at DESC
                LIMIT {int(limit)}
                """
            ).fetchall()
            cols = ["id", "repo", "type", "title", "priority",
                    "summary", "actions", "sentiment", "updated_at"]
            return [dict(zip(cols, r, strict=True)) for r in rows]


def get_store() -> Store:
    return Store(settings.duckdb_path)
