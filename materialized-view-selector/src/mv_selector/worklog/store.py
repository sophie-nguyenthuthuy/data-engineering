"""
SQLite-backed worklog store.

Deduplicates queries by fingerprint and tracks frequency / total cost so the
optimizer always works with aggregate statistics rather than raw rows.
"""

from __future__ import annotations

import json
import sqlite3
from contextlib import contextmanager
from datetime import datetime
from pathlib import Path
from typing import Generator, Optional

from ..models import QueryRecord, Warehouse
from ..query_analyzer import fingerprint


_SCHEMA = """
CREATE TABLE IF NOT EXISTS query_records (
    fp          TEXT PRIMARY KEY,
    query_id    TEXT,
    sql         TEXT NOT NULL,
    warehouse   TEXT NOT NULL,
    executed_at TEXT NOT NULL,
    duration_ms INTEGER,
    bytes_processed INTEGER,
    cost_usd    REAL,
    frequency   INTEGER DEFAULT 1,
    user        TEXT,
    project_or_account TEXT,
    dataset_or_schema  TEXT
);

CREATE TABLE IF NOT EXISTS ingestion_log (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    warehouse   TEXT,
    fetched_at  TEXT,
    row_count   INTEGER
);
"""


class WorklogStore:
    def __init__(self, path: Optional[Path] = None) -> None:
        self._path = path or Path(".worklog.db")
        self._init()

    @contextmanager
    def _db(self) -> Generator[sqlite3.Connection, None, None]:
        con = sqlite3.connect(str(self._path))
        con.row_factory = sqlite3.Row
        try:
            yield con
            con.commit()
        finally:
            con.close()

    def _init(self) -> None:
        with self._db() as con:
            con.executescript(_SCHEMA)

    # ------------------------------------------------------------------

    def upsert(self, records: list[QueryRecord]) -> int:
        """Insert or increment frequency for each record. Returns rows upserted."""
        count = 0
        with self._db() as con:
            for r in records:
                fp = fingerprint(r.sql)
                existing = con.execute(
                    "SELECT frequency, cost_usd FROM query_records WHERE fp = ?",
                    (fp,),
                ).fetchone()
                if existing:
                    con.execute(
                        "UPDATE query_records SET frequency = frequency + 1, "
                        "cost_usd = cost_usd + ? WHERE fp = ?",
                        (r.cost_usd, fp),
                    )
                else:
                    con.execute(
                        """
                        INSERT INTO query_records
                            (fp, query_id, sql, warehouse, executed_at,
                             duration_ms, bytes_processed, cost_usd, frequency,
                             user, project_or_account, dataset_or_schema)
                        VALUES (?,?,?,?,?,?,?,?,?,?,?,?)
                        """,
                        (
                            fp,
                            r.query_id,
                            r.sql,
                            r.warehouse.value,
                            r.executed_at.isoformat(),
                            r.duration_ms,
                            r.bytes_processed,
                            r.cost_usd,
                            r.frequency,
                            r.user,
                            r.project_or_account,
                            r.dataset_or_schema,
                        ),
                    )
                    count += 1
        return count

    def load(
        self,
        warehouse: Optional[Warehouse] = None,
        limit: int = 50_000,
    ) -> list[QueryRecord]:
        where = ""
        params: list = []
        if warehouse:
            where = "WHERE warehouse = ?"
            params.append(warehouse.value)
        with self._db() as con:
            rows = con.execute(
                f"SELECT * FROM query_records {where} "
                f"ORDER BY cost_usd * frequency DESC LIMIT ?",
                params + [limit],
            ).fetchall()
        return [_row_to_record(r) for r in rows]

    def record_ingestion(
        self, warehouse: Warehouse, row_count: int
    ) -> None:
        with self._db() as con:
            con.execute(
                "INSERT INTO ingestion_log (warehouse, fetched_at, row_count) "
                "VALUES (?,?,?)",
                (warehouse.value, datetime.utcnow().isoformat(), row_count),
            )

    def stats(self) -> dict:
        with self._db() as con:
            total = con.execute(
                "SELECT COUNT(*), SUM(frequency), SUM(cost_usd) FROM query_records"
            ).fetchone()
        return {
            "unique_queries": total[0],
            "total_executions": total[1] or 0,
            "total_cost_usd": round(total[2] or 0.0, 4),
        }


def _row_to_record(row: sqlite3.Row) -> QueryRecord:
    return QueryRecord(
        query_id=row["query_id"],
        sql=row["sql"],
        warehouse=Warehouse(row["warehouse"]),
        executed_at=datetime.fromisoformat(row["executed_at"]),
        duration_ms=row["duration_ms"] or 0,
        bytes_processed=row["bytes_processed"] or 0,
        cost_usd=row["cost_usd"] or 0.0,
        frequency=row["frequency"],
        user=row["user"],
        project_or_account=row["project_or_account"],
        dataset_or_schema=row["dataset_or_schema"],
    )
