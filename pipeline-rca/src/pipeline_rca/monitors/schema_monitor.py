"""Detect and record schema / pipeline changes that are candidate root causes."""

from __future__ import annotations

import hashlib
import json
import logging
import sqlite3
from datetime import datetime
from pathlib import Path
from typing import Any

from pipeline_rca.models import ChangeCategoryKind, SchemaChange

logger = logging.getLogger(__name__)

_DDL_INIT = """
CREATE TABLE IF NOT EXISTS schema_snapshots (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    table_name  TEXT NOT NULL,
    snapshot    TEXT NOT NULL,   -- JSON-serialised column list
    fingerprint TEXT NOT NULL,
    captured_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS schema_change_log (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    table_name  TEXT NOT NULL,
    column_name TEXT,
    kind        TEXT NOT NULL,
    details     TEXT NOT NULL,   -- JSON
    occurred_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS pipeline_event_log (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    table_name  TEXT NOT NULL,
    kind        TEXT NOT NULL,
    details     TEXT NOT NULL,
    occurred_at TEXT NOT NULL
);
"""


class SchemaStore:
    """Lightweight SQLite-backed store for schema snapshots and change events."""

    def __init__(self, db_path: str | Path = ":memory:") -> None:
        self._conn = sqlite3.connect(str(db_path), check_same_thread=False)
        self._conn.executescript(_DDL_INIT)
        self._conn.commit()

    # ------------------------------------------------------------------
    # Schema snapshot / diff
    # ------------------------------------------------------------------

    def snapshot_columns(
        self, table_name: str, columns: list[dict[str, Any]], captured_at: datetime | None = None
    ) -> list[SchemaChange]:
        """
        Record a column snapshot for *table_name* and return any diff vs the
        previous snapshot as SchemaChange objects.
        """
        captured_at = captured_at or datetime.utcnow()
        snapshot_json = json.dumps(columns, sort_keys=True)
        fingerprint = hashlib.sha256(snapshot_json.encode()).hexdigest()

        cur = self._conn.execute(
            "SELECT snapshot, fingerprint FROM schema_snapshots "
            "WHERE table_name = ? ORDER BY id DESC LIMIT 1",
            (table_name,),
        )
        row = cur.fetchone()

        self._conn.execute(
            "INSERT INTO schema_snapshots (table_name, snapshot, fingerprint, captured_at) "
            "VALUES (?, ?, ?, ?)",
            (table_name, snapshot_json, fingerprint, captured_at.isoformat()),
        )
        self._conn.commit()

        if row is None or row[1] == fingerprint:
            return []

        prev_columns: list[dict[str, Any]] = json.loads(row[0])
        changes = _diff_columns(table_name, prev_columns, columns, captured_at)

        for c in changes:
            self._conn.execute(
                "INSERT INTO schema_change_log "
                "(table_name, column_name, kind, details, occurred_at) "
                "VALUES (?, ?, ?, ?, ?)",
                (
                    c.table,
                    c.column,
                    c.kind.value,
                    json.dumps(c.details),
                    c.occurred_at.isoformat(),
                ),
            )
        self._conn.commit()

        logger.info("Detected %d schema change(s) for table %s", len(changes), table_name)
        return changes

    def record_pipeline_event(
        self,
        table_name: str,
        kind: ChangeCategoryKind,
        details: dict[str, Any],
        occurred_at: datetime | None = None,
    ) -> SchemaChange:
        occurred_at = occurred_at or datetime.utcnow()
        self._conn.execute(
            "INSERT INTO pipeline_event_log (table_name, kind, details, occurred_at) "
            "VALUES (?, ?, ?, ?)",
            (table_name, kind.value, json.dumps(details), occurred_at.isoformat()),
        )
        self._conn.commit()
        return SchemaChange(
            table=table_name,
            column=None,
            kind=kind,
            occurred_at=occurred_at,
            details=details,
        )

    def get_recent_changes(
        self, tables: list[str], since: datetime
    ) -> list[SchemaChange]:
        """Return all schema + pipeline changes for *tables* since *since*."""
        placeholders = ",".join("?" * len(tables))
        changes: list[SchemaChange] = []

        cur = self._conn.execute(
            f"SELECT table_name, column_name, kind, details, occurred_at "
            f"FROM schema_change_log "
            f"WHERE table_name IN ({placeholders}) AND occurred_at >= ? "
            f"ORDER BY occurred_at DESC",
            (*tables, since.isoformat()),
        )
        for row in cur.fetchall():
            changes.append(
                SchemaChange(
                    table=row[0],
                    column=row[1],
                    kind=ChangeCategoryKind(row[2]),
                    occurred_at=datetime.fromisoformat(row[4]),
                    details=json.loads(row[3]),
                )
            )

        cur = self._conn.execute(
            f"SELECT table_name, kind, details, occurred_at "
            f"FROM pipeline_event_log "
            f"WHERE table_name IN ({placeholders}) AND occurred_at >= ? "
            f"ORDER BY occurred_at DESC",
            (*tables, since.isoformat()),
        )
        for row in cur.fetchall():
            changes.append(
                SchemaChange(
                    table=row[0],
                    column=None,
                    kind=ChangeCategoryKind(row[1]),
                    occurred_at=datetime.fromisoformat(row[3]),
                    details=json.loads(row[2]),
                )
            )

        changes.sort(key=lambda c: c.occurred_at, reverse=True)
        return changes


# ------------------------------------------------------------------
# Internal helpers
# ------------------------------------------------------------------

def _diff_columns(
    table: str,
    prev: list[dict[str, Any]],
    curr: list[dict[str, Any]],
    occurred_at: datetime,
) -> list[SchemaChange]:
    prev_map = {c["name"]: c for c in prev}
    curr_map = {c["name"]: c for c in curr}
    changes: list[SchemaChange] = []

    for name in set(prev_map) - set(curr_map):
        changes.append(
            SchemaChange(
                table=table,
                column=name,
                kind=ChangeCategoryKind.COLUMN_DROPPED,
                occurred_at=occurred_at,
                details={"old": prev_map[name]},
            )
        )

    for name in set(curr_map) - set(prev_map):
        changes.append(
            SchemaChange(
                table=table,
                column=name,
                kind=ChangeCategoryKind.COLUMN_ADDED,
                occurred_at=occurred_at,
                details={"new": curr_map[name]},
            )
        )

    for name in set(prev_map) & set(curr_map):
        p, c = prev_map[name], curr_map[name]
        if p.get("type") != c.get("type"):
            changes.append(
                SchemaChange(
                    table=table,
                    column=name,
                    kind=ChangeCategoryKind.TYPE_CHANGED,
                    occurred_at=occurred_at,
                    details={"from": p.get("type"), "to": c.get("type")},
                )
            )

    return changes
