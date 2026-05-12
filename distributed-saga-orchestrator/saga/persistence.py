from __future__ import annotations

import json
import sqlite3
import time
from contextlib import contextmanager
from dataclasses import asdict, dataclass, field
from enum import Enum
from pathlib import Path
from typing import Any, Generator


class SagaStatus(str, Enum):
    PENDING = "pending"
    RUNNING = "running"
    COMPENSATING = "compensating"
    COMPLETED = "completed"
    FAILED = "failed"
    COMPENSATED = "compensated"


@dataclass
class SagaRecord:
    saga_id: str
    saga_type: str
    status: SagaStatus = SagaStatus.PENDING
    context: dict[str, Any] = field(default_factory=dict)
    step_records: list[dict] = field(default_factory=list)
    created_at: float = field(default_factory=time.time)
    updated_at: float = field(default_factory=time.time)
    completed_at: float | None = None
    failure_step: str | None = None
    failure_reason: str | None = None
    compensation_errors: list[dict] = field(default_factory=list)


_SCHEMA = """
CREATE TABLE IF NOT EXISTS saga_runs (
    saga_id       TEXT PRIMARY KEY,
    saga_type     TEXT NOT NULL,
    status        TEXT NOT NULL,
    context       TEXT NOT NULL,
    step_records  TEXT NOT NULL,
    created_at    REAL NOT NULL,
    updated_at    REAL NOT NULL,
    completed_at  REAL,
    failure_step  TEXT,
    failure_reason TEXT,
    compensation_errors TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_saga_status ON saga_runs(status);
CREATE INDEX IF NOT EXISTS idx_saga_type   ON saga_runs(saga_type);
"""


class SagaStore:
    """SQLite-backed durable store for saga state."""

    def __init__(self, db_path: str | Path = ":memory:") -> None:
        self._db_path = str(db_path)
        self._conn = sqlite3.connect(self._db_path, check_same_thread=False)
        self._conn.row_factory = sqlite3.Row
        self._conn.executescript(_SCHEMA)
        self._conn.commit()

    @contextmanager
    def _tx(self) -> Generator[sqlite3.Connection, None, None]:
        try:
            yield self._conn
            self._conn.commit()
        except Exception:
            self._conn.rollback()
            raise

    def save(self, record: SagaRecord) -> None:
        record.updated_at = time.time()
        with self._tx() as conn:
            conn.execute(
                """
                INSERT INTO saga_runs
                    (saga_id, saga_type, status, context, step_records,
                     created_at, updated_at, completed_at,
                     failure_step, failure_reason, compensation_errors)
                VALUES (?,?,?,?,?,?,?,?,?,?,?)
                ON CONFLICT(saga_id) DO UPDATE SET
                    status=excluded.status,
                    context=excluded.context,
                    step_records=excluded.step_records,
                    updated_at=excluded.updated_at,
                    completed_at=excluded.completed_at,
                    failure_step=excluded.failure_step,
                    failure_reason=excluded.failure_reason,
                    compensation_errors=excluded.compensation_errors
                """,
                (
                    record.saga_id,
                    record.saga_type,
                    record.status.value,
                    json.dumps(record.context),
                    json.dumps(record.step_records),
                    record.created_at,
                    record.updated_at,
                    record.completed_at,
                    record.failure_step,
                    record.failure_reason,
                    json.dumps(record.compensation_errors),
                ),
            )

    def load(self, saga_id: str) -> SagaRecord | None:
        row = self._conn.execute(
            "SELECT * FROM saga_runs WHERE saga_id = ?", (saga_id,)
        ).fetchone()
        if row is None:
            return None
        return self._row_to_record(row)

    def list_by_status(self, status: SagaStatus) -> list[SagaRecord]:
        rows = self._conn.execute(
            "SELECT * FROM saga_runs WHERE status = ? ORDER BY created_at DESC",
            (status.value,),
        ).fetchall()
        return [self._row_to_record(r) for r in rows]

    def list_by_type(self, saga_type: str) -> list[SagaRecord]:
        rows = self._conn.execute(
            "SELECT * FROM saga_runs WHERE saga_type = ? ORDER BY created_at DESC",
            (saga_type,),
        ).fetchall()
        return [self._row_to_record(r) for r in rows]

    def _row_to_record(self, row: sqlite3.Row) -> SagaRecord:
        return SagaRecord(
            saga_id=row["saga_id"],
            saga_type=row["saga_type"],
            status=SagaStatus(row["status"]),
            context=json.loads(row["context"]),
            step_records=json.loads(row["step_records"]),
            created_at=row["created_at"],
            updated_at=row["updated_at"],
            completed_at=row["completed_at"],
            failure_step=row["failure_step"],
            failure_reason=row["failure_reason"],
            compensation_errors=json.loads(row["compensation_errors"]),
        )

    def close(self) -> None:
        self._conn.close()
