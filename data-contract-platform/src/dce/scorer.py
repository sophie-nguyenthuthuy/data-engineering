"""Reliability scoring per producer, backed by a SQLite store."""

from __future__ import annotations

import json
import sqlite3
from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path
from typing import Iterator

from .validator import ValidationResult


@dataclass
class ProducerScore:
    producer: str
    contract_id: str
    total_runs: int
    passed_runs: int
    reliability_score: float   # 0.0 – 1.0
    last_validated_at: str
    last_passed: bool


class ReliabilityStore:
    """Persist validation results and compute rolling reliability scores."""

    def __init__(self, db_path: Path | str = "reliability.db"):
        self.db_path = Path(db_path)
        self._init_db()

    @contextmanager
    def _conn(self) -> Iterator[sqlite3.Connection]:
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        try:
            yield conn
            conn.commit()
        finally:
            conn.close()

    def _init_db(self) -> None:
        with self._conn() as conn:
            conn.executescript("""
                CREATE TABLE IF NOT EXISTS validation_runs (
                    id          INTEGER PRIMARY KEY AUTOINCREMENT,
                    producer    TEXT    NOT NULL,
                    contract_id TEXT    NOT NULL,
                    version     TEXT    NOT NULL,
                    validated_at TEXT   NOT NULL,
                    passed      INTEGER NOT NULL,
                    error_count INTEGER NOT NULL,
                    warning_count INTEGER NOT NULL,
                    stats       TEXT    NOT NULL,
                    issues      TEXT    NOT NULL
                );
                CREATE INDEX IF NOT EXISTS idx_runs_producer
                    ON validation_runs(producer, contract_id);
            """)

    # ------------------------------------------------------------------ #

    def record(self, result: ValidationResult) -> None:
        """Persist a ValidationResult."""
        with self._conn() as conn:
            conn.execute(
                """
                INSERT INTO validation_runs
                    (producer, contract_id, version, validated_at,
                     passed, error_count, warning_count, stats, issues)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    result.producer,
                    result.contract_id,
                    result.contract_version,
                    result.validated_at,
                    1 if result.passed else 0,
                    len(result.errors()),
                    len(result.warnings()),
                    json.dumps(result.stats, default=str),
                    json.dumps(
                        [{"rule": i.rule, "severity": i.severity, "message": i.message}
                         for i in result.issues]
                    ),
                ),
            )

    def score(
        self,
        producer: str,
        contract_id: str,
        *,
        window: int = 100,
    ) -> ProducerScore | None:
        """Return the reliability score for the last *window* runs."""
        with self._conn() as conn:
            rows = conn.execute(
                """
                SELECT passed, validated_at
                FROM validation_runs
                WHERE producer = ? AND contract_id = ?
                ORDER BY id DESC
                LIMIT ?
                """,
                (producer, contract_id, window),
            ).fetchall()

        if not rows:
            return None

        total = len(rows)
        passed = sum(r["passed"] for r in rows)
        return ProducerScore(
            producer=producer,
            contract_id=contract_id,
            total_runs=total,
            passed_runs=passed,
            reliability_score=round(passed / total, 4),
            last_validated_at=rows[0]["validated_at"],
            last_passed=bool(rows[0]["passed"]),
        )

    def all_scores(self, *, window: int = 100) -> list[ProducerScore]:
        """Return scores for every (producer, contract_id) pair."""
        with self._conn() as conn:
            pairs = conn.execute(
                "SELECT DISTINCT producer, contract_id FROM validation_runs"
            ).fetchall()
        return [
            s
            for p in pairs
            if (s := self.score(p["producer"], p["contract_id"], window=window))
        ]

    def history(
        self,
        producer: str,
        contract_id: str,
        *,
        limit: int = 50,
    ) -> list[dict]:
        with self._conn() as conn:
            rows = conn.execute(
                """
                SELECT validated_at, passed, error_count, warning_count, stats
                FROM validation_runs
                WHERE producer = ? AND contract_id = ?
                ORDER BY id DESC
                LIMIT ?
                """,
                (producer, contract_id, limit),
            ).fetchall()
        return [dict(r) for r in rows]
