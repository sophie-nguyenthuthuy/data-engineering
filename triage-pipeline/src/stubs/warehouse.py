"""BigQuery stub backed by DuckDB. Exposes a BQ-shaped `insert_rows_json`
and a narrow `query` surface. Schema mirrors what you'd create in BQ with
`bq mk --table dataset.emails_processed`.
"""
from __future__ import annotations

import json
import threading
from typing import Any, Iterable

import duckdb

from ..config import DATA_DIR

_LOCK = threading.Lock()
_DB_PATH = DATA_DIR / "warehouse.duckdb"


def _conn() -> duckdb.DuckDBPyConnection:
    return duckdb.connect(str(_DB_PATH))


def init() -> None:
    with _LOCK, _conn() as c:
        c.execute(
            """
            CREATE TABLE IF NOT EXISTS emails_raw (
                id          VARCHAR PRIMARY KEY,
                tenant_id   VARCHAR NOT NULL,
                sender      VARCHAR,
                subject     VARCHAR,
                body        VARCHAR,
                received_at TIMESTAMP,
                true_label  VARCHAR,
                ingested_at TIMESTAMP DEFAULT now()
            )
            """
        )
        c.execute(
            """
            CREATE TABLE IF NOT EXISTS emails_processed (
                id             VARCHAR PRIMARY KEY,
                tenant_id      VARCHAR NOT NULL,
                predicted_label VARCHAR NOT NULL,
                confidence     DOUBLE NOT NULL,
                summary        VARCHAR NOT NULL,
                priority       VARCHAR NOT NULL,  -- low | med | high
                model          VARCHAR NOT NULL,
                latency_ms     INTEGER NOT NULL,
                processed_at   TIMESTAMP DEFAULT now()
            )
            """
        )
        c.execute(
            """
            CREATE TABLE IF NOT EXISTS runs (
                run_id      VARCHAR PRIMARY KEY,
                kind        VARCHAR NOT NULL,  -- ingest | process | eval
                tenant_id   VARCHAR,
                status      VARCHAR NOT NULL,  -- running | ok | failed
                started_at  TIMESTAMP DEFAULT now(),
                finished_at TIMESTAMP,
                details     VARCHAR
            )
            """
        )
        c.execute(
            """
            CREATE TABLE IF NOT EXISTS eval_results (
                run_id      VARCHAR NOT NULL,
                label       VARCHAR NOT NULL,
                precision   DOUBLE NOT NULL,
                recall      DOUBLE NOT NULL,
                f1          DOUBLE NOT NULL,
                support     INTEGER NOT NULL,
                PRIMARY KEY (run_id, label)
            )
            """
        )


def insert_raw(row: dict[str, Any]) -> None:
    with _LOCK, _conn() as c:
        c.execute(
            """
            INSERT INTO emails_raw
              (id, tenant_id, sender, subject, body, received_at, true_label)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT (id) DO NOTHING
            """,
            [
                row["id"], row["tenant_id"], row["sender"], row["subject"],
                row["body"], row["received_at"], row.get("true_label"),
            ],
        )


def insert_processed(row: dict[str, Any]) -> None:
    with _LOCK, _conn() as c:
        c.execute(
            """
            INSERT INTO emails_processed
              (id, tenant_id, predicted_label, confidence, summary, priority, model, latency_ms)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT (id) DO UPDATE SET
              predicted_label = EXCLUDED.predicted_label,
              confidence = EXCLUDED.confidence,
              summary = EXCLUDED.summary,
              priority = EXCLUDED.priority,
              model = EXCLUDED.model,
              latency_ms = EXCLUDED.latency_ms,
              processed_at = now()
            """,
            [
                row["id"], row["tenant_id"], row["predicted_label"],
                row["confidence"], row["summary"], row["priority"],
                row["model"], row["latency_ms"],
            ],
        )


def start_run(run_id: str, kind: str, tenant_id: str | None = None) -> None:
    with _LOCK, _conn() as c:
        c.execute(
            "INSERT INTO runs (run_id, kind, tenant_id, status) VALUES (?, ?, ?, 'running')",
            [run_id, kind, tenant_id],
        )


def finish_run(run_id: str, status: str, details: str = "") -> None:
    with _LOCK, _conn() as c:
        c.execute(
            "UPDATE runs SET status = ?, finished_at = now(), details = ? WHERE run_id = ?",
            [status, details, run_id],
        )


def query(sql: str, params: Iterable | None = None) -> list[dict[str, Any]]:
    with _LOCK, _conn() as c:
        cur = c.execute(sql, list(params) if params else None)
        cols = [d[0] for d in cur.description]
        return [dict(zip(cols, r)) for r in cur.fetchall()]


def write_eval(run_id: str, per_label: list[dict[str, Any]]) -> None:
    with _LOCK, _conn() as c:
        for row in per_label:
            c.execute(
                """
                INSERT INTO eval_results (run_id, label, precision, recall, f1, support)
                VALUES (?, ?, ?, ?, ?, ?)
                ON CONFLICT (run_id, label) DO UPDATE SET
                  precision = EXCLUDED.precision,
                  recall = EXCLUDED.recall,
                  f1 = EXCLUDED.f1,
                  support = EXCLUDED.support
                """,
                [run_id, row["label"], row["precision"], row["recall"], row["f1"], row["support"]],
            )
