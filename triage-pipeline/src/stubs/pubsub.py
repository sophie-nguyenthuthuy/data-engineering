"""Local Pub/Sub stub with ack deadlines, retry counter, DLQ routing.

Keeps the GCP Pub/Sub surface area: publish / pull / ack / nack. Uses a DuckDB
table as the queue so we get crash-recovery + inspectability for free, and the
dashboard can just SELECT from it.
"""
from __future__ import annotations

import json
import threading
import time
from dataclasses import dataclass
from typing import Any

import duckdb

from ..config import DATA_DIR

_LOCK = threading.Lock()
_DB_PATH = DATA_DIR / "pubsub.duckdb"


def _conn() -> duckdb.DuckDBPyConnection:
    return duckdb.connect(str(_DB_PATH))


def init() -> None:
    with _LOCK, _conn() as c:
        c.execute(
            """
            CREATE TABLE IF NOT EXISTS messages (
                message_id   VARCHAR PRIMARY KEY,
                topic        VARCHAR NOT NULL,
                payload      VARCHAR NOT NULL,
                attributes   VARCHAR NOT NULL,
                state        VARCHAR NOT NULL,  -- pending | leased | acked | dlq
                delivery_count INTEGER NOT NULL DEFAULT 0,
                lease_expires_at TIMESTAMP,
                published_at TIMESTAMP NOT NULL,
                last_error   VARCHAR
            )
            """
        )


@dataclass
class Message:
    message_id: str
    topic: str
    payload: dict
    attributes: dict
    delivery_count: int


def publish(topic: str, payload: dict, attributes: dict | None = None) -> str:
    import uuid

    mid = str(uuid.uuid4())
    with _LOCK, _conn() as c:
        c.execute(
            """
            INSERT INTO messages
              (message_id, topic, payload, attributes, state, published_at)
            VALUES (?, ?, ?, ?, 'pending', now())
            """,
            [mid, topic, json.dumps(payload), json.dumps(attributes or {})],
        )
    return mid


def pull(topic: str, max_messages: int = 10, lease_seconds: int = 30) -> list[Message]:
    now = time.time()
    with _LOCK, _conn() as c:
        # expire stale leases first
        c.execute(
            """
            UPDATE messages
            SET state = 'pending'
            WHERE state = 'leased' AND lease_expires_at < now()
            """
        )
        rows = c.execute(
            """
            SELECT message_id, topic, payload, attributes, delivery_count
            FROM messages
            WHERE topic = ? AND state = 'pending'
            ORDER BY published_at
            LIMIT ?
            """,
            [topic, max_messages],
        ).fetchall()
        if not rows:
            return []
        ids = [r[0] for r in rows]
        c.execute(
            f"""
            UPDATE messages
            SET state = 'leased',
                lease_expires_at = now() + INTERVAL {lease_seconds} SECOND,
                delivery_count = delivery_count + 1
            WHERE message_id IN ({','.join('?' for _ in ids)})
            """,
            ids,
        )
    return [
        Message(
            message_id=r[0],
            topic=r[1],
            payload=json.loads(r[2]),
            attributes=json.loads(r[3]),
            delivery_count=r[4] + 1,
        )
        for r in rows
    ]


def ack(message_id: str) -> None:
    with _LOCK, _conn() as c:
        c.execute(
            "UPDATE messages SET state = 'acked' WHERE message_id = ?",
            [message_id],
        )


def nack(message_id: str, error: str, max_retries: int, dlq_topic: str) -> bool:
    """Return True if routed to DLQ, False if simply re-queued."""
    with _LOCK, _conn() as c:
        row = c.execute(
            "SELECT delivery_count, topic, payload, attributes FROM messages WHERE message_id = ?",
            [message_id],
        ).fetchone()
        if not row:
            return False
        delivery_count, topic, payload, attrs = row
        if delivery_count >= max_retries:
            c.execute(
                "UPDATE messages SET state = 'dlq', last_error = ? WHERE message_id = ?",
                [error, message_id],
            )
            # mirror a marker row into the dlq topic for dashboard queries
            import uuid
            c.execute(
                """
                INSERT INTO messages
                  (message_id, topic, payload, attributes, state, delivery_count, published_at, last_error)
                VALUES (?, ?, ?, ?, 'pending', 0, now(), ?)
                """,
                [str(uuid.uuid4()), dlq_topic, payload, attrs, error],
            )
            return True
        c.execute(
            """
            UPDATE messages
            SET state = 'pending', lease_expires_at = NULL, last_error = ?
            WHERE message_id = ?
            """,
            [error, message_id],
        )
        return False


def stats() -> list[dict[str, Any]]:
    with _LOCK, _conn() as c:
        rows = c.execute(
            """
            SELECT topic, state, COUNT(*) AS n
            FROM messages
            GROUP BY topic, state
            ORDER BY topic, state
            """
        ).fetchall()
    return [{"topic": r[0], "state": r[1], "count": r[2]} for r in rows]
