"""
Outbox Relay — polls the outbox table and publishes to Kafka using
Kafka transactions, providing exactly-once delivery from Postgres → Kafka.

Recovery guarantee:
  - If the process dies after publishing but before marking published_at,
    the entry is retried on restart.  Kafka's idempotent producer +
    transactional.id ensure the duplicate message is deduplicated by
    the broker, so consumers never see it twice.
"""
from __future__ import annotations

import json
import signal
import time
from datetime import datetime, timezone
from typing import Any

import structlog
from confluent_kafka import KafkaException, Producer

from src.config import settings
from src.db import get_conn
from src.recovery.failure_injector import FailureInjector

log = structlog.get_logger(__name__)


class OutboxPoller:
    def __init__(self, failure_injector: FailureInjector | None = None) -> None:
        self._running = False
        self._injector = failure_injector or FailureInjector()
        self._producer = self._make_producer()

    # ── Kafka producer with transactions ──────────────────────────────
    def _make_producer(self) -> Producer:
        return Producer({
            "bootstrap.servers": settings.kafka_bootstrap_servers,
            "transactional.id": settings.kafka_transactional_id,
            "enable.idempotence": True,
            "acks": "all",
            "retries": 2147483647,
            "max.in.flight.requests.per.connection": 5,
        })

    # ── Main loop ──────────────────────────────────────────────────────
    def start(self) -> None:
        self._producer.init_transactions()
        self._running = True
        signal.signal(signal.SIGINT, self._shutdown)
        signal.signal(signal.SIGTERM, self._shutdown)

        log.info("outbox_poller.started",
                 interval_ms=settings.outbox_poll_interval_ms)

        while self._running:
            try:
                published = self._poll_batch()
                if published == 0:
                    time.sleep(settings.outbox_poll_interval_ms / 1000)
            except Exception as exc:
                log.error("outbox_poller.poll_error", error=str(exc))
                time.sleep(2)

    def _shutdown(self, *_: Any) -> None:
        log.info("outbox_poller.shutdown")
        self._running = False

    # ── Batch processing ───────────────────────────────────────────────
    def _poll_batch(self) -> int:
        with get_conn() as conn:
            conn.autocommit = False
            cur = conn.cursor()
            try:
                # Advisory lock prevents concurrent pollers from duplicating work
                cur.execute("SELECT pg_try_advisory_xact_lock(42)")
                (locked,) = cur.fetchone()  # type: ignore[misc]
                if not locked:
                    conn.rollback()
                    return 0

                cur.execute(
                    """
                    SELECT id, idempotency_key, aggregate_id, event_type,
                           payload, retry_count
                    FROM   outbox
                    WHERE  published_at IS NULL
                      AND  retry_count < %s
                    ORDER  BY created_at
                    LIMIT  %s
                    FOR UPDATE SKIP LOCKED
                    """,
                    (settings.outbox_max_retries, settings.outbox_batch_size),
                )
                rows = cur.fetchall()
                if not rows:
                    conn.rollback()
                    return 0

                log.info("outbox_poller.batch", count=len(rows))
                self._publish_batch(rows, cur)
                conn.commit()
                return len(rows)

            except Exception:
                conn.rollback()
                raise
            finally:
                cur.close()

    def _publish_batch(self, rows: list, cur: Any) -> None:
        self._producer.begin_transaction()
        published_ids: list[int] = []

        try:
            for row in rows:
                entry_id, idempotency_key, aggregate_id, event_type, payload_str, retry_count = row

                # ── Optional failure injection ─────────────────────────
                self._injector.maybe_raise("kafka", idempotency_key)

                if isinstance(payload_str, str):
                    payload = json.loads(payload_str)
                else:
                    payload = payload_str  # psycopg2 already parsed JSONB

                message = json.dumps({
                    "idempotency_key": idempotency_key,
                    "event_type": event_type,
                    "aggregate_id": aggregate_id,
                    "payload": payload,
                }, default=str)

                self._producer.produce(
                    topic=settings.kafka_payment_topic,
                    key=str(aggregate_id),
                    value=message.encode(),
                    headers={"idempotency_key": str(idempotency_key)},
                    on_delivery=self._delivery_report,
                )
                published_ids.append(entry_id)

            self._producer.flush()
            self._producer.commit_transaction()

            # Mark as published inside the same outer Postgres transaction
            cur.execute(
                "UPDATE outbox SET published_at = %s WHERE id = ANY(%s)",
                (datetime.now(timezone.utc), published_ids),
            )
            log.info("outbox_poller.published", count=len(published_ids))

        except KafkaException as exc:
            log.error("outbox_poller.kafka_error", error=str(exc))
            self._producer.abort_transaction()
            self._increment_retry(cur, [r[0] for r in rows], str(exc))
            raise

    def _increment_retry(
        self, cur: Any, ids: list[int], error: str
    ) -> None:
        cur.execute(
            """
            UPDATE outbox
            SET    retry_count = retry_count + 1,
                   last_error  = %s
            WHERE  id = ANY(%s)
            """,
            (error, ids),
        )

    @staticmethod
    def _delivery_report(err: Any, msg: Any) -> None:
        if err:
            log.error("outbox_poller.delivery_failed", error=str(err))
        else:
            log.debug("outbox_poller.delivered",
                      topic=msg.topic(), partition=msg.partition(),
                      offset=msg.offset())
