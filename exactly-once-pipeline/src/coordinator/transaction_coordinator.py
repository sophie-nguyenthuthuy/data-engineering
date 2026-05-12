"""
Distributed Transaction Coordinator — implements the Saga pattern.

Each payment spans four systems:
  1. Postgres ledger   (written by PaymentService, always first)
  2. Kafka             (published by OutboxPoller)
  3. Data warehouse    (consumed by WarehouseConsumer)
  4. Notification queue (consumed by NotificationConsumer)

The coordinator persists step progress in `transaction_states`.
On restart it picks up incomplete sagas and drives them to completion
or triggers compensating transactions if a step permanently fails.

Saga state machine:
  CREATED → KAFKA_PUBLISHED → WAREHOUSE_WRITTEN → NOTIFICATION_SENT → COMPLETED
                  ↓                   ↓                    ↓
              FAILED             FAILED                FAILED
                  ↓
             COMPENSATING → (ledger reversed) → COMPENSATED
"""
from __future__ import annotations

import json
from datetime import datetime, timezone
from typing import Any

import structlog

from src.db import transaction
from src.models import TransactionStep

log = structlog.get_logger(__name__)


class TransactionCoordinator:
    # ── Step advancement ───────────────────────────────────────────────
    def advance(self, idempotency_key: str, step: TransactionStep) -> None:
        """Mark a step as complete and advance the saga."""
        column_map = {
            TransactionStep.KAFKA_PUBLISHED: "kafka_published",
            TransactionStep.WAREHOUSE_WRITTEN: "warehouse_ack",
            TransactionStep.NOTIFICATION_SENT: "notification_ack",
        }

        col = column_map.get(step)
        if col is None:
            return

        with transaction() as cur:
            cur.execute(
                f"""
                UPDATE transaction_states
                SET    {col}        = TRUE,
                       current_step = %s,
                       updated_at   = NOW()
                WHERE  idempotency_key = %s
                RETURNING kafka_published, warehouse_ack, notification_ack
                """,
                (step.value, idempotency_key),
            )
            row = cur.fetchone()
            if row and row["kafka_published"] and row["warehouse_ack"] and row["notification_ack"]:
                cur.execute(
                    """
                    UPDATE transaction_states
                    SET    current_step  = 'COMPLETED',
                           completed_at = NOW()
                    WHERE  idempotency_key = %s
                    """,
                    (idempotency_key,),
                )
                log.info("coordinator.saga_completed", idempotency_key=idempotency_key)
            else:
                log.info("coordinator.step_advanced", step=step.value,
                         idempotency_key=idempotency_key)

    # ── Failure recording ──────────────────────────────────────────────
    def record_failure(
        self,
        idempotency_key: str,
        step: TransactionStep,
        error: str,
        *,
        permanent: bool = False,
    ) -> None:
        with transaction() as cur:
            if permanent:
                cur.execute(
                    """
                    UPDATE transaction_states
                    SET    current_step  = 'FAILED',
                           failed_at     = NOW(),
                           error_message = %s,
                           retry_count   = retry_count + 1
                    WHERE  idempotency_key = %s
                    """,
                    (error, idempotency_key),
                )
                log.error("coordinator.permanent_failure",
                          step=step.value, idempotency_key=idempotency_key,
                          error=error)
            else:
                cur.execute(
                    """
                    UPDATE transaction_states
                    SET    retry_count   = retry_count + 1,
                           error_message = %s
                    WHERE  idempotency_key = %s
                    """,
                    (error, idempotency_key),
                )
                log.warning("coordinator.transient_failure",
                            step=step.value, idempotency_key=idempotency_key,
                            error=error)

    # ── Compensation (rollback) ────────────────────────────────────────
    def compensate(self, idempotency_key: str, reason: str) -> None:
        """
        Reverse a payment that partially completed.
        In production this would debit/credit accounts; here we mark the
        ledger row as COMPENSATED and record the saga outcome.
        """
        log.warning("coordinator.compensating",
                    idempotency_key=idempotency_key, reason=reason)

        with transaction() as cur:
            cur.execute(
                """
                UPDATE transaction_states
                SET    current_step = 'COMPENSATING',
                       error_message = %s
                WHERE  idempotency_key = %s
                """,
                (reason, idempotency_key),
            )
            cur.execute(
                """
                UPDATE ledger
                SET    status = 'COMPENSATED'
                WHERE  idempotency_key = %s
                """,
                (idempotency_key,),
            )
            cur.execute(
                """
                UPDATE transaction_states
                SET    current_step = 'FAILED',
                       failed_at    = NOW()
                WHERE  idempotency_key = %s
                """,
                (idempotency_key,),
            )

        log.info("coordinator.compensated", idempotency_key=idempotency_key)

    # ── Recovery scan ─────────────────────────────────────────────────
    def recover_incomplete(self, max_age_minutes: int = 30) -> list[dict[str, Any]]:
        """
        Return sagas that started but never completed within max_age_minutes.
        The caller decides whether to retry or compensate.
        """
        with transaction() as cur:
            cur.execute(
                """
                SELECT *
                FROM   transaction_states
                WHERE  current_step NOT IN ('COMPLETED', 'FAILED')
                  AND  created_at < NOW() - INTERVAL '1 minute' * %s
                ORDER  BY created_at
                """,
                (max_age_minutes,),
            )
            rows = cur.fetchall()

        if rows:
            log.warning("coordinator.incomplete_sagas", count=len(rows))
        return [dict(r) for r in rows]

    # ── Status query ──────────────────────────────────────────────────
    def get_status(self, idempotency_key: str) -> dict[str, Any] | None:
        with transaction() as cur:
            cur.execute(
                "SELECT * FROM transaction_states WHERE idempotency_key = %s",
                (idempotency_key,),
            )
            row = cur.fetchone()
        return dict(row) if row else None
