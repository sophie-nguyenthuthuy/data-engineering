"""
Payment Service — writes to ledger + outbox in a single Postgres transaction.

This is the entry-point for every payment.  The two inserts are atomic:
either both land or neither does, so the outbox always mirrors the ledger.
"""
from __future__ import annotations

import json
from datetime import datetime

import structlog

from src.db import transaction
from src.models import OutboxEntry, PaymentEvent, TransactionState, TransactionStep

log = structlog.get_logger(__name__)


class PaymentService:
    def create_payment(self, event: PaymentEvent) -> TransactionState:
        """
        Atomically:
          1. Insert into ledger (source of truth)
          2. Insert into outbox (relay will publish to Kafka)
          3. Insert into transaction_states (coordinator tracking)
        All three rows share the same idempotency_key, so the whole
        operation is idempotent — re-submitting the same key is a no-op.
        """
        log.info("payment.creating", payment_id=event.payment_id,
                 idempotency_key=event.idempotency_key, amount=str(event.amount))

        payload = json.loads(event.model_dump_json())

        with transaction() as cur:
            # ── 1. Ledger ──────────────────────────────────────────────
            cur.execute(
                """
                INSERT INTO ledger
                    (payment_id, idempotency_key, sender_account, receiver_account,
                     amount, currency, description, status)
                VALUES (%s, %s, %s, %s, %s, %s, %s, 'PENDING')
                ON CONFLICT (idempotency_key) DO NOTHING
                RETURNING payment_id
                """,
                (
                    event.payment_id, event.idempotency_key,
                    event.sender_account, event.receiver_account,
                    event.amount, event.currency, event.description,
                ),
            )
            row = cur.fetchone()
            if row is None:
                # Idempotent re-submission: return existing coordinator state
                log.info("payment.duplicate_idempotency_key",
                         idempotency_key=event.idempotency_key)
                cur.execute(
                    "SELECT * FROM transaction_states WHERE idempotency_key = %s",
                    (event.idempotency_key,),
                )
                state_row = cur.fetchone()
                return TransactionState(**dict(state_row))  # type: ignore[arg-type]

            # ── 2. Outbox ──────────────────────────────────────────────
            cur.execute(
                """
                INSERT INTO outbox
                    (idempotency_key, aggregate_type, aggregate_id, event_type, payload)
                VALUES (%s, 'payment', %s, 'PaymentCreated', %s)
                """,
                (event.idempotency_key, event.payment_id, json.dumps(payload)),
            )

            # ── 3. Transaction coordinator state ──────────────────────
            cur.execute(
                """
                INSERT INTO transaction_states
                    (idempotency_key, payment_id, current_step)
                VALUES (%s, %s, 'CREATED')
                RETURNING *
                """,
                (event.idempotency_key, event.payment_id),
            )
            state_row = cur.fetchone()

        state = TransactionState(**dict(state_row))  # type: ignore[arg-type]
        log.info("payment.created", payment_id=event.payment_id,
                 transaction_id=state.transaction_id)
        return state
