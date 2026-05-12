"""
Tests for exactly-once guarantees across the pipeline.
These are integration tests — requires the Postgres Docker container.
"""
from __future__ import annotations

import uuid
from decimal import Decimal

import pytest

from src.models import PaymentEvent, TransactionStep


class TestAtomicLedgerOutbox:
    """Ledger and outbox must be written in the same transaction."""

    def test_both_rows_created(self, payment_service, db):
        cur, keys = db
        event = PaymentEvent(
            sender_account="A1", receiver_account="A2", amount=Decimal("50.00")
        )
        keys.append(event.idempotency_key)

        payment_service.create_payment(event)

        cur.execute("SELECT 1 FROM ledger WHERE idempotency_key = %s::uuid",
                    (event.idempotency_key,))
        assert cur.fetchone() is not None, "ledger row missing"

        cur.execute("SELECT 1 FROM outbox WHERE idempotency_key = %s::uuid",
                    (event.idempotency_key,))
        assert cur.fetchone() is not None, "outbox row missing"

        cur.execute("SELECT 1 FROM transaction_states WHERE idempotency_key = %s::uuid",
                    (event.idempotency_key,))
        assert cur.fetchone() is not None, "coordinator state missing"


class TestIdempotency:
    """Submitting the same idempotency key twice must be a no-op."""

    def test_duplicate_returns_same_state(self, payment_service, db):
        cur, keys = db
        event = PaymentEvent(
            sender_account="B1", receiver_account="B2", amount=Decimal("25.00")
        )
        keys.append(event.idempotency_key)

        s1 = payment_service.create_payment(event)
        s2 = payment_service.create_payment(event)  # duplicate

        assert s1.transaction_id == s2.transaction_id

        # Only one row in each table
        for table in ("ledger", "outbox", "transaction_states"):
            cur.execute(
                f"SELECT COUNT(*) AS c FROM {table} WHERE idempotency_key = %s::uuid",
                (event.idempotency_key,),
            )
            count = cur.fetchone()["c"]
            assert count == 1, f"Expected 1 row in {table}, got {count}"

    def test_idempotency_log_prevents_double_processing(self, db):
        """Simulates a consumer checking the idempotency log."""
        cur, keys = db
        key = str(uuid.uuid4())
        keys.append(key)

        # Insert as if already processed
        cur.execute(
            "INSERT INTO idempotency_log (idempotency_key, consumer) VALUES (%s::uuid, %s)",
            (key, "warehouse"),
        )

        # Check
        cur.execute(
            "SELECT 1 FROM idempotency_log WHERE idempotency_key = %s::uuid AND consumer = %s",
            (key, "warehouse"),
        )
        assert cur.fetchone() is not None

        # A second insert must be silently ignored (ON CONFLICT DO NOTHING)
        cur.execute(
            "INSERT INTO idempotency_log (idempotency_key, consumer) VALUES (%s::uuid, %s) ON CONFLICT DO NOTHING",
            (key, "warehouse"),
        )
        cur.execute(
            "SELECT COUNT(*) AS c FROM idempotency_log WHERE idempotency_key = %s::uuid AND consumer = %s",
            (key, "warehouse"),
        )
        assert cur.fetchone()["c"] == 1


class TestCoordinator:
    """Distributed transaction coordinator tracks saga steps."""

    def test_step_advancement(self, payment_service, coordinator, db):
        cur, keys = db
        event = PaymentEvent(
            sender_account="C1", receiver_account="C2", amount=Decimal("10.00")
        )
        keys.append(event.idempotency_key)
        payment_service.create_payment(event)

        coordinator.advance(event.idempotency_key, TransactionStep.KAFKA_PUBLISHED)
        state = coordinator.get_status(event.idempotency_key)
        assert state["kafka_published"] is True
        assert state["current_step"] == TransactionStep.KAFKA_PUBLISHED.value

    def test_completed_when_all_steps_done(self, payment_service, coordinator, db):
        cur, keys = db
        event = PaymentEvent(
            sender_account="D1", receiver_account="D2", amount=Decimal("99.00")
        )
        keys.append(event.idempotency_key)
        payment_service.create_payment(event)

        for step in (
            TransactionStep.KAFKA_PUBLISHED,
            TransactionStep.WAREHOUSE_WRITTEN,
            TransactionStep.NOTIFICATION_SENT,
        ):
            coordinator.advance(event.idempotency_key, step)

        state = coordinator.get_status(event.idempotency_key)
        assert state["current_step"] == "COMPLETED"
        assert state["completed_at"] is not None

    def test_failure_recorded(self, payment_service, coordinator, db):
        cur, keys = db
        event = PaymentEvent(
            sender_account="E1", receiver_account="E2", amount=Decimal("5.00")
        )
        keys.append(event.idempotency_key)
        payment_service.create_payment(event)

        coordinator.record_failure(
            event.idempotency_key,
            TransactionStep.KAFKA_PUBLISHED,
            "connection refused",
            permanent=True,
        )

        state = coordinator.get_status(event.idempotency_key)
        assert state["current_step"] == "FAILED"
        assert "connection refused" in state["error_message"]

    def test_compensation(self, payment_service, coordinator, db):
        cur, keys = db
        event = PaymentEvent(
            sender_account="F1", receiver_account="F2", amount=Decimal("200.00")
        )
        keys.append(event.idempotency_key)
        payment_service.create_payment(event)

        coordinator.compensate(event.idempotency_key, "test compensation")

        # Ledger row should be COMPENSATED
        cur.execute(
            "SELECT status FROM ledger WHERE idempotency_key = %s::uuid",
            (event.idempotency_key,),
        )
        row = cur.fetchone()
        assert row["status"] == "COMPENSATED"

        # Saga step should be FAILED (final state after compensation)
        state = coordinator.get_status(event.idempotency_key)
        assert state["current_step"] == "FAILED"


class TestOutbox:
    """Outbox entries are unpublished on creation and marked after relay."""

    def test_outbox_starts_unpublished(self, payment_service, db):
        cur, keys = db
        event = PaymentEvent(
            sender_account="G1", receiver_account="G2", amount=Decimal("1.00")
        )
        keys.append(event.idempotency_key)
        payment_service.create_payment(event)

        cur.execute(
            "SELECT published_at FROM outbox WHERE idempotency_key = %s::uuid",
            (event.idempotency_key,),
        )
        row = cur.fetchone()
        assert row is not None
        assert row["published_at"] is None, "outbox should be unpublished initially"
