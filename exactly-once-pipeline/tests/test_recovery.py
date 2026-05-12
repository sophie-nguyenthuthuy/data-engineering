"""
Tests for failure injection and recovery behaviour.
"""
from __future__ import annotations

import uuid
from decimal import Decimal
from unittest.mock import MagicMock, patch

import pytest

from src.recovery.failure_injector import FailureInjector, PermanentFailure, TransientFailure


class TestFailureInjector:
    def test_no_failure_when_step_mismatch(self):
        injector = FailureInjector(step="kafka", rate=1.0)
        injector.maybe_raise("warehouse", "key-1")  # should not raise

    def test_transient_failure_raised(self):
        injector = FailureInjector(step="kafka", rate=1.0)
        with pytest.raises(TransientFailure):
            injector.maybe_raise("kafka", "key-1")

    def test_permanent_failure_raised(self):
        injector = FailureInjector(step="warehouse", rate=1.0, permanent=True)
        with pytest.raises(PermanentFailure):
            injector.maybe_raise("warehouse", "key-2")

    def test_zero_rate_never_raises(self):
        injector = FailureInjector(step="kafka", rate=0.0)
        for _ in range(100):
            injector.maybe_raise("kafka", "key-3")  # should never raise

    def test_reset_clears_call_counts(self):
        injector = FailureInjector(step="kafka", rate=1.0)
        try:
            injector.maybe_raise("kafka", "key-4")
        except TransientFailure:
            pass
        assert "key-4" in injector._call_count
        injector.reset()
        assert "key-4" not in injector._call_count


class TestIdempotencyGuardInConsumer:
    """Unit-tests the base consumer's duplicate detection."""

    def test_duplicate_message_skipped(self):
        from src.consumers.base_consumer import BaseConsumer
        from src.models import TransactionStep

        class DummyConsumer(BaseConsumer):
            consumer_name = "warehouse"
            group_id = "test-group"
            transaction_step = TransactionStep.WAREHOUSE_WRITTEN
            process_calls: int = 0

            def process(self, key, payload):
                DummyConsumer.process_calls += 1

        consumer = DummyConsumer()

        # Simulate already_processed returning True
        with patch.object(consumer, "_already_processed", return_value=True):
            mock_msg = MagicMock()
            mock_msg.value.return_value = b'{"idempotency_key": "abc", "payload": {}}'
            consumer._consumer = MagicMock()
            consumer._handle(mock_msg)

        assert DummyConsumer.process_calls == 0


class TestRecoveryAgent:
    """Recovery agent correctly identifies incomplete sagas."""

    def test_recover_incomplete_returns_stuck_sagas(self, payment_service, coordinator, db):
        cur, keys = db
        event_1 = __import__("src.models", fromlist=["PaymentEvent"]).PaymentEvent(
            sender_account="R1", receiver_account="R2", amount=Decimal("7.00")
        )
        keys.append(event_1.idempotency_key)
        payment_service.create_payment(event_1)

        # Force the saga to appear old by backdating created_at
        cur.execute(
            """
            UPDATE transaction_states
            SET created_at = NOW() - INTERVAL '10 minutes'
            WHERE idempotency_key = %s::uuid
            """,
            (event_1.idempotency_key,),
        )

        incomplete = coordinator.recover_incomplete(max_age_minutes=5)
        keys_found = [str(r["idempotency_key"]) for r in incomplete]
        assert event_1.idempotency_key in keys_found
