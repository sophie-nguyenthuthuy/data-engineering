"""
Pytest fixtures.

Integration tests require the Docker Compose stack to be running:
    docker compose up -d
"""
from __future__ import annotations

import json
import os
import uuid

import psycopg2
import psycopg2.extras
import pytest

DSN = os.getenv(
    "TEST_POSTGRES_DSN",
    "postgresql://pipeline:pipeline@localhost:5432/payments",
)


@pytest.fixture(scope="session")
def raw_conn():
    conn = psycopg2.connect(DSN)
    conn.autocommit = True
    yield conn
    conn.close()


@pytest.fixture()
def db(raw_conn):
    """Return a cursor and clean up affected rows after each test."""
    idempotency_keys: list[str] = []
    cur = raw_conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

    yield cur, idempotency_keys

    # Clean up by idempotency key
    for key in idempotency_keys:
        for table in [
            "notification_log", "warehouse_payments", "idempotency_log",
            "outbox", "transaction_states", "ledger",
        ]:
            cur.execute(
                f"DELETE FROM {table} WHERE idempotency_key = %s::uuid",
                (key,),
            )
    cur.close()


@pytest.fixture()
def payment_service(monkeypatch):
    # Override DSN for test isolation
    monkeypatch.setenv("POSTGRES_DSN", DSN)
    from src.payment_service import PaymentService
    return PaymentService()


@pytest.fixture()
def coordinator():
    from src.coordinator.transaction_coordinator import TransactionCoordinator
    return TransactionCoordinator()
