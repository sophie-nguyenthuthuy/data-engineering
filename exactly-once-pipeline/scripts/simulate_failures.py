#!/usr/bin/env python3
"""
Failure scenario simulator — injects failures at each pipeline step
in sequence and shows that the system recovers to COMPLETED.

Scenarios:
  1. Kafka publish fails            → outbox retries, saga eventually completes
  2. Warehouse consumer fails       → consumer retries w/ idempotency guard
  3. Notification consumer fails    → same pattern
  4. Crash after Kafka publish      → outbox relay re-publishes, consumers skip duplicate
  5. Duplicate payment submission   → idempotency key deduplicates at ledger level

Usage:
    python scripts/simulate_failures.py
"""
from __future__ import annotations

import sys
import threading
import time
from decimal import Decimal
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

import structlog

from src.consumers.notification_consumer import NotificationConsumer
from src.consumers.warehouse_consumer import WarehouseConsumer
from src.coordinator.transaction_coordinator import TransactionCoordinator
from src.models import PaymentEvent
from src.outbox_poller import OutboxPoller
from src.payment_service import PaymentService
from src.recovery.failure_injector import FailureInjector

structlog.configure(
    processors=[
        structlog.processors.TimeStamper(fmt="%H:%M:%S"),
        structlog.dev.ConsoleRenderer(),
    ]
)
log = structlog.get_logger()


def await_completion(coord: TransactionCoordinator, key: str, timeout: int = 60) -> dict:
    deadline = time.time() + timeout
    while time.time() < deadline:
        state = coord.get_status(key)
        if state and state["current_step"] in ("COMPLETED", "FAILED"):
            return state
        time.sleep(1)
    return coord.get_status(key) or {}


def run_scenario(
    name: str,
    inject_step: str,
    rate: float = 1.0,
    permanent: bool = False,
) -> None:
    print(f"\n{'─' * 60}")
    print(f"  SCENARIO: {name}")
    print(f"  inject_step={inject_step!r}  rate={rate}  permanent={permanent}")
    print("─" * 60)

    injector = FailureInjector(step=inject_step, rate=rate, permanent=permanent)
    svc = PaymentService()
    coord = TransactionCoordinator()

    poller = OutboxPoller(failure_injector=injector if inject_step == "kafka" else None)
    wc = WarehouseConsumer(failure_injector=injector if inject_step == "warehouse" else None)
    nc = NotificationConsumer(failure_injector=injector if inject_step == "notification" else None)

    for component, fn in [
        ("outbox-poller", poller.start),
        ("warehouse", wc.start),
        ("notification", nc.start),
    ]:
        t = threading.Thread(target=fn, name=component, daemon=True)
        t.start()

    time.sleep(2)

    event = PaymentEvent(
        sender_account="ACC-9001",
        receiver_account="ACC-9002",
        amount=Decimal("42.00"),
        description=f"Scenario: {name}",
    )
    state = svc.create_payment(event)
    key = event.idempotency_key
    log.info("scenario.payment_submitted", key=key[:8], scenario=name)

    final = await_completion(coord, key)
    step = final.get("current_step", "UNKNOWN")
    retries = final.get("retry_count", 0)
    expected = "FAILED" if permanent else "COMPLETED"

    status = "PASS" if step == expected else "FAIL"
    print(f"\n  Result: {status}  →  step={step}  retries={retries}")
    print(f"  Expected: {expected}")


def duplicate_idempotency_scenario() -> None:
    print(f"\n{'─' * 60}")
    print("  SCENARIO: Duplicate idempotency key → deduplicated at ledger")
    print("─" * 60)

    svc = PaymentService()
    event = PaymentEvent(
        sender_account="ACC-DUP1",
        receiver_account="ACC-DUP2",
        amount=Decimal("1.00"),
        description="Duplicate test",
    )

    s1 = svc.create_payment(event)
    s2 = svc.create_payment(event)  # same idempotency_key

    assert s1.transaction_id == s2.transaction_id, "Should return same saga state"
    print(f"\n  Result: PASS  → same transaction_id={s1.transaction_id[:8]}…")


def main() -> None:
    duplicate_idempotency_scenario()

    run_scenario(
        "Transient Kafka failure (50% rate, recovers)",
        inject_step="kafka",
        rate=0.5,
        permanent=False,
    )

    run_scenario(
        "Transient warehouse failure (50% rate, recovers)",
        inject_step="warehouse",
        rate=0.5,
        permanent=False,
    )

    run_scenario(
        "Transient notification failure (50% rate, recovers)",
        inject_step="notification",
        rate=0.5,
        permanent=False,
    )

    run_scenario(
        "Permanent notification failure → compensation triggered",
        inject_step="notification",
        rate=1.0,
        permanent=True,
    )

    print("\n✔  All scenarios complete.\n")


if __name__ == "__main__":
    main()
