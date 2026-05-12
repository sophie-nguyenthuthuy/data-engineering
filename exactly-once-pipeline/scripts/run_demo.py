#!/usr/bin/env python3
"""
Demo script — runs all pipeline components in threads and submits
sample payments, then displays the coordinator state for each one.

Usage:
    python scripts/run_demo.py
    python scripts/run_demo.py --fail kafka      # inject Kafka failures
    python scripts/run_demo.py --fail warehouse  # inject warehouse failures
    python scripts/run_demo.py --fail notification
"""
from __future__ import annotations

import argparse
import json
import sys
import threading
import time
import uuid
from decimal import Decimal
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

import structlog

from src.config import settings
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


def make_injector(step: str) -> FailureInjector:
    if step:
        log.warning("demo.failure_injection_enabled", step=step, rate=0.5)
        return FailureInjector(step=step, rate=0.5)
    return FailureInjector()


def run_in_thread(name: str, fn: callable) -> threading.Thread:
    t = threading.Thread(target=fn, name=name, daemon=True)
    t.start()
    return t


def print_status(coordinator: TransactionCoordinator, keys: list[str]) -> None:
    print("\n" + "=" * 70)
    print(f"{'PAYMENT':^40} {'STEP':^20} {'K':^3} {'W':^3} {'N':^3}")
    print("=" * 70)
    for key in keys:
        state = coordinator.get_status(key)
        if not state:
            print(f"{key[:36]:<40} {'NOT FOUND':^20}")
            continue
        pid = str(state["payment_id"])[:8]
        step = str(state["current_step"])
        k = "✓" if state["kafka_published"] else "·"
        w = "✓" if state["warehouse_ack"] else "·"
        n = "✓" if state["notification_ack"] else "·"
        retries = state["retry_count"]
        r = f" (retries={retries})" if retries else ""
        print(f"  {pid}…{key[:8]}  {step:<24}{r:>10}  {k}  {w}  {n}")
    print("=" * 70)
    print("  K=Kafka  W=Warehouse  N=Notification  ✓=done  ·=pending")


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--fail", default="", choices=["", "kafka", "warehouse", "notification"])
    parser.add_argument("--count", type=int, default=5)
    args = parser.parse_args()

    injector = make_injector(args.fail)
    svc = PaymentService()
    coord = TransactionCoordinator()

    # ── Start pipeline components in background threads ───────────────
    poller = OutboxPoller(failure_injector=injector if args.fail == "kafka" else None)
    wc = WarehouseConsumer(failure_injector=injector if args.fail == "warehouse" else None)
    nc = NotificationConsumer(failure_injector=injector if args.fail == "notification" else None)

    run_in_thread("outbox-poller", poller.start)
    run_in_thread("warehouse-consumer", wc.start)
    run_in_thread("notification-consumer", nc.start)

    log.info("demo.components_started")
    time.sleep(3)   # let consumers warm up

    # ── Submit payments ───────────────────────────────────────────────
    idempotency_keys: list[str] = []
    for i in range(args.count):
        event = PaymentEvent(
            sender_account=f"ACC-{1000 + i}",
            receiver_account=f"ACC-{2000 + i}",
            amount=Decimal(f"{(i + 1) * 100}.00"),
            description=f"Demo payment #{i + 1}",
        )
        state = svc.create_payment(event)
        idempotency_keys.append(event.idempotency_key)
        log.info("demo.payment_submitted",
                 payment_id=event.payment_id,
                 idempotency_key=event.idempotency_key,
                 amount=str(event.amount))
        time.sleep(0.2)

    # ── Wait and poll status ──────────────────────────────────────────
    log.info("demo.waiting_for_pipeline")
    for _ in range(12):
        time.sleep(5)
        print_status(coord, idempotency_keys)
        all_done = all(
            (s := coord.get_status(k)) and s["current_step"] in ("COMPLETED", "FAILED")
            for k in idempotency_keys
        )
        if all_done:
            break

    print("\n✔  Demo complete.\n")


if __name__ == "__main__":
    main()
