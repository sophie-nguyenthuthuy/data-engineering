"""
Warehouse Consumer — idempotently writes payment events to the analytics
warehouse (warehouse_payments table).

Exactly-once guarantee:
  - Reads only committed Kafka messages (isolation.level=read_committed)
  - Checks idempotency_log before inserting
  - INSERT … ON CONFLICT DO NOTHING as a second safety net
  - Kafka offset committed only after successful DB write
"""
from __future__ import annotations

from decimal import Decimal
from typing import Any

import structlog

from src.config import settings
from src.consumers.base_consumer import BaseConsumer
from src.db import transaction
from src.models import TransactionStep
from src.recovery.failure_injector import FailureInjector

log = structlog.get_logger(__name__)


class WarehouseConsumer(BaseConsumer):
    consumer_name = "warehouse"
    group_id = settings.kafka_consumer_group_warehouse
    transaction_step = TransactionStep.WAREHOUSE_WRITTEN

    def __init__(self, failure_injector: FailureInjector | None = None) -> None:
        super().__init__(failure_injector)

    def process(self, idempotency_key: str, payload: dict[str, Any]) -> None:
        with transaction() as cur:
            cur.execute(
                """
                INSERT INTO warehouse_payments
                    (payment_id, idempotency_key, sender_account, receiver_account,
                     amount, currency, description, event_metadata)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (idempotency_key) DO NOTHING
                """,
                (
                    payload["payment_id"],
                    idempotency_key,
                    payload["sender_account"],
                    payload["receiver_account"],
                    Decimal(str(payload["amount"])),
                    payload.get("currency", "USD"),
                    payload.get("description", ""),
                    "{}",
                ),
            )

        log.info("warehouse.written",
                 payment_id=payload["payment_id"],
                 idempotency_key=idempotency_key)
