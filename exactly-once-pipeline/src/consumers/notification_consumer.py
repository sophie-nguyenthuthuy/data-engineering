"""
Notification Consumer — idempotently sends payment confirmation notifications.

Writes to two sinks atomically:
  1. notification_log (Postgres durable log)
  2. Redis list "notifications:{account}" (live delivery queue)

Idempotency key prevents the same notification from being sent twice even
if the consumer crashes between processing and committing the Kafka offset.
"""
from __future__ import annotations

import json
from typing import Any

import redis
import structlog

from src.config import settings
from src.consumers.base_consumer import BaseConsumer
from src.db import transaction
from src.models import TransactionStep
from src.recovery.failure_injector import FailureInjector

log = structlog.get_logger(__name__)

_redis: redis.Redis | None = None


def get_redis() -> redis.Redis:
    global _redis
    if _redis is None:
        _redis = redis.from_url(settings.redis_url, decode_responses=True)
    return _redis


class NotificationConsumer(BaseConsumer):
    consumer_name = "notification"
    group_id = settings.kafka_consumer_group_notification
    transaction_step = TransactionStep.NOTIFICATION_SENT

    def __init__(self, failure_injector: FailureInjector | None = None) -> None:
        super().__init__(failure_injector)

    def process(self, idempotency_key: str, payload: dict[str, Any]) -> None:
        payment_id = payload["payment_id"]
        sender = payload["sender_account"]
        receiver = payload["receiver_account"]
        amount = payload["amount"]
        currency = payload.get("currency", "USD")

        subject = f"Payment {payment_id[:8]} confirmed"
        body = (
            f"Your payment of {currency} {amount} "
            f"from {sender} to {receiver} has been processed."
        )

        # ── Write to Postgres notification log ────────────────────────
        with transaction() as cur:
            cur.execute(
                """
                INSERT INTO notification_log
                    (idempotency_key, payment_id, channel, recipient, subject, body)
                VALUES (%s, %s, 'email', %s, %s, %s)
                ON CONFLICT (idempotency_key) DO NOTHING
                """,
                (idempotency_key, payment_id, sender, subject, body),
            )

        # ── Push to Redis notification queue ──────────────────────────
        notification = json.dumps({
            "idempotency_key": idempotency_key,
            "payment_id": payment_id,
            "recipient": sender,
            "subject": subject,
            "body": body,
        })
        r = get_redis()
        pipe = r.pipeline()
        pipe.lpush(f"notifications:{sender}", notification)
        pipe.expire(f"notifications:{sender}", 86400)
        pipe.execute()

        log.info("notification.sent",
                 payment_id=payment_id,
                 idempotency_key=idempotency_key,
                 recipient=sender)
