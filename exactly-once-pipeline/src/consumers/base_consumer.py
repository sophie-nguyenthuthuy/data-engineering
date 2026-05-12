"""
Base Kafka consumer with:
  - At-least-once delivery (manual offset commit after successful processing)
  - Idempotency guard (checks idempotency_log before processing)
  - Dead-letter queue for permanently failed messages
  - Exponential back-off retries via tenacity
"""
from __future__ import annotations

import json
import signal
import time
from abc import ABC, abstractmethod
from typing import Any

import structlog
from confluent_kafka import Consumer, KafkaError, KafkaException, Producer
from tenacity import retry, stop_after_attempt, wait_exponential

from src.config import settings
from src.coordinator.transaction_coordinator import TransactionCoordinator
from src.db import transaction
from src.models import TransactionStep
from src.recovery.failure_injector import FailureInjector, PermanentFailure, TransientFailure

log = structlog.get_logger(__name__)

coordinator = TransactionCoordinator()


class BaseConsumer(ABC):
    consumer_name: str  # subclass must set
    group_id: str       # subclass must set
    transaction_step: TransactionStep  # which saga step this consumer completes

    def __init__(self, failure_injector: FailureInjector | None = None) -> None:
        self._running = False
        self._injector = failure_injector or FailureInjector()
        self._consumer = Consumer({
            "bootstrap.servers": settings.kafka_bootstrap_servers,
            "group.id": self.group_id,
            "auto.offset.reset": "earliest",
            "enable.auto.commit": False,   # manual commit for exactly-once
            "isolation.level": "read_committed",  # skip Kafka-transactional aborts
        })
        self._dlq_producer = Producer({
            "bootstrap.servers": settings.kafka_bootstrap_servers,
            "enable.idempotence": True,
        })

    # ── Lifecycle ──────────────────────────────────────────────────────
    def start(self) -> None:
        self._consumer.subscribe([settings.kafka_payment_topic])
        self._running = True
        signal.signal(signal.SIGINT, self._shutdown)
        signal.signal(signal.SIGTERM, self._shutdown)
        log.info(f"{self.consumer_name}.started")

        while self._running:
            msg = self._consumer.poll(timeout=1.0)
            if msg is None:
                continue
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                raise KafkaException(msg.error())
            self._handle(msg)

        self._consumer.close()

    def _shutdown(self, *_: Any) -> None:
        log.info(f"{self.consumer_name}.shutdown")
        self._running = False

    # ── Message handling ───────────────────────────────────────────────
    def _handle(self, msg: Any) -> None:
        try:
            envelope = json.loads(msg.value().decode())
        except Exception as exc:
            log.error(f"{self.consumer_name}.parse_error", error=str(exc))
            self._consumer.commit(message=msg)
            return

        idempotency_key = envelope.get("idempotency_key", "")
        payload = envelope.get("payload", {})

        # ── Idempotency check ──────────────────────────────────────────
        if self._already_processed(idempotency_key):
            log.info(f"{self.consumer_name}.duplicate_skipped",
                     idempotency_key=idempotency_key)
            self._consumer.commit(message=msg)
            return

        try:
            self._process_with_retry(idempotency_key, payload)
            self._mark_processed(idempotency_key)
            coordinator.advance(idempotency_key, self.transaction_step)
            self._consumer.commit(message=msg)
            log.info(f"{self.consumer_name}.processed",
                     idempotency_key=idempotency_key)

        except PermanentFailure as exc:
            log.error(f"{self.consumer_name}.permanent_failure",
                      idempotency_key=idempotency_key, error=str(exc))
            self._send_to_dlq(msg, str(exc))
            coordinator.record_failure(
                idempotency_key, self.transaction_step, str(exc), permanent=True
            )
            coordinator.compensate(idempotency_key, str(exc))
            self._consumer.commit(message=msg)

        except Exception as exc:
            log.error(f"{self.consumer_name}.transient_failure",
                      idempotency_key=idempotency_key, error=str(exc))
            coordinator.record_failure(
                idempotency_key, self.transaction_step, str(exc), permanent=False
            )
            # Do NOT commit — message will be re-delivered

    @retry(
        reraise=True,
        stop=stop_after_attempt(5),
        wait=wait_exponential(multiplier=0.5, min=0.5, max=30),
    )
    def _process_with_retry(self, idempotency_key: str, payload: dict[str, Any]) -> None:
        self._injector.maybe_raise(self.consumer_name, idempotency_key)
        self.process(idempotency_key, payload)

    # ── Idempotency log ───────────────────────────────────────────────
    def _already_processed(self, idempotency_key: str) -> bool:
        with transaction() as cur:
            cur.execute(
                "SELECT 1 FROM idempotency_log WHERE idempotency_key = %s AND consumer = %s",
                (idempotency_key, self.consumer_name),
            )
            return cur.fetchone() is not None

    def _mark_processed(self, idempotency_key: str) -> None:
        with transaction() as cur:
            cur.execute(
                """
                INSERT INTO idempotency_log (idempotency_key, consumer)
                VALUES (%s, %s)
                ON CONFLICT DO NOTHING
                """,
                (idempotency_key, self.consumer_name),
            )

    # ── Dead-letter queue ─────────────────────────────────────────────
    def _send_to_dlq(self, original_msg: Any, error: str) -> None:
        dlq_payload = json.dumps({
            "original_topic": original_msg.topic(),
            "original_partition": original_msg.partition(),
            "original_offset": original_msg.offset(),
            "consumer": self.consumer_name,
            "error": error,
            "payload": original_msg.value().decode(),
        })
        self._dlq_producer.produce(
            topic=settings.kafka_dlq_topic,
            value=dlq_payload.encode(),
        )
        self._dlq_producer.flush()

    # ── Abstract: subclass implements domain logic ────────────────────
    @abstractmethod
    def process(self, idempotency_key: str, payload: dict[str, Any]) -> None:
        ...
