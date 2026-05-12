from __future__ import annotations
import json
from typing import Any

import structlog
from confluent_kafka import Producer

from ..config import settings
from ..models import ValidationResult

log = structlog.get_logger(__name__)


class KafkaResultProducer:
    """
    Publishes ValidationResult objects to the quality-results Kafka topic
    so that downstream consumers (alerting, lineage, etc.) can react.
    """

    def __init__(self) -> None:
        self._producer = Producer(
            {
                "bootstrap.servers": settings.kafka_bootstrap_servers,
                "acks": "all",
                "retries": 3,
                "retry.backoff.ms": 500,
                "compression.type": "snappy",
            }
        )

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    async def publish_result(self, result: ValidationResult) -> None:
        payload = result.model_dump_json().encode("utf-8")
        key = result.batch_id.encode("utf-8")

        self._producer.produce(
            topic=settings.kafka_results_topic,
            key=key,
            value=payload,
            on_delivery=self._delivery_callback,
        )
        # Flush after every message for at-least-once delivery guarantees
        self._producer.flush(timeout=10)
        log.info(
            "result_published",
            batch_id=result.batch_id,
            table=result.table_name,
            status=result.status,
        )

    def close(self) -> None:
        self._producer.flush()
        log.info("kafka_producer_closed")

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _delivery_callback(err: Any, msg: Any) -> None:
        if err:
            log.error("kafka_delivery_failed", error=str(err))
        else:
            log.debug(
                "kafka_delivery_ok",
                topic=msg.topic(),
                partition=msg.partition(),
                offset=msg.offset(),
            )
