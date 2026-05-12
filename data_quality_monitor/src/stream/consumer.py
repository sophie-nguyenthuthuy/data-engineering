from __future__ import annotations
import asyncio
import json
from collections.abc import AsyncIterator
from typing import Any

import structlog
from confluent_kafka import Consumer, KafkaError, KafkaException

from ..config import settings
from ..models import MicroBatch, BatchMetadata

log = structlog.get_logger(__name__)


class KafkaBatchConsumer:
    """
    Async wrapper around a confluent-kafka Consumer.

    Yields MicroBatch objects from the configured input topic.
    Messages are expected to be JSON-serialised MicroBatch payloads.
    """

    def __init__(self) -> None:
        self._conf: dict[str, Any] = {
            "bootstrap.servers": settings.kafka_bootstrap_servers,
            "group.id": settings.kafka_consumer_group,
            "auto.offset.reset": "earliest",
            "enable.auto.commit": False,
            "max.poll.interval.ms": 300_000,
            "session.timeout.ms": 30_000,
        }
        self._consumer: Consumer | None = None
        self._running = False

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    def _ensure_consumer(self) -> Consumer:
        if self._consumer is None:
            self._consumer = Consumer(self._conf)
            self._consumer.subscribe([settings.kafka_input_topic])
            log.info("kafka_consumer_subscribed", topic=settings.kafka_input_topic)
        return self._consumer

    async def close(self) -> None:
        self._running = False
        if self._consumer:
            self._consumer.close()
            self._consumer = None
            log.info("kafka_consumer_closed")

    # ------------------------------------------------------------------
    # Streaming
    # ------------------------------------------------------------------

    async def stream_batches(self) -> AsyncIterator[MicroBatch]:
        """
        Continuously poll Kafka and yield decoded MicroBatch objects.

        Offsets are committed only after successful downstream processing
        (caller is responsible for calling `commit` on the consumer after
        each successful yield).
        """
        self._running = True
        consumer = self._ensure_consumer()

        while self._running:
            # Non-blocking poll; yield control back to the event loop each tick
            msg = await asyncio.get_event_loop().run_in_executor(
                None, consumer.poll, settings.kafka_batch_timeout_ms / 1000
            )

            if msg is None:
                continue

            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    log.debug("partition_eof", partition=msg.partition())
                    continue
                raise KafkaException(msg.error())

            batch = self._decode(msg.value())
            if batch is not None:
                log.info(
                    "batch_received",
                    batch_id=batch.batch_id,
                    table=batch.metadata.table_name,
                    rows=batch.row_count,
                )
                yield batch
                # Commit after caller has processed the batch
                consumer.commit(message=msg, asynchronous=False)

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _decode(raw: bytes | None) -> MicroBatch | None:
        if not raw:
            return None
        try:
            payload = json.loads(raw.decode("utf-8"))
            return MicroBatch.model_validate(payload)
        except Exception as exc:
            log.warning("batch_decode_failed", error=str(exc))
            return None
