from __future__ import annotations
import asyncio
import json
from collections.abc import AsyncIterator

import structlog
from confluent_kafka import Consumer, KafkaError, KafkaException

from ..config import settings
from ..models import FeatureVector, BatchMetadata
from ..models.feature import FeatureVector  # noqa: F811

log = structlog.get_logger(__name__)


class ServingEventConsumer:
    """
    Consumes serving-request events from Kafka.
    Each message is expected to be a JSON-serialised FeatureVector.

    Yields FeatureVector objects into the async pipeline for drift monitoring.
    """

    def __init__(self) -> None:
        self._consumer: Consumer | None = None
        self._running = False

    def _ensure_consumer(self) -> Consumer:
        if self._consumer is None:
            self._consumer = Consumer({
                "bootstrap.servers": settings.kafka_bootstrap_servers,
                "group.id": settings.kafka_consumer_group,
                "auto.offset.reset": "latest",
                "enable.auto.commit": False,
            })
            self._consumer.subscribe([settings.kafka_serving_topic])
            log.info("serving_consumer_subscribed", topic=settings.kafka_serving_topic)
        return self._consumer

    async def stream(self) -> AsyncIterator[FeatureVector]:
        self._running = True
        consumer = self._ensure_consumer()
        while self._running:
            msg = await asyncio.get_event_loop().run_in_executor(
                None, consumer.poll, 1.0
            )
            if msg is None:
                continue
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                raise KafkaException(msg.error())
            vec = self._decode(msg.value())
            if vec:
                consumer.commit(message=msg, asynchronous=False)
                yield vec

    async def close(self) -> None:
        self._running = False
        if self._consumer:
            self._consumer.close()

    @staticmethod
    def _decode(raw: bytes | None) -> FeatureVector | None:
        if not raw:
            return None
        try:
            return FeatureVector.model_validate(json.loads(raw.decode()))
        except Exception as exc:
            log.warning("serving_event_decode_failed", error=str(exc))
            return None
