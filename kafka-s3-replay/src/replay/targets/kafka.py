"""Kafka target — replays events into a Kafka cluster via confluent-kafka."""

from __future__ import annotations

import asyncio
import logging
from typing import Any

import anyio

from replay.models import Event, KafkaTargetConfig
from replay.targets.base import BaseTarget

logger = logging.getLogger(__name__)


class KafkaTarget(BaseTarget):
    """
    Publishes replayed events to a Kafka cluster.

    Topic mapping:
      - If config.topic_mapping contains an entry for the source topic, the event
        is produced to the mapped destination topic.
      - Otherwise the event is produced to the same topic name.

    The producer is flushed after every 500 messages or on close().
    """

    FLUSH_EVERY = 500

    def __init__(self, config: KafkaTargetConfig) -> None:
        self.config = config
        self._producer: Any = None
        self._sent = 0

    async def open(self) -> None:
        from confluent_kafka import Producer  # local import so non-kafka users don't need it

        conf: dict[str, Any] = {
            "bootstrap.servers": self.config.bootstrap_servers,
            "acks": "all",
            "enable.idempotence": True,
            "retries": 5,
            "retry.backoff.ms": 500,
        }
        conf.update(self.config.producer_config)
        self._producer = Producer(conf)
        logger.info("Kafka producer connected to %s", self.config.bootstrap_servers)

    async def send(self, event: Event) -> None:
        dest_topic = self.config.topic_mapping.get(event.topic, event.topic)
        headers = [(k, v) for k, v in event.headers.items()]
        # Add replay provenance headers
        headers.append(("x-replay-source-topic", event.topic.encode()))
        headers.append(("x-replay-source-offset", str(event.offset).encode()))
        headers.append(("x-replay-timestamp", event.timestamp.isoformat().encode()))

        await anyio.to_thread.run_sync(
            lambda: self._producer.produce(  # type: ignore[union-attr]
                topic=dest_topic,
                key=event.key,
                value=event.value,
                headers=headers,
                on_delivery=self._delivery_callback,
            )
        )
        self._sent += 1
        if self._sent % self.FLUSH_EVERY == 0:
            await self._flush()

    async def close(self) -> None:
        if self._producer:
            await self._flush()
            logger.info("Kafka producer closed. Total sent: %d", self._sent)

    async def _flush(self) -> None:
        await anyio.to_thread.run_sync(lambda: self._producer.flush(timeout=30))  # type: ignore[union-attr]

    @staticmethod
    def _delivery_callback(err: Any, msg: Any) -> None:
        if err:
            logger.error("Kafka delivery failed: %s", err)
        else:
            logger.debug("Delivered to %s [%d] @ %d", msg.topic(), msg.partition(), msg.offset())
