"""
Feature event producer — publishes feature updates to Kafka.

Message schema (JSON):
{
  "group":      "user_features",
  "entity_id":  "user_123",
  "features":   {"total_purchases": 42, "churn_risk_score": 0.12},
  "event_ts":   1714000000000   # epoch ms
}
"""
from __future__ import annotations

import json
import time
from typing import Any

import structlog
from confluent_kafka import Producer

log = structlog.get_logger(__name__)


class FeatureProducer:
    def __init__(
        self,
        bootstrap_servers: str = "localhost:9092",
        topic: str = "feature-events",
        **kafka_kwargs: Any,
    ) -> None:
        self._topic = topic
        conf = {
            "bootstrap.servers": bootstrap_servers,
            "acks": "1",
            "linger.ms": "0",          # no batching delay
            "compression.type": "snappy",
            "queue.buffering.max.messages": 100_000,
            **kafka_kwargs,
        }
        self._producer = Producer(conf)

    def publish(
        self,
        group: str,
        entity_id: str,
        features: dict[str, Any],
        event_ts_ms: int | None = None,
    ) -> None:
        msg = {
            "group": group,
            "entity_id": entity_id,
            "features": features,
            "event_ts": event_ts_ms or int(time.time() * 1000),
        }
        self._producer.produce(
            topic=self._topic,
            key=f"{group}:{entity_id}".encode(),
            value=json.dumps(msg).encode(),
            on_delivery=self._on_delivery,
        )
        # poll to trigger delivery callbacks without blocking
        self._producer.poll(0)

    def publish_batch(
        self, events: list[tuple[str, str, dict[str, Any]]]
    ) -> None:
        """Publish many events; flushes at end."""
        ts_ms = int(time.time() * 1000)
        for group, entity_id, features in events:
            msg = {
                "group": group,
                "entity_id": entity_id,
                "features": features,
                "event_ts": ts_ms,
            }
            self._producer.produce(
                topic=self._topic,
                key=f"{group}:{entity_id}".encode(),
                value=json.dumps(msg).encode(),
            )
        self._producer.flush()

    def flush(self, timeout: float = 10.0) -> None:
        self._producer.flush(timeout=timeout)

    @staticmethod
    def _on_delivery(err: Any, msg: Any) -> None:
        if err:
            log.error("kafka delivery failed", error=str(err))
        else:
            log.debug(
                "kafka delivered",
                topic=msg.topic(),
                partition=msg.partition(),
                offset=msg.offset(),
            )
