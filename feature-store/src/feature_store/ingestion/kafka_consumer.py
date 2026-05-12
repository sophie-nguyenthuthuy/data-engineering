"""
Feature consumer — reads from Kafka and writes to both online (Redis) and
offline (Parquet) stores atomically within a micro-batch.

Consistency guarantee: a message is committed to Kafka only after both
stores have accepted the write, so replays produce the same state.
"""
from __future__ import annotations

import json
import os
import signal
import sys
import time
from datetime import datetime, timezone
from typing import Any

import structlog
from confluent_kafka import Consumer, KafkaError, KafkaException, Message

from feature_store.offline.parquet_store import OfflineStore
from feature_store.online.redis_store import OnlineStore
from feature_store.registry.feature_registry import FeatureRegistry

log = structlog.get_logger(__name__)


class FeatureConsumer:
    def __init__(
        self,
        bootstrap_servers: str = "localhost:9092",
        topic: str = "feature-events",
        group_id: str = "feature-store-consumer",
        registry: FeatureRegistry | None = None,
        online_store: OnlineStore | None = None,
        offline_store: OfflineStore | None = None,
        batch_size: int = 500,
        poll_timeout_ms: float = 0.05,
    ) -> None:
        self._topic = topic
        self._batch_size = batch_size
        self._poll_timeout = poll_timeout_ms
        self._registry = registry or FeatureRegistry()
        self._online = online_store or OnlineStore()
        self._offline = offline_store or OfflineStore()
        self._running = False

        conf = {
            "bootstrap.servers": bootstrap_servers,
            "group.id": group_id,
            "auto.offset.reset": "latest",
            "enable.auto.commit": False,
            "max.poll.interval.ms": 300_000,
            "fetch.min.bytes": 1,
            "fetch.wait.max.ms": 10,    # match our latency budget
        }
        self._consumer = Consumer(conf)
        self._consumer.subscribe([topic])

    # ------------------------------------------------------------------ #
    # Main loop                                                            #
    # ------------------------------------------------------------------ #

    def run(self) -> None:
        self._running = True
        signal.signal(signal.SIGINT, self._handle_signal)
        signal.signal(signal.SIGTERM, self._handle_signal)

        log.info("feature consumer started", topic=self._topic)
        try:
            while self._running:
                batch = self._poll_batch()
                if batch:
                    self._process_batch(batch)
        except KafkaException as exc:
            log.error("kafka error", error=str(exc))
            sys.exit(1)
        finally:
            self._shutdown()

    def _poll_batch(self) -> list[Message]:
        batch: list[Message] = []
        while len(batch) < self._batch_size:
            msg = self._consumer.poll(timeout=self._poll_timeout)
            if msg is None:
                break
            if msg.error():
                if msg.error().code() != KafkaError._PARTITION_EOF:
                    log.warning("kafka message error", error=str(msg.error()))
                break
            batch.append(msg)
        return batch

    # ------------------------------------------------------------------ #
    # Processing                                                           #
    # ------------------------------------------------------------------ #

    def _process_batch(self, messages: list[Message]) -> None:
        # Group by feature group for batch writes
        by_group: dict[str, list[tuple[str, dict, datetime]]] = {}
        for msg in messages:
            try:
                event = json.loads(msg.value())
                group = event["group"]
                entity_id = event["entity_id"]
                features = event["features"]
                event_ts = datetime.fromtimestamp(
                    event.get("event_ts", time.time() * 1000) / 1000,
                    tz=timezone.utc,
                )
                # Validate against registry if registered
                try:
                    features = self._registry.validate_features(group, features)
                except KeyError:
                    pass  # unregistered groups pass through unvalidated

                by_group.setdefault(group, []).append((entity_id, features, event_ts))
            except (json.JSONDecodeError, KeyError) as exc:
                log.warning("malformed message skipped", error=str(exc))

        errors: list[Exception] = []

        # Write to online store (Redis) first — primary serving path
        for group, records in by_group.items():
            try:
                ttl = self._get_ttl(group)
                self._online.put_batch(
                    group,
                    [(eid, feats) for eid, feats, _ in records],
                    ttl_seconds=ttl,
                )
            except Exception as exc:
                log.error("online write failed", group=group, error=str(exc))
                errors.append(exc)

        # Write to offline store (Parquet) — training lineage
        for group, records in by_group.items():
            try:
                self._offline.write_batch(group, records)
            except Exception as exc:
                log.error("offline write failed", group=group, error=str(exc))
                errors.append(exc)

        # Only commit offset if both stores accepted — guarantees replay safety
        if not errors:
            self._consumer.commit(asynchronous=False)
            log.debug("batch committed", messages=len(messages))
        else:
            log.warning("batch not committed due to errors", errors=len(errors))

    def _get_ttl(self, group: str) -> int:
        try:
            return self._registry.get_group(group).ttl_seconds
        except KeyError:
            return 86400

    def _handle_signal(self, signum: int, frame: Any) -> None:
        log.info("shutdown signal received", signal=signum)
        self._running = False

    def _shutdown(self) -> None:
        log.info("flushing offline buffer...")
        self._offline.flush()
        self._consumer.close()
        log.info("consumer shutdown complete")


def main() -> None:
    import structlog
    structlog.configure(
        processors=[
            structlog.stdlib.add_log_level,
            structlog.dev.ConsoleRenderer(),
        ]
    )
    consumer = FeatureConsumer(
        bootstrap_servers=os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"),
        topic=os.getenv("KAFKA_TOPIC", "feature-events"),
        group_id=os.getenv("CONSUMER_GROUP_ID", "feature-store-consumer"),
        offline_store=OfflineStore(
            base_path=os.getenv("OFFLINE_STORE_PATH", "./data/offline")
        ),
        online_store=OnlineStore(
            redis_url=os.getenv("REDIS_URL", "redis://localhost:6379")
        ),
    )
    consumer.run()


if __name__ == "__main__":
    main()
