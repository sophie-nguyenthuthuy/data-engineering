"""
Streaming processor: consumes raw events from Kafka, applies the same
transformation functions as the batch path, then writes to:
  1. The online store (Redis) — latest feature vector per entity
  2. The production distribution ring-buffer — for nightly drift detection
"""
from __future__ import annotations

import json
import logging
import os
import signal
import sys
import time

from confluent_kafka import Consumer, KafkaError, KafkaException

from feature_store.online_store import OnlineStore
from feature_store.registry import FeatureType
from feature_store.transformations import build_registry

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

KAFKA_TOPIC = "raw-events"
CONSUMER_GROUP = "feature-store-stream-processor"


class StreamProcessor:
    def __init__(
        self,
        bootstrap_servers: str | None = None,
        online_store: OnlineStore | None = None,
    ) -> None:
        self.registry = build_registry()
        self.online_store = online_store or OnlineStore()
        self._running = True

        bs = bootstrap_servers or os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
        self._consumer = Consumer({
            "bootstrap.servers": bs,
            "group.id": CONSUMER_GROUP,
            "auto.offset.reset": "earliest",
            "enable.auto.commit": True,
        })
        self._consumer.subscribe([KAFKA_TOPIC])

        signal.signal(signal.SIGTERM, self._shutdown)
        signal.signal(signal.SIGINT, self._shutdown)

    def _shutdown(self, *_) -> None:
        logger.info("Shutting down stream processor…")
        self._running = False

    # ------------------------------------------------------------------
    # Per-event processing (uses the shared transform functions)
    # ------------------------------------------------------------------

    def _process_event(self, record: dict) -> None:
        # Load global stats from Redis for z-score computation
        context = self.online_store.get_global_stats()

        features: dict = {}
        for feat in self.registry.all_features():
            value = feat.compute(record, context)
            features[feat.name] = value

            # Feed production distribution ring-buffer
            self.online_store.push_feature_value(feat.name, value)

        entity_id = str(record.get("user_id", "unknown"))
        self.online_store.set_features(entity_id, features)

    # ------------------------------------------------------------------
    # Main loop
    # ------------------------------------------------------------------

    def run(self) -> None:
        logger.info("Stream processor started; consuming from topic '%s'", KAFKA_TOPIC)
        processed = 0
        try:
            while self._running:
                msg = self._consumer.poll(timeout=1.0)
                if msg is None:
                    continue
                if msg.error():
                    if msg.error().code() == KafkaError._PARTITION_EOF:
                        continue
                    raise KafkaException(msg.error())

                try:
                    record = json.loads(msg.value().decode("utf-8"))
                    self._process_event(record)
                    processed += 1
                    if processed % 1000 == 0:
                        logger.info("Processed %d events", processed)
                except (json.JSONDecodeError, Exception) as exc:
                    logger.warning("Failed to process event: %s", exc)
        finally:
            self._consumer.close()
            logger.info("Stream processor stopped after %d events", processed)


def main() -> None:
    processor = StreamProcessor()
    processor.run()


if __name__ == "__main__":
    main()
