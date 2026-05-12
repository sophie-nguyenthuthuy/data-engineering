"""Replay manager: reads historical files and publishes them to Kafka (or local file)."""

from __future__ import annotations

import json
import logging
import time
from enum import Enum
from pathlib import Path
from typing import Iterator

from src.config import HISTORICAL_DIR, LOCAL_KAFKA_FILE, config
from src.lambda_arch.models import Event

logger = logging.getLogger(__name__)


class ProcessorMode(str, Enum):
    """Operating mode for the Kappa stream processor."""

    REPLAY = "REPLAY"
    LIVE = "LIVE"


class ReplayManager:
    """Reads historical event files and publishes them to the replay Kafka topic.

    In LOCAL_MODE the events are written to a local JSONL file.
    """

    def __init__(
        self,
        historical_dir: Path = HISTORICAL_DIR,
        local_mode: bool | None = None,
        rate: int | None = None,
    ) -> None:
        self.historical_dir = historical_dir
        self.local_mode = config.local_mode if local_mode is None else local_mode
        self.rate = rate or config.backfill_rate  # events per second (0 = no rate limiting)

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def replay(self, topic: str | None = None) -> int:
        """Replay all historical events; returns the number of events published."""
        topic = topic or config.kafka.topic_replay
        events = list(self._load_events())
        total = len(events)
        logger.info("Replaying %d historical events to topic '%s'", total, topic)

        if self.local_mode:
            self._publish_local(events, topic)
        else:
            self._publish_kafka(events, topic)

        logger.info("Replay complete: %d events published", total)
        return total

    def iter_events(self) -> Iterator[Event]:
        """Iterate over all historical events in chronological order."""
        yield from self._load_events()

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _load_events(self) -> Iterator[Event]:
        """Yield events sorted by timestamp from all daily JSON files."""
        json_files = sorted(self.historical_dir.glob("*.json"))
        for path in json_files:
            try:
                with open(path) as fh:
                    records = json.load(fh)
                for record in records:
                    try:
                        yield Event.model_validate(record)
                    except Exception as exc:  # noqa: BLE001
                        logger.warning("Skipping invalid record in %s: %s", path.name, exc)
            except Exception as exc:  # noqa: BLE001
                logger.error("Failed to read %s: %s", path, exc)

    def _publish_local(self, events: list[Event], topic: str) -> None:
        """Write events to the local JSONL file, preserving original timestamps."""
        LOCAL_KAFKA_FILE.parent.mkdir(parents=True, exist_ok=True)
        delay = 1.0 / self.rate if self.rate > 0 else 0.0
        with open(LOCAL_KAFKA_FILE, "a") as fh:
            for event in events:
                record = {
                    "topic": topic,
                    "payload": json.loads(event.model_dump_json()),
                }
                fh.write(json.dumps(record) + "\n")
                if delay:
                    time.sleep(delay)

    def _publish_kafka(self, events: list[Event], topic: str) -> None:
        """Publish events to a Kafka topic, preserving original timestamps."""
        try:
            from kafka import KafkaProducer  # type: ignore[import]
        except ImportError:
            logger.error("kafka-python not installed; cannot publish to Kafka")
            return

        producer = KafkaProducer(
            bootstrap_servers=config.kafka.brokers,
            value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        )
        delay = 1.0 / self.rate if self.rate > 0 else 0.0
        try:
            for event in events:
                producer.send(topic, value=json.loads(event.model_dump_json()))
                if delay:
                    time.sleep(delay)
            producer.flush()
        finally:
            producer.close()
