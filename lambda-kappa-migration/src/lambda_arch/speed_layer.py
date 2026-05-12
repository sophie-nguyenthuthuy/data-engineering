"""Speed layer: Kafka (or local-file) consumer that maintains real-time views."""

from __future__ import annotations

import json
import logging
import threading
import time
from pathlib import Path
from typing import Callable

from src.config import LOCAL_KAFKA_FILE, config
from src.lambda_arch.models import Event, RealTimeView

logger = logging.getLogger(__name__)


class SpeedLayer:
    """Incremental consumer that keeps a RealTimeView up to date.

    In LOCAL_MODE it tails a local JSONL file instead of connecting to Kafka.
    """

    def __init__(self, local_mode: bool | None = None) -> None:
        self.local_mode: bool = config.local_mode if local_mode is None else local_mode
        self.view = RealTimeView()
        self._stop_event = threading.Event()
        self._thread: threading.Thread | None = None

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def process_event(self, event: Event) -> None:
        """Apply a single event to the real-time view (thread-safe)."""
        hour = event.hour_bucket()
        self.view.hourly_event_counts.increment(hour, event.event_type)
        self.view.user_totals.update(event.user_id, event.amount)
        self.view.event_type_summary.update(event.event_type, event.amount)

    def process_events(self, events: list[Event]) -> None:
        """Apply a batch of events sequentially."""
        for event in events:
            self.process_event(event)

    def start(self, on_message: Callable[[Event], None] | None = None) -> None:
        """Start the consumer in a background thread."""
        self._stop_event.clear()
        if self.local_mode:
            self._thread = threading.Thread(
                target=self._consume_local,
                args=(on_message,),
                daemon=True,
            )
        else:
            self._thread = threading.Thread(
                target=self._consume_kafka,
                args=(on_message,),
                daemon=True,
            )
        self._thread.start()
        logger.info("SpeedLayer started (local_mode=%s)", self.local_mode)

    def stop(self) -> None:
        """Signal the consumer to stop and wait for the thread."""
        self._stop_event.set()
        if self._thread:
            self._thread.join(timeout=5)
        logger.info("SpeedLayer stopped")

    def get_view(self) -> RealTimeView:
        """Return the current real-time view snapshot."""
        return self.view

    # ------------------------------------------------------------------
    # Local-mode consumer
    # ------------------------------------------------------------------

    def _consume_local(self, on_message: Callable[[Event], None] | None) -> None:
        """Tail the local JSONL file, processing new lines as they appear."""
        local_file: Path = LOCAL_KAFKA_FILE
        if not local_file.exists():
            logger.debug("Local Kafka file does not exist yet: %s", local_file)

        position = 0
        while not self._stop_event.is_set():
            if local_file.exists():
                with open(local_file) as fh:
                    fh.seek(position)
                    for raw in fh:
                        raw = raw.strip()
                        if not raw:
                            continue
                        try:
                            record = json.loads(raw)
                            if record.get("topic") == config.kafka.topic_live:
                                event = Event.model_validate(record["payload"])
                                self.process_event(event)
                                if on_message:
                                    on_message(event)
                        except Exception as exc:  # noqa: BLE001
                            logger.warning("Skipping malformed local message: %s", exc)
                    position = fh.tell()
            time.sleep(0.1)

    # ------------------------------------------------------------------
    # Kafka consumer
    # ------------------------------------------------------------------

    def _consume_kafka(self, on_message: Callable[[Event], None] | None) -> None:
        """Consume from Kafka topic, applying each event to the real-time view."""
        try:
            from kafka import KafkaConsumer  # type: ignore[import]
        except ImportError:
            logger.error("kafka-python not installed; cannot start Kafka consumer")
            return

        consumer = KafkaConsumer(
            config.kafka.topic_live,
            bootstrap_servers=config.kafka.brokers,
            group_id=config.kafka.consumer_group_speed,
            auto_offset_reset=config.kafka.auto_offset_reset,
            value_deserializer=lambda b: json.loads(b.decode("utf-8")),
        )
        logger.info("SpeedLayer Kafka consumer started on topic '%s'", config.kafka.topic_live)
        try:
            for msg in consumer:
                if self._stop_event.is_set():
                    break
                try:
                    event = Event.model_validate(msg.value)
                    self.process_event(event)
                    if on_message:
                        on_message(event)
                except Exception as exc:  # noqa: BLE001
                    logger.warning("Skipping malformed Kafka message: %s", exc)
        finally:
            consumer.close()
