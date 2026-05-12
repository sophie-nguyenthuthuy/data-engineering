"""Kappa stream processor: unified consumer for both replay and live events."""

from __future__ import annotations

import json
import logging
import threading
import time
from pathlib import Path
from typing import Callable

from src.config import HISTORICAL_DIR, LOCAL_KAFKA_FILE, config
from src.kappa_arch.replay_manager import ProcessorMode
from src.kappa_arch.state_store import StateStore
from src.lambda_arch.models import Event

logger = logging.getLogger(__name__)


class KappaProcessor:
    """Unified stream processor that handles REPLAY and LIVE event modes.

    In REPLAY mode the processor drains all historical events from the replay
    topic (or local file) before switching to LIVE mode.  After a successful
    replay the state store holds the complete historical state, and subsequent
    live events are applied incrementally on top.
    """

    def __init__(
        self,
        local_mode: bool | None = None,
        state_store: StateStore | None = None,
    ) -> None:
        self.local_mode = config.local_mode if local_mode is None else local_mode
        self.state = state_store or StateStore()
        self.mode = ProcessorMode.LIVE
        self._stop_event = threading.Event()
        self._thread: threading.Thread | None = None

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def process_event(self, event: Event) -> None:
        """Apply a single event to the state store."""
        self.state.apply_event(
            hour=event.hour_bucket(),
            user_id=event.user_id,
            event_type=event.event_type,
            amount=event.amount,
        )

    def process_events(self, events: list[Event]) -> None:
        """Apply a list of events sequentially."""
        for event in events:
            self.process_event(event)

    def run_replay(self, historical_dir: Path = HISTORICAL_DIR) -> int:
        """Synchronously replay all historical events and return the event count.

        After this call completes the processor is ready to switch to LIVE mode.
        """
        from src.kappa_arch.replay_manager import ReplayManager

        self.mode = ProcessorMode.REPLAY
        self.state.reset()
        logger.info("KappaProcessor: starting REPLAY mode")

        manager = ReplayManager(historical_dir=historical_dir, local_mode=True, rate=0)
        count = 0
        for event in manager.iter_events():
            self.process_event(event)
            count += 1

        self.mode = ProcessorMode.LIVE
        logger.info("KappaProcessor: REPLAY complete (%d events), switching to LIVE", count)
        return count

    def start_live(self, on_message: Callable[[Event], None] | None = None) -> None:
        """Start consuming live events in a background thread."""
        self.mode = ProcessorMode.LIVE
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
        logger.info("KappaProcessor live consumer started (local_mode=%s)", self.local_mode)

    def stop(self) -> None:
        """Stop the live consumer thread."""
        self._stop_event.set()
        if self._thread:
            self._thread.join(timeout=5)
        logger.info("KappaProcessor stopped")

    def get_results(self) -> dict:
        """Return a snapshot of all current aggregation results."""
        return self.state.snapshot()

    # ------------------------------------------------------------------
    # Local-mode consumer (live events from JSONL file)
    # ------------------------------------------------------------------

    def _consume_local(self, on_message: Callable[[Event], None] | None) -> None:
        """Tail the local JSONL file for live events."""
        position = 0
        while not self._stop_event.is_set():
            if LOCAL_KAFKA_FILE.exists():
                with open(LOCAL_KAFKA_FILE) as fh:
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
    # Kafka consumer (live events from Kafka topic)
    # ------------------------------------------------------------------

    def _consume_kafka(self, on_message: Callable[[Event], None] | None) -> None:
        """Consume live events from Kafka."""
        try:
            from kafka import KafkaConsumer  # type: ignore[import]
        except ImportError:
            logger.error("kafka-python not installed; cannot start Kafka consumer")
            return

        consumer = KafkaConsumer(
            config.kafka.topic_live,
            bootstrap_servers=config.kafka.brokers,
            group_id=config.kafka.consumer_group_kappa,
            auto_offset_reset=config.kafka.auto_offset_reset,
            value_deserializer=lambda b: json.loads(b.decode("utf-8")),
        )
        logger.info("KappaProcessor Kafka consumer started on topic '%s'", config.kafka.topic_live)
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
