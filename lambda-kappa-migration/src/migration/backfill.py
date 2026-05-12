"""Backfill job: reads historical events and publishes them to Kafka (or local file)."""

from __future__ import annotations

import json
import logging
import time
from pathlib import Path

from src.config import HISTORICAL_DIR, LOCAL_KAFKA_FILE, config
from src.kappa_arch.replay_manager import ReplayManager
from src.lambda_arch.models import Event

logger = logging.getLogger(__name__)


class BackfillJob:
    """Orchestrates the backfill of historical events into the Kafka replay topic.

    Preserves original event timestamps so the stream processor sees events in
    their natural chronological order, enabling deterministic state recomputation.
    """

    def __init__(
        self,
        historical_dir: Path = HISTORICAL_DIR,
        local_mode: bool | None = None,
        rate: int | None = None,
    ) -> None:
        self.historical_dir = historical_dir
        self.local_mode = config.local_mode if local_mode is None else local_mode
        self.rate = rate if rate is not None else config.backfill_rate

    def run(self) -> dict[str, int]:
        """Execute the backfill; returns stats dict with event count."""
        logger.info(
            "BackfillJob starting: source=%s, local_mode=%s, rate=%d/s",
            self.historical_dir,
            self.local_mode,
            self.rate,
        )
        manager = ReplayManager(
            historical_dir=self.historical_dir,
            local_mode=self.local_mode,
            rate=self.rate,
        )
        topic = config.kafka.topic_replay
        published = manager.replay(topic=topic)
        stats = {"events_published": published, "topic": topic}  # type: ignore[assignment]
        logger.info("BackfillJob complete: %s", stats)
        return stats

    def dry_run(self) -> dict[str, int]:
        """Count events without publishing; useful for preflight checks."""
        count = 0
        for _ in ReplayManager(
            historical_dir=self.historical_dir,
            local_mode=True,
            rate=0,
        ).iter_events():
            count += 1
        return {"events_found": count}
