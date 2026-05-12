"""Batch layer: reads historical events from local files and computes aggregate views."""

from __future__ import annotations

import json
import logging
from pathlib import Path
from typing import Iterable

import pandas as pd

from src.config import HISTORICAL_DIR, config
from src.lambda_arch.models import (
    BatchView,
    Event,
    EventTypeSummary,
    HourlyEventCounts,
    UserTotals,
)

logger = logging.getLogger(__name__)


class BatchProcessor:
    """Reads all JSON event files from data/historical/ and builds a BatchView."""

    def __init__(self, historical_dir: Path = HISTORICAL_DIR) -> None:
        self.historical_dir = historical_dir

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def run(self) -> BatchView:
        """Execute the full batch job and return the resulting BatchView."""
        logger.info("Starting batch processing from %s", self.historical_dir)
        events = list(self._load_events())
        logger.info("Loaded %d events for batch processing", len(events))
        view = self._compute_aggregates(events)
        logger.info("Batch processing complete")
        return view

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _load_events(self) -> Iterable[Event]:
        """Yield Event objects from all JSON files in the historical directory."""
        json_files = sorted(self.historical_dir.glob("*.json"))
        if not json_files:
            logger.warning("No JSON files found in %s", self.historical_dir)
            return

        for path in json_files:
            logger.debug("Loading events from %s", path)
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

    def _compute_aggregates(self, events: list[Event]) -> BatchView:
        """Build a BatchView from a list of events using pandas for aggregation."""
        if not events:
            return BatchView()

        df = pd.DataFrame(
            [
                {
                    "event_id": e.event_id,
                    "user_id": e.user_id,
                    "event_type": e.event_type,
                    "amount": e.amount,
                    "timestamp": e.timestamp,
                    "hour_bucket": e.hour_bucket(),
                }
                for e in events
            ]
        )

        view = BatchView()

        # --- hourly_event_counts ---
        hourly_counts = (
            df.groupby(["hour_bucket", "event_type"]).size().reset_index(name="count")
        )
        hourly_ec = HourlyEventCounts()
        for _, row in hourly_counts.iterrows():
            hourly_ec.increment(str(row["hour_bucket"]), str(row["event_type"]), int(row["count"]))
        view.hourly_event_counts = hourly_ec

        # --- user_totals ---
        user_agg = (
            df.groupby("user_id")
            .agg(total_amount=("amount", "sum"), event_count=("event_id", "count"))
            .reset_index()
        )
        user_totals = UserTotals()
        for _, row in user_agg.iterrows():
            user_totals.update(str(row["user_id"]), float(row["total_amount"]), int(row["event_count"]))
        view.user_totals = user_totals

        # --- event_type_summary ---
        et_agg = (
            df.groupby("event_type")
            .agg(count=("event_id", "count"), total_amount=("amount", "sum"))
            .reset_index()
        )
        et_summary = EventTypeSummary()
        for _, row in et_agg.iterrows():
            cnt = int(row["count"])
            total = float(row["total_amount"])
            et_summary.data[str(row["event_type"])] = {
                "count": cnt,
                "total_amount": total,
                "avg_amount": total / cnt if cnt else 0.0,
            }
        view.event_type_summary = et_summary

        return view
