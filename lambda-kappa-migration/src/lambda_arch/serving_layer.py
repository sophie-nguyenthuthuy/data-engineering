"""Serving layer: merges batch view and real-time view to answer queries."""

from __future__ import annotations

import logging
from typing import Any

from src.lambda_arch.models import (
    BatchView,
    EventTypeSummary,
    HourlyEventCounts,
    RealTimeView,
    UserTotals,
)

logger = logging.getLogger(__name__)


class ServingLayer:
    """Merges the immutable BatchView with the mutable RealTimeView.

    The serving layer applies the speed-layer delta on top of the batch baseline
    to produce query results that are consistent up to the current moment.
    """

    def __init__(self, batch_view: BatchView, realtime_view: RealTimeView) -> None:
        self.batch_view = batch_view
        self.realtime_view = realtime_view

    # ------------------------------------------------------------------
    # Public query interface
    # ------------------------------------------------------------------

    def get_hourly_event_counts(self) -> dict[str, dict[str, int]]:
        """Return merged hourly event counts (batch + real-time)."""
        merged: dict[str, dict[str, int]] = {}
        self._merge_hourly(merged, self.batch_view.hourly_event_counts.data)
        self._merge_hourly(merged, self.realtime_view.hourly_event_counts.data)
        return merged

    def get_user_totals(self) -> dict[str, dict[str, Any]]:
        """Return merged per-user totals (batch + real-time)."""
        merged: dict[str, dict[str, Any]] = {}
        self._merge_user_totals(merged, self.batch_view.user_totals.data)
        self._merge_user_totals(merged, self.realtime_view.user_totals.data)
        return merged

    def get_event_type_summary(self) -> dict[str, dict[str, Any]]:
        """Return merged event-type summary (batch + real-time)."""
        merged: dict[str, dict[str, Any]] = {}
        self._merge_event_type_summary(merged, self.batch_view.event_type_summary.data)
        self._merge_event_type_summary(merged, self.realtime_view.event_type_summary.data)
        # Recompute averages after merging
        for entry in merged.values():
            cnt = int(entry.get("count", 0))
            entry["avg_amount"] = float(entry.get("total_amount", 0.0)) / cnt if cnt else 0.0
        return merged

    def query_user(self, user_id: str) -> dict[str, Any] | None:
        """Return aggregated data for a specific user, or None if unknown."""
        totals = self.get_user_totals()
        return totals.get(user_id)

    # ------------------------------------------------------------------
    # Merge helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _merge_hourly(
        target: dict[str, dict[str, int]],
        source: dict[str, dict[str, int]],
    ) -> None:
        for hour, counts in source.items():
            bucket = target.setdefault(hour, {})
            for event_type, count in counts.items():
                bucket[event_type] = bucket.get(event_type, 0) + count

    @staticmethod
    def _merge_user_totals(
        target: dict[str, dict[str, Any]],
        source: dict[str, dict[str, Any]],
    ) -> None:
        for user_id, vals in source.items():
            entry = target.setdefault(user_id, {"total_amount": 0.0, "event_count": 0})
            entry["total_amount"] = float(entry["total_amount"]) + float(vals.get("total_amount", 0.0))
            entry["event_count"] = int(entry["event_count"]) + int(vals.get("event_count", 0))

    @staticmethod
    def _merge_event_type_summary(
        target: dict[str, dict[str, Any]],
        source: dict[str, dict[str, Any]],
    ) -> None:
        for et, vals in source.items():
            entry = target.setdefault(et, {"count": 0, "total_amount": 0.0, "avg_amount": 0.0})
            entry["count"] = int(entry["count"]) + int(vals.get("count", 0))
            entry["total_amount"] = float(entry["total_amount"]) + float(vals.get("total_amount", 0.0))
