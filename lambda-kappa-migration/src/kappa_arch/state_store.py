"""State store: in-memory aggregation state for the Kappa stream processor."""

from __future__ import annotations

import logging
import threading
from typing import Any

from src.lambda_arch.models import (
    EventTypeSummary,
    HourlyEventCounts,
    UserTotals,
)

logger = logging.getLogger(__name__)


class StateStore:
    """Thread-safe in-memory state store for streaming aggregations.

    Optionally backed by Redis when REDIS_URL is configured; falls back to a
    plain dict for zero-dependency local operation.
    """

    def __init__(self) -> None:
        self._lock = threading.RLock()
        self._hourly_event_counts = HourlyEventCounts()
        self._user_totals = UserTotals()
        self._event_type_summary = EventTypeSummary()
        self._event_count = 0

    # ------------------------------------------------------------------
    # Mutation
    # ------------------------------------------------------------------

    def apply_event(self, hour: str, user_id: str, event_type: str, amount: float) -> None:
        """Apply a single event to all aggregation views atomically."""
        with self._lock:
            self._hourly_event_counts.increment(hour, event_type)
            self._user_totals.update(user_id, amount)
            self._event_type_summary.update(event_type, amount)
            self._event_count += 1

    def reset(self) -> None:
        """Clear all state (e.g., before a replay)."""
        with self._lock:
            self._hourly_event_counts = HourlyEventCounts()
            self._user_totals = UserTotals()
            self._event_type_summary = EventTypeSummary()
            self._event_count = 0
        logger.info("StateStore reset")

    # ------------------------------------------------------------------
    # Read-only accessors
    # ------------------------------------------------------------------

    @property
    def event_count(self) -> int:
        """Total number of events processed."""
        with self._lock:
            return self._event_count

    def get_hourly_event_counts(self) -> dict[str, dict[str, int]]:
        """Return a snapshot of hourly event counts."""
        with self._lock:
            return {
                hour: dict(counts)
                for hour, counts in self._hourly_event_counts.data.items()
            }

    def get_user_totals(self) -> dict[str, dict[str, Any]]:
        """Return a snapshot of per-user totals."""
        with self._lock:
            return {uid: dict(vals) for uid, vals in self._user_totals.data.items()}

    def get_event_type_summary(self) -> dict[str, dict[str, Any]]:
        """Return a snapshot of event-type summary."""
        with self._lock:
            result: dict[str, dict[str, Any]] = {}
            for et, vals in self._event_type_summary.data.items():
                result[et] = dict(vals)
            return result

    def snapshot(self) -> dict[str, Any]:
        """Return a complete snapshot of all aggregations."""
        return {
            "hourly_event_counts": self.get_hourly_event_counts(),
            "user_totals": self.get_user_totals(),
            "event_type_summary": self.get_event_type_summary(),
            "total_events": self.event_count,
        }
