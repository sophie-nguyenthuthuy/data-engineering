"""Pydantic data models shared across the Lambda architecture layers."""

from __future__ import annotations

from datetime import datetime
from typing import Any

from pydantic import BaseModel, Field


class Event(BaseModel):
    """A single domain event flowing through the pipeline."""

    event_id: str
    user_id: str
    event_type: str  # "purchase", "view", "click", "signup"
    amount: float
    timestamp: datetime
    metadata: dict[str, Any] = Field(default_factory=dict)

    def hour_bucket(self) -> str:
        """Return ISO-formatted hour bucket string, e.g. '2024-01-01T14'."""
        return self.timestamp.strftime("%Y-%m-%dT%H")


class HourlyEventCounts(BaseModel):
    """Hourly event counts per event type: {hour_bucket: {event_type: count}}."""

    data: dict[str, dict[str, int]] = Field(default_factory=dict)

    def increment(self, hour: str, event_type: str, delta: int = 1) -> None:
        """Increment the count for a given hour and event type."""
        self.data.setdefault(hour, {}).setdefault(event_type, 0)
        self.data[hour][event_type] += delta


class UserTotals(BaseModel):
    """Per-user aggregation: {user_id: {total_amount, event_count}}."""

    data: dict[str, dict[str, float | int]] = Field(default_factory=dict)

    def update(self, user_id: str, amount: float, count: int = 1) -> None:
        """Add an event's contribution to the user totals."""
        entry = self.data.setdefault(user_id, {"total_amount": 0.0, "event_count": 0})
        entry["total_amount"] = float(entry["total_amount"]) + amount
        entry["event_count"] = int(entry["event_count"]) + count


class EventTypeSummary(BaseModel):
    """Per-event-type summary: {event_type: {count, total_amount, avg_amount}}."""

    data: dict[str, dict[str, float | int]] = Field(default_factory=dict)

    def update(self, event_type: str, amount: float) -> None:
        """Add an event's contribution to the event-type summary."""
        entry = self.data.setdefault(
            event_type,
            {"count": 0, "total_amount": 0.0, "avg_amount": 0.0},
        )
        entry["count"] = int(entry["count"]) + 1
        entry["total_amount"] = float(entry["total_amount"]) + amount
        entry["avg_amount"] = float(entry["total_amount"]) / int(entry["count"])


class BatchView(BaseModel):
    """Immutable snapshot produced by the batch layer."""

    computed_at: datetime = Field(default_factory=datetime.utcnow)
    hourly_event_counts: HourlyEventCounts = Field(default_factory=HourlyEventCounts)
    user_totals: UserTotals = Field(default_factory=UserTotals)
    event_type_summary: EventTypeSummary = Field(default_factory=EventTypeSummary)


class RealTimeView(BaseModel):
    """Mutable view maintained by the speed layer."""

    updated_at: datetime = Field(default_factory=datetime.utcnow)
    hourly_event_counts: HourlyEventCounts = Field(default_factory=HourlyEventCounts)
    user_totals: UserTotals = Field(default_factory=UserTotals)
    event_type_summary: EventTypeSummary = Field(default_factory=EventTypeSummary)
