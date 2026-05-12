"""Time-window utilities."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone

from dateutil.parser import parse as parse_dt

from replay.models import TimeWindow

MAX_WINDOW_DAYS = 30


def window_from_days_ago(days: int, end: datetime | None = None) -> TimeWindow:
    """Create a window from N days ago until *end* (default: now)."""
    if days < 1 or days > MAX_WINDOW_DAYS:
        raise ValueError(f"days must be between 1 and {MAX_WINDOW_DAYS}, got {days}")
    end = end or datetime.now(tz=timezone.utc)
    start = end - timedelta(days=days)
    return TimeWindow(start=start, end=end)


def parse_window(start: str, end: str) -> TimeWindow:
    """Parse ISO-8601 strings into a TimeWindow."""
    s = _ensure_tz(parse_dt(start))
    e = _ensure_tz(parse_dt(end))
    return TimeWindow(start=s, end=e)


def split_window(window: TimeWindow, chunk_hours: int = 24) -> list[TimeWindow]:
    """Split a large window into smaller chunks for incremental processing."""
    chunks: list[TimeWindow] = []
    current = window.start
    delta = timedelta(hours=chunk_hours)
    while current < window.end:
        chunk_end = min(current + delta, window.end)
        chunks.append(TimeWindow(start=current, end=chunk_end))
        current = chunk_end
    return chunks


def _ensure_tz(dt: datetime) -> datetime:
    if dt.tzinfo is None:
        return dt.replace(tzinfo=timezone.utc)
    return dt
