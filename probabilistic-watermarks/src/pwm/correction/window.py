"""Window-aggregate state — owns the value-per-window for a single key."""

from __future__ import annotations

import threading
from dataclasses import dataclass, field
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from collections.abc import Callable


@dataclass
class TumblingWindowState:
    """Per-(key, window) state with pluggable aggregation function.

    The agg function takes (old_value, new_record_value) → new_value.
    """

    window_size: float = 60.0
    _values: dict[tuple[object, float], object] = field(default_factory=dict)
    _closed: set[tuple[object, float]] = field(default_factory=set)
    _lock: threading.RLock = field(default_factory=threading.RLock)  # type: ignore[assignment]

    def _window_of(self, event_time: float) -> float:
        return (event_time // self.window_size) * self.window_size

    def add(self, key: object, event_time: float, value: object,
            agg_fn: Callable[[object, object], object]) -> tuple[float, object]:
        """Add a record; return (window_start, updated_value)."""
        ws = self._window_of(event_time)
        with self._lock:
            old = self._values.get((key, ws))
            new = value if old is None else agg_fn(old, value)
            self._values[(key, ws)] = new
            return ws, new

    def close(self, key: object, window_start: float) -> None:
        with self._lock:
            self._closed.add((key, window_start))

    def is_closed(self, key: object, window_start: float) -> bool:
        with self._lock:
            return (key, window_start) in self._closed

    def value(self, key: object, window_start: float) -> object | None:
        with self._lock:
            return self._values.get((key, window_start))

    def update_closed(self, key: object, window_start: float, new_value: object) -> object | None:
        """Update a closed window's value. Returns the prior value."""
        with self._lock:
            old = self._values.get((key, window_start))
            self._values[(key, window_start)] = new_value
            return old
