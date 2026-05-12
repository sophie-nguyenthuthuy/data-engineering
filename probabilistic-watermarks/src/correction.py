"""Correction stream: late records → deltas applied to closed windows."""
from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass, field
from typing import Callable


@dataclass
class CorrectionStream:
    """Buffers late records and provides delta-update callbacks per (key, window)."""
    window_size: float = 60.0
    _closed_windows: dict = field(default_factory=lambda: defaultdict(dict))
    _on_correction: Callable | None = None

    def close_window(self, key, window_start: float, current_value):
        """Mark a window closed; future late records will produce corrections."""
        self._closed_windows[key][window_start] = current_value

    def _window_start(self, event_time: float) -> float:
        return (event_time // self.window_size) * self.window_size

    def submit_late(self, key, event_time: float, value, agg_fn: Callable):
        """A late record arrives. If its window is closed, emit a correction.

        agg_fn(old_value, new_value_to_add) → updated value.
        """
        ws = self._window_start(event_time)
        if ws not in self._closed_windows.get(key, {}):
            return None  # window not closed yet — should not get here
        old = self._closed_windows[key][ws]
        updated = agg_fn(old, value)
        self._closed_windows[key][ws] = updated
        correction = (key, ws, old, updated)
        if self._on_correction:
            self._on_correction(*correction)
        return correction

    def on_correction(self, fn: Callable):
        self._on_correction = fn


__all__ = ["CorrectionStream"]
