"""Correction stream: emits a record whenever a late event updates a
window whose result was previously published.

Downstream consumers must idempotently apply (key, window_start, old, new)
to back-correct their materialised view.
"""

from __future__ import annotations

import threading
from dataclasses import dataclass, field
from typing import TYPE_CHECKING

from pwm.correction.window import TumblingWindowState

if TYPE_CHECKING:
    from collections.abc import Callable


@dataclass(frozen=True, slots=True)
class CorrectionRecord:
    key: object
    window_start: float
    old_value: object
    new_value: object


@dataclass
class CorrectionStream:
    window: TumblingWindowState = field(default_factory=TumblingWindowState)
    _emitted: list[CorrectionRecord] = field(default_factory=list)
    _on_correction: Callable[[CorrectionRecord], None] | None = None
    _lock: threading.RLock = field(default_factory=threading.RLock)  # type: ignore[assignment]

    def submit_late(
        self,
        key: object,
        event_time: float,
        value: object,
        agg_fn: Callable[[object, object], object],
    ) -> CorrectionRecord | None:
        """Apply a late record to its (already-closed) window. Returns the
        emitted correction or None if the window wasn't closed yet."""
        ws = self.window._window_of(event_time)
        if not self.window.is_closed(key, ws):
            return None
        with self._lock:
            old = self.window.value(key, ws)
            if old is None:
                # Window was closed without any data; treat old as identity
                old = value
                new = value
            else:
                new = agg_fn(old, value)
            self.window.update_closed(key, ws, new)
            record = CorrectionRecord(key=key, window_start=ws, old_value=old, new_value=new)
            self._emitted.append(record)
        if self._on_correction is not None:
            self._on_correction(record)
        return record

    def on_correction(self, fn: Callable[[CorrectionRecord], None]) -> None:
        with self._lock:
            self._on_correction = fn

    @property
    def n_emitted(self) -> int:
        with self._lock:
            return len(self._emitted)
