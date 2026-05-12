from __future__ import annotations

import threading
from dataclasses import dataclass, field
from typing import Any

from .clock import HybridLogicalClock, WallClock
from .timestamp import ZERO, HLCTimestamp


@dataclass
class Entry:
    key: str
    value: Any
    ts: HLCTimestamp
    node_id: str


class MetadataStore:
    """
    Key-value metadata store backed by a Hybrid Logical Clock.

    Every write is stamped with the node's HLC tick.  Causal reads are
    supported via `causal_get`: the store blocks until it has observed
    all events up to a caller-supplied minimum timestamp, ensuring the
    caller never reads stale data that predates something it already saw.
    """

    def __init__(self, clock: HybridLogicalClock | WallClock) -> None:
        self._clock = clock
        self._data: dict[str, Entry] = {}
        self._history: list[Entry] = []
        self._lock = threading.Lock()
        self._watermark_cv = threading.Condition(self._lock)

    # ------------------------------------------------------------------
    # Write path
    # ------------------------------------------------------------------

    def put(self, key: str, value: Any, remote_ts: HLCTimestamp | None = None) -> HLCTimestamp:
        """
        Write *key=value*.  If *remote_ts* is provided (replication), update
        the clock via `update` before stamping; otherwise tick locally.
        """
        with self._lock:
            ts = self._clock.update(remote_ts) if remote_ts else self._clock.tick()
            entry = Entry(key=key, value=value, ts=ts, node_id=self._clock.node_id)
            self._data[key] = entry
            self._history.append(entry)
            self._watermark_cv.notify_all()
            return ts

    # ------------------------------------------------------------------
    # Read path
    # ------------------------------------------------------------------

    def get(self, key: str) -> tuple[Any, HLCTimestamp] | None:
        """Immediate read — no causal guarantees."""
        with self._lock:
            entry = self._data.get(key)
            return (entry.value, entry.ts) if entry else None

    def causal_get(
        self,
        key: str,
        after: HLCTimestamp,
        timeout_s: float = 1.0,
    ) -> tuple[Any, HLCTimestamp] | None:
        """
        Read *key* only after this store has processed all events with
        timestamp >= *after*.  Returns None on timeout.

        This prevents stale reads: a client that wrote at timestamp T can
        call causal_get(key, after=T) and be guaranteed not to see an older
        value, even across replicas.
        """
        with self._watermark_cv:
            ok = self._watermark_cv.wait_for(
                lambda: self._has_seen(after),
                timeout=timeout_s,
            )
            if not ok:
                return None
            entry = self._data.get(key)
            return (entry.value, entry.ts) if entry else None

    def _has_seen(self, ts: HLCTimestamp) -> bool:
        """True if any stored entry has a timestamp >= ts (same key or any key)."""
        return self._clock.peek() >= ts

    # ------------------------------------------------------------------
    # Inspection
    # ------------------------------------------------------------------

    def history(self) -> list[Entry]:
        with self._lock:
            return list(self._history)

    def watermark(self) -> HLCTimestamp:
        return self._clock.peek()
