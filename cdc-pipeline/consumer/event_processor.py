"""
Reorder buffer for out-of-order CDC events.

Debezium can deliver events out of LSN order when:
  - Multiple Kafka partitions are consumed concurrently
  - Connector restarts cause re-delivery of overlapping windows
  - Network delays between Kafka brokers and consumers

Strategy:
  - Buffer events in a min-heap ordered by LSN
  - Watermark = min(max_lsn_seen per partition)  — guarantees all events
    below the watermark have been received from every partition
  - Also flush when wall-clock lag exceeds `lag_tolerance_ms` to handle
    stalled or idle partitions
  - Idempotency: warehouse_sink uses INSERT ... ON CONFLICT DO UPDATE,
    so duplicate delivery after a restart is safe
"""

import heapq
import logging
import time
import threading
from dataclasses import dataclass, field
from typing import Dict, List, Tuple

log = logging.getLogger(__name__)


@dataclass(order=True)
class _BufferedEvent:
    lsn: int
    ts_ms: int
    partition: int = field(compare=False)
    offset: int    = field(compare=False)
    topic: str     = field(compare=False)
    payload: dict  = field(compare=False)


class ReorderBuffer:
    """Thread-safe LSN-ordered event buffer with watermark-based flushing."""

    def __init__(self, lag_tolerance_ms: int = 30_000, max_buffer_size: int = 10_000):
        self._lag_tolerance_ms = lag_tolerance_ms
        self._max_buffer_size  = max_buffer_size
        self._heap: List[_BufferedEvent] = []
        # partition -> highest LSN seen on that partition
        self._partition_high_watermark: Dict[Tuple[str, int], int] = {}
        self._last_forced_flush = time.monotonic()
        self._lock = threading.Lock()
        self._stats = {"buffered": 0, "flushed": 0, "forced_flushes": 0}

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def add(self, topic: str, partition: int, offset: int, lsn: int, ts_ms: int, payload: dict) -> List[dict]:
        """Add an event; returns any events that are now ready to process."""
        with self._lock:
            event = _BufferedEvent(
                lsn=lsn, ts_ms=ts_ms,
                partition=partition, offset=offset,
                topic=topic, payload=payload,
            )
            heapq.heappush(self._heap, event)
            key = (topic, partition)
            self._partition_high_watermark[key] = max(
                self._partition_high_watermark.get(key, 0), lsn
            )
            self._stats["buffered"] += 1

            if len(self._heap) > self._max_buffer_size:
                log.warning("Buffer full (%d events) — forcing flush", len(self._heap))
                return self._flush_all()

            return self._flush_ready()

    def drain(self) -> List[dict]:
        """Flush all remaining buffered events (used on shutdown)."""
        with self._lock:
            return self._flush_all()

    def _flush_ready_public(self) -> List[dict]:
        """Public wrapper for periodic tick-based flushing (not a full drain)."""
        with self._lock:
            return self._flush_ready()

    @property
    def stats(self) -> dict:
        with self._lock:
            return {**self._stats, "buffered_now": len(self._heap)}

    # ------------------------------------------------------------------
    # Internal helpers (must be called with self._lock held)
    # ------------------------------------------------------------------

    def _safe_lsn(self) -> int:
        """LSN below which all partitions have been observed — safe to flush."""
        if not self._partition_high_watermark:
            return 0
        return min(self._partition_high_watermark.values())

    def _flush_ready(self) -> List[dict]:
        safe = self._safe_lsn()
        now  = time.monotonic()
        age_ms = (now - self._last_forced_flush) * 1000
        force  = age_ms >= self._lag_tolerance_ms

        if force:
            self._stats["forced_flushes"] += 1
            log.debug("Watermark lag %dms ≥ tolerance — forcing flush", age_ms)

        ready = []
        while self._heap:
            top = self._heap[0]
            if top.lsn <= safe or force:
                heapq.heappop(self._heap)
                ready.append(self._enrich(top))
            else:
                break

        if ready:
            self._stats["flushed"] += len(ready)
            self._last_forced_flush = now

        return ready

    def _flush_all(self) -> List[dict]:
        events = sorted(self._heap)
        self._heap.clear()
        self._stats["flushed"] += len(events)
        self._last_forced_flush = time.monotonic()
        return [self._enrich(e) for e in events]

    @staticmethod
    def _enrich(event: _BufferedEvent) -> dict:
        return {
            "_meta": {
                "topic":     event.topic,
                "partition": event.partition,
                "offset":    event.offset,
                "lsn":       event.lsn,
                "ts_ms":     event.ts_ms,
            },
            **event.payload,
        }
