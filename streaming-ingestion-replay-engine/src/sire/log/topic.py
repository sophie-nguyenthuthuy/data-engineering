"""Single-partition topic — an ordered list of segments.

A real Kafka topic has many partitions; the replay-engine model is
simpler: one topic = one logical partition. The :class:`Topic` keeps an
in-memory list of :class:`Segment` instances + a monotonically
increasing next-offset counter.
"""

from __future__ import annotations

import threading
import time
from dataclasses import dataclass, field
from typing import TYPE_CHECKING

from sire.log.record import Record
from sire.log.segment import Segment

if TYPE_CHECKING:
    from collections.abc import Iterator


@dataclass
class Topic:
    """One-partition topic with append + seek semantics."""

    name: str
    segment_size_records: int = 1_000
    _segments: list[Segment] = field(default_factory=list, repr=False)
    _next_offset: int = 0
    _lock: threading.RLock = field(default_factory=threading.RLock, repr=False)

    def __post_init__(self) -> None:
        if not self.name:
            raise ValueError("name must be non-empty")
        if self.segment_size_records < 1:
            raise ValueError("segment_size_records must be ≥ 1")

    # ------------------------------------------------------------ write

    def append(self, key: bytes, value: bytes, timestamp: int | None = None) -> int:
        """Append one record; return the assigned offset."""
        ts = int(time.time() * 1000) if timestamp is None else timestamp
        with self._lock:
            if not self._segments or len(self._segments[-1]) >= self.segment_size_records:
                self._segments.append(Segment())
            rec = Record(offset=self._next_offset, timestamp=ts, key=key, value=value)
            self._segments[-1].append(rec)
            self._next_offset += 1
            return rec.offset

    # ------------------------------------------------------------- read

    def __len__(self) -> int:
        with self._lock:
            return self._next_offset

    @property
    def next_offset(self) -> int:
        with self._lock:
            return self._next_offset

    def seek_offset(self, offset: int) -> tuple[int, int] | None:
        """Return ``(segment_index, byte_pos)`` for ``offset``, or ``None``."""
        if offset < 0:
            raise ValueError("offset must be ≥ 0")
        with self._lock:
            for i, seg in enumerate(self._segments):
                if seg.first_offset() is None:
                    continue
                if (last := seg.last_offset()) is None:
                    continue
                first = seg.first_offset()
                assert first is not None
                if first <= offset <= last:
                    return i, seg.byte_pos_at_offset(offset)
            return None

    def seek_timestamp(self, timestamp: int) -> tuple[int, int] | None:
        """Earliest record with ``record.timestamp >= timestamp``."""
        with self._lock:
            for i, seg in enumerate(self._segments):
                pos = seg.byte_pos_after_timestamp(timestamp)
                if pos is not None:
                    return i, pos
            return None

    def iter_from(self, segment_index: int, byte_pos: int) -> Iterator[Record]:
        """Iterate records starting at ``(segment_index, byte_pos)``."""
        with self._lock:
            if segment_index < 0 or segment_index >= len(self._segments):
                return
            # Snapshot segment list so an appender doesn't move it under us.
            tail = list(self._segments[segment_index:])
        # We must keep yielding even as new segments arrive; the cursor
        # picks that up on its next pass.
        first = True
        for seg in tail:
            yield from seg.iter_from(byte_pos if first else 0)
            first = False

    def segments(self) -> list[Segment]:
        with self._lock:
            return list(self._segments)


__all__ = ["Topic"]
