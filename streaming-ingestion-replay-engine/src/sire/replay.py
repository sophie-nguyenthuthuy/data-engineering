"""Replay engine — the public façade.

A :class:`ReplayEngine` ties a topic + transform + sink + offset store
together. Three replay entry points:

  * ``from_beginning`` — start at offset 0.
  * ``from_offset(N)`` — start at offset N.
  * ``from_timestamp(t)`` — start at the earliest record with
    ``record.timestamp >= t``.

A fourth, ``from_committed(group)``, resumes where the named consumer
group last committed via the :class:`OffsetStore`.
"""

from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
from typing import TYPE_CHECKING

from sire.log.cursor import Cursor, EndOfLog
from sire.transforms.base import SKIP

if TYPE_CHECKING:
    from sire.log.topic import Topic
    from sire.offsets import OffsetStore
    from sire.sinks.base import Sink
    from sire.transforms.base import Transform


class ReplayPosition(str, Enum):
    """Where to start a replay."""

    BEGINNING = "beginning"
    OFFSET = "offset"
    TIMESTAMP = "timestamp"
    COMMITTED = "committed"


@dataclass
class ReplayEngine:
    """Drive a :class:`Topic` through transform → sink."""

    topic: Topic
    sink: Sink
    transform: Transform | None = None
    offsets: OffsetStore | None = None

    def from_beginning(self, *, max_records: int | None = None) -> int:
        return self._run(Cursor(topic=self.topic), max_records=max_records)

    def from_offset(self, offset: int, *, max_records: int | None = None) -> int:
        if offset < 0:
            raise ValueError("offset must be ≥ 0")
        pos = self.topic.seek_offset(offset)
        if pos is None:
            return 0
        seg_idx, byte_pos = pos
        return self._run(
            Cursor(topic=self.topic, segment_index=seg_idx, byte_pos=byte_pos),
            max_records=max_records,
        )

    def from_timestamp(self, timestamp: int, *, max_records: int | None = None) -> int:
        pos = self.topic.seek_timestamp(timestamp)
        if pos is None:
            return 0
        seg_idx, byte_pos = pos
        return self._run(
            Cursor(topic=self.topic, segment_index=seg_idx, byte_pos=byte_pos),
            max_records=max_records,
        )

    def from_committed(self, group: str, *, max_records: int | None = None) -> int:
        if self.offsets is None:
            raise RuntimeError("from_committed requires an OffsetStore")
        nxt = self.offsets.get(group=group, topic=self.topic.name)
        return self.from_offset(nxt, max_records=max_records)

    # ----------------------------------------------------------- internal

    def _run(self, cursor: Cursor, *, max_records: int | None) -> int:
        n_emitted = 0
        last_offset = -1
        while True:
            if max_records is not None and n_emitted >= max_records:
                break
            item = cursor.next()
            if isinstance(item, EndOfLog):
                break
            last_offset = item.offset
            transformed = item if self.transform is None else self.transform.apply(item)
            if transformed is SKIP:
                continue
            assert hasattr(transformed, "offset")
            self.sink.write(transformed)  # type: ignore[arg-type]
            n_emitted += 1
        self.sink.flush()
        # If we have an offset store, the caller can commit explicitly
        # via offsets.commit(); we don't auto-commit so a transform
        # failure mid-batch doesn't accidentally advance the watermark.
        _ = last_offset
        return n_emitted


__all__ = ["ReplayEngine", "ReplayPosition"]
