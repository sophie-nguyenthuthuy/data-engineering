"""Replay cursor — a stateful pointer into a topic.

The cursor remembers ``(segment_index, byte_pos)`` and surfaces a
``Record | EndOfLog`` on each :meth:`next` call so the consumer can
distinguish "nothing more right now" from "EOF on a closed log".
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from collections.abc import Iterator

    from sire.log.record import Record
    from sire.log.topic import Topic


@dataclass(frozen=True, slots=True)
class EndOfLog:
    """Sentinel returned when no more records are currently available."""

    last_offset: int


@dataclass
class Cursor:
    """Stateful cursor over a :class:`Topic`."""

    topic: Topic
    segment_index: int = 0
    byte_pos: int = 0
    last_offset: int = -1  # offset of the most-recently emitted record
    _iter: Iterator[Record] | None = field(default=None, repr=False)

    def __post_init__(self) -> None:
        if self.segment_index < 0:
            raise ValueError("segment_index must be ≥ 0")
        if self.byte_pos < 0:
            raise ValueError("byte_pos must be ≥ 0")

    # ------------------------------------------------------------- ops

    def next(self) -> Record | EndOfLog:
        if self._iter is None:
            self._iter = self.topic.iter_from(self.segment_index, self.byte_pos)
        try:
            record = next(self._iter)
        except StopIteration:
            return EndOfLog(last_offset=self.last_offset)
        self.last_offset = record.offset
        return record

    def rewind(self, segment_index: int = 0, byte_pos: int = 0) -> None:
        self.segment_index = segment_index
        self.byte_pos = byte_pos
        self._iter = None
        self.last_offset = -1


__all__ = ["Cursor", "EndOfLog"]
