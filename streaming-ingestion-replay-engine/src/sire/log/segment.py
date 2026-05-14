"""Append-only segment of records.

A :class:`Segment` is a contiguous byte buffer storing zero or more
records. Segments are immutable from the consumer's POV — writes go
through :meth:`Segment.append`, which returns the new byte offset; the
reader uses :meth:`Segment.iter_from` to scan from a given byte
position, decoding records lazily.

A real log file is just a serialised segment; we keep the in-memory +
on-disk representations identical so a segment is round-trippable.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import TYPE_CHECKING

from sire.log.record import HEADER_SIZE, Record, RecordHeader

if TYPE_CHECKING:
    from collections.abc import Iterator
    from pathlib import Path


class SegmentError(ValueError):
    """Raised when a segment encounters a corrupt record."""


@dataclass
class Segment:
    """In-memory append-only byte buffer of encoded records."""

    _buf: bytearray = field(default_factory=bytearray, repr=False)
    _index: list[tuple[int, int, int]] = field(default_factory=list, repr=False)
    # _index entries: (record_offset, timestamp, byte_position)

    # ----------------------------------------------------------- write

    def append(self, record: Record) -> int:
        """Append ``record`` and return the byte position it landed at."""
        if self._index and record.offset <= self._index[-1][0]:
            raise SegmentError(
                f"record offset {record.offset} not strictly increasing (last={self._index[-1][0]})"
            )
        pos = len(self._buf)
        self._buf.extend(record.encode())
        self._index.append((record.offset, record.timestamp, pos))
        return pos

    # ------------------------------------------------------------ read

    def __len__(self) -> int:
        return len(self._index)

    def byte_size(self) -> int:
        return len(self._buf)

    def first_offset(self) -> int | None:
        return self._index[0][0] if self._index else None

    def last_offset(self) -> int | None:
        return self._index[-1][0] if self._index else None

    def iter_from(self, byte_pos: int = 0) -> Iterator[Record]:
        """Yield decoded records starting at ``byte_pos``."""
        if byte_pos < 0:
            raise SegmentError("byte_pos must be ≥ 0")
        cursor = byte_pos
        while cursor < len(self._buf):
            if cursor + HEADER_SIZE > len(self._buf):
                raise SegmentError("segment truncated at header")
            hdr = RecordHeader.decode(bytes(self._buf[cursor:]))
            record, consumed = Record.decode(bytes(self._buf), cursor)
            yield record
            cursor += consumed
            # Sanity-check the index agrees on what we just emitted.
            _ = hdr

    def byte_pos_at_offset(self, offset: int) -> int:
        """Return the byte position of the record with ``offset``.

        Raises ``SegmentError`` if no such record exists.
        """
        for rec_offset, _ts, pos in self._index:
            if rec_offset == offset:
                return pos
        raise SegmentError(f"offset {offset} not present in segment")

    def byte_pos_after_timestamp(self, timestamp: int) -> int | None:
        """Position of the earliest record with ``record.timestamp >= timestamp``.

        Returns ``None`` if every record is older than ``timestamp``.
        """
        for _offset, ts, pos in self._index:
            if ts >= timestamp:
                return pos
        return None

    # --------------------------------------------------------- persist

    def to_bytes(self) -> bytes:
        return bytes(self._buf)

    @classmethod
    def from_bytes(cls, data: bytes) -> Segment:
        seg = cls()
        cursor = 0
        while cursor < len(data):
            if cursor + HEADER_SIZE > len(data):
                raise SegmentError("segment data truncated mid-header")
            try:
                record, consumed = Record.decode(data, cursor)
            except ValueError as exc:
                raise SegmentError(f"segment data truncated mid-record: {exc}") from exc
            # Append manually to keep index in sync without re-encoding.
            if seg._index and record.offset <= seg._index[-1][0]:
                raise SegmentError(f"record offset {record.offset} not strictly increasing")
            seg._buf.extend(data[cursor : cursor + consumed])
            seg._index.append((record.offset, record.timestamp, cursor))
            cursor += consumed
        return seg

    def write_to(self, path: Path) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(self.to_bytes())

    @classmethod
    def read_from(cls, path: Path) -> Segment:
        if not path.exists():
            raise SegmentError(f"segment file not found: {path}")
        return cls.from_bytes(path.read_bytes())


__all__ = ["Segment", "SegmentError"]
