"""Streaming binlog reader.

Consumes a byte stream that begins with the four-byte binlog magic
``\\xfe\\x62\\x69\\x6e``, then iterates ``(header, payload)`` event
pairs and dispatches the payload to the typed decoder for events we
recognise.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import IO, TYPE_CHECKING

if TYPE_CHECKING:
    from collections.abc import Iterator

from lcdc.mysql.events import (
    EventType,
    QueryEvent,
    RotateEvent,
    RowsEvent,
    RowsEventKind,
    TableMapEvent,
    XidEvent,
)
from lcdc.mysql.header import HEADER_LEN, EventHeader

MAGIC = b"\xfe\x62\x69\x6e"  # "\xfebin"


class MagicHeaderError(ValueError):
    """Raised when the binlog stream does not start with the magic header."""


DecodedEvent = RotateEvent | QueryEvent | XidEvent | TableMapEvent | RowsEvent | bytes


@dataclass
class BinlogReader:
    """Iterator over decoded events from a binlog byte stream."""

    stream: IO[bytes]
    require_magic: bool = True
    _magic_consumed: bool = False

    def __iter__(self) -> Iterator[tuple[EventHeader, DecodedEvent]]:
        if self.require_magic and not self._magic_consumed:
            self._consume_magic()
        while True:
            head = self.stream.read(HEADER_LEN)
            if not head:
                return
            if len(head) < HEADER_LEN:
                raise ValueError(f"truncated event header: {len(head)} bytes")
            header = EventHeader.decode(head)
            payload_len = header.payload_size
            payload = self.stream.read(payload_len) if payload_len > 0 else b""
            if len(payload) < payload_len:
                raise ValueError(
                    f"truncated event payload: expected {payload_len} got {len(payload)}"
                )
            yield header, _dispatch(header, payload)

    # ----------------------------------------------------------- private

    def _consume_magic(self) -> None:
        magic = self.stream.read(4)
        if magic != MAGIC:
            raise MagicHeaderError(f"bad binlog magic: {magic!r} != {MAGIC!r}")
        self._magic_consumed = True


def _dispatch(header: EventHeader, payload: bytes) -> DecodedEvent:
    et = header.event_type
    if et == EventType.ROTATE.value:
        return RotateEvent.decode(payload)
    if et == EventType.QUERY.value:
        return QueryEvent.decode(payload)
    if et == EventType.XID.value:
        return XidEvent.decode(payload)
    if et == EventType.TABLE_MAP.value:
        return TableMapEvent.decode(payload)
    if et == EventType.WRITE_ROWS_V2.value:
        return RowsEvent.decode(payload, RowsEventKind.INSERT)
    if et == EventType.UPDATE_ROWS_V2.value:
        return RowsEvent.decode(payload, RowsEventKind.UPDATE)
    if et == EventType.DELETE_ROWS_V2.value:
        return RowsEvent.decode(payload, RowsEventKind.DELETE)
    return payload  # unknown event — return the raw bytes


__all__ = ["MAGIC", "BinlogReader", "MagicHeaderError"]
