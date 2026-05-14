"""Typed event variants parsed out of a MySQL binlog stream.

We implement the slice of MySQL Internals' event taxonomy that an
ingest pipeline actually needs:

  * ``ROTATE_EVENT`` (0x04) — binlog file rotation; carries the next
    file name and the position to seek to in it.
  * ``QUERY_EVENT`` (0x02) — DDL or transaction-control statement.
  * ``TABLE_MAP_EVENT`` (0x13) — per-table-id schema map; emitted
    immediately before each WRITE/UPDATE/DELETE_ROWS event.
  * ``WRITE_ROWS_EVENTv2`` (0x1E) — insert.
  * ``UPDATE_ROWS_EVENTv2`` (0x1F) — before+after image pair.
  * ``DELETE_ROWS_EVENTv2`` (0x20) — before image.
  * ``XID_EVENT`` (0x10) — transaction commit marker.

All decoders are pure functions over the **payload bytes** (header
already consumed) so the reader can dispatch on ``event_type`` and
delegate.
"""

from __future__ import annotations

import struct
from dataclasses import dataclass, field
from enum import IntEnum


class EventType(IntEnum):
    """Subset of MySQL 5.7+ event types we decode."""

    UNKNOWN = 0x00
    QUERY = 0x02
    ROTATE = 0x04
    FORMAT_DESCRIPTION = 0x0F
    XID = 0x10
    TABLE_MAP = 0x13
    WRITE_ROWS_V2 = 0x1E
    UPDATE_ROWS_V2 = 0x1F
    DELETE_ROWS_V2 = 0x20


# ---------------------------------------------------------------- helpers


def _read_lenenc_int(buf: bytes, offset: int) -> tuple[int, int]:
    """Decode a MySQL length-encoded integer; returns ``(value, new_offset)``."""
    if offset >= len(buf):
        raise ValueError("truncated length-encoded int")
    first = buf[offset]
    if first < 0xFB:
        return first, offset + 1
    if first == 0xFC:
        return int.from_bytes(buf[offset + 1 : offset + 3], "little"), offset + 3
    if first == 0xFD:
        return int.from_bytes(buf[offset + 1 : offset + 4], "little"), offset + 4
    if first == 0xFE:
        return int.from_bytes(buf[offset + 1 : offset + 9], "little"), offset + 9
    raise ValueError(f"invalid length-encoded prefix 0x{first:02x}")


def _read_lenenc_str(buf: bytes, offset: int) -> tuple[bytes, int]:
    length, off = _read_lenenc_int(buf, offset)
    return buf[off : off + length], off + length


# ----------------------------------------------------------------- ROTATE


@dataclass(frozen=True, slots=True)
class RotateEvent:
    """``ROTATE_EVENT`` — next-file pointer."""

    next_position: int
    next_file: str

    @classmethod
    def decode(cls, payload: bytes) -> RotateEvent:
        if len(payload) < 8:
            raise ValueError("rotate event too short")
        (pos,) = struct.unpack_from("<Q", payload, 0)
        name = payload[8:].decode("utf-8", errors="replace").rstrip("\x00")
        if not name:
            raise ValueError("rotate event missing next file name")
        return cls(next_position=pos, next_file=name)


# ----------------------------------------------------------------- QUERY


@dataclass(frozen=True, slots=True)
class QueryEvent:
    """``QUERY_EVENT`` — captures DDL + control statements."""

    slave_proxy_id: int
    execution_time: int
    error_code: int
    schema: str
    query: str

    @classmethod
    def decode(cls, payload: bytes) -> QueryEvent:
        # Post-header fixed part: 13 bytes.
        if len(payload) < 13:
            raise ValueError("query event too short")
        proxy_id, exec_time, schema_len, error_code, status_len = struct.unpack_from(
            "<IIBHH", payload, 0
        )
        cursor = 13 + status_len
        if cursor + schema_len + 1 > len(payload):
            raise ValueError("query event truncated at schema")
        schema = payload[cursor : cursor + schema_len].decode("utf-8", errors="replace")
        # +1 for the null byte after schema.
        cursor += schema_len + 1
        query = payload[cursor:].decode("utf-8", errors="replace")
        return cls(
            slave_proxy_id=proxy_id,
            execution_time=exec_time,
            error_code=error_code,
            schema=schema,
            query=query,
        )


# ----------------------------------------------------------------- XID


@dataclass(frozen=True, slots=True)
class XidEvent:
    """``XID_EVENT`` — transaction-commit marker (carries the XA xid)."""

    xid: int

    @classmethod
    def decode(cls, payload: bytes) -> XidEvent:
        if len(payload) < 8:
            raise ValueError("xid event too short")
        (xid,) = struct.unpack_from("<Q", payload, 0)
        return cls(xid=xid)


# ---------------------------------------------------------- TABLE_MAP


@dataclass(frozen=True, slots=True)
class TableMapEvent:
    """``TABLE_MAP_EVENT`` — maps a table id to its schema for row events."""

    table_id: int
    flags: int
    schema: str
    table: str
    column_types: tuple[int, ...]

    @classmethod
    def decode(cls, payload: bytes) -> TableMapEvent:
        if len(payload) < 8:
            raise ValueError("table map event too short")
        # 6-byte table id (little-endian) + 2-byte flags
        table_id = int.from_bytes(payload[0:6], "little")
        flags = int.from_bytes(payload[6:8], "little")
        cursor = 8
        if cursor + 1 > len(payload):
            raise ValueError("table map event truncated at schema length")
        schema_len = payload[cursor]
        cursor += 1
        schema = payload[cursor : cursor + schema_len].decode("utf-8", errors="replace")
        cursor += schema_len + 1  # +1 for null terminator
        if cursor + 1 > len(payload):
            raise ValueError("table map event truncated at table length")
        table_len = payload[cursor]
        cursor += 1
        table = payload[cursor : cursor + table_len].decode("utf-8", errors="replace")
        cursor += table_len + 1  # +1 for null terminator
        ncols, cursor = _read_lenenc_int(payload, cursor)
        if cursor + ncols > len(payload):
            raise ValueError("table map event truncated at column types")
        cols = tuple(payload[cursor : cursor + ncols])
        return cls(
            table_id=table_id,
            flags=flags,
            schema=schema,
            table=table,
            column_types=cols,
        )


# ----------------------------------------------------------------- ROWS


class RowsEventKind(IntEnum):
    """Which row-mutation flavour produced the event."""

    INSERT = EventType.WRITE_ROWS_V2.value
    UPDATE = EventType.UPDATE_ROWS_V2.value
    DELETE = EventType.DELETE_ROWS_V2.value


@dataclass(frozen=True, slots=True)
class RowsEvent:
    """``WRITE/UPDATE/DELETE_ROWS_EVENTv2`` decoded shell.

    We parse the v2 envelope (table id, flags, extra header, column
    count, included-columns bitmap) and surface the **raw image bytes**
    that follow. Decoding individual column values requires the prior
    :class:`TableMapEvent`'s schema; that responsibility lives on the
    consumer rather than this decoder.
    """

    kind: RowsEventKind
    table_id: int
    flags: int
    column_count: int
    included_columns: bytes
    image: bytes
    after_image: bytes | None = field(default=None)

    @classmethod
    def decode(cls, payload: bytes, kind: RowsEventKind) -> RowsEvent:
        if len(payload) < 8:
            raise ValueError("rows event too short")
        table_id = int.from_bytes(payload[0:6], "little")
        flags = int.from_bytes(payload[6:8], "little")
        cursor = 8
        # v2 events carry an "extra info" block: 2-byte length (includes those
        # 2 bytes) followed by the data.
        extra_len = int.from_bytes(payload[cursor : cursor + 2], "little")
        cursor += extra_len
        ncols, cursor = _read_lenenc_int(payload, cursor)
        bitmap_bytes = (ncols + 7) // 8
        if cursor + bitmap_bytes > len(payload):
            raise ValueError("rows event truncated at included-columns bitmap")
        included = payload[cursor : cursor + bitmap_bytes]
        cursor += bitmap_bytes
        if kind == RowsEventKind.UPDATE:
            # Update events carry a second "columns-updated" bitmap.
            if cursor + bitmap_bytes > len(payload):
                raise ValueError("update rows event truncated at second bitmap")
            cursor += bitmap_bytes
            # The remaining payload is `before_image || after_image`; we split
            # at the halfway mark by convention — the actual structure depends
            # on the per-row null bitmap, which the consumer must decode.
            tail = payload[cursor:]
            mid = len(tail) // 2
            return cls(
                kind=kind,
                table_id=table_id,
                flags=flags,
                column_count=ncols,
                included_columns=included,
                image=tail[:mid],
                after_image=tail[mid:],
            )
        return cls(
            kind=kind,
            table_id=table_id,
            flags=flags,
            column_count=ncols,
            included_columns=included,
            image=payload[cursor:],
            after_image=None,
        )


__all__ = [
    "EventType",
    "QueryEvent",
    "RotateEvent",
    "RowsEvent",
    "RowsEventKind",
    "TableMapEvent",
    "XidEvent",
]
