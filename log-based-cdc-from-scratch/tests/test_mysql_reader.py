"""End-to-end binlog reader tests."""

from __future__ import annotations

import io
import struct

import pytest

from lcdc.mysql.events import QueryEvent, RotateEvent, XidEvent
from lcdc.mysql.header import HEADER_LEN, EventHeader
from lcdc.mysql.reader import MAGIC, BinlogReader, MagicHeaderError


def _event_bytes(event_type: int, payload: bytes, *, log_pos: int = 0) -> bytes:
    size = HEADER_LEN + len(payload)
    h = EventHeader(
        timestamp=0,
        event_type=event_type,
        server_id=1,
        event_size=size,
        log_pos=log_pos,
        flags=0,
    )
    return h.encode() + payload


def _build_query_payload(schema: str, query: str) -> bytes:
    schema_b = schema.encode()
    query_b = query.encode()
    head = struct.pack("<IIBHH", 1, 0, len(schema_b), 0, 0)
    return head + schema_b + b"\x00" + query_b


def test_reader_consumes_magic():
    rotate = struct.pack("<Q", 200) + b"binlog.000002"
    stream = io.BytesIO(MAGIC + _event_bytes(0x04, rotate))
    events = list(BinlogReader(stream=stream))
    assert len(events) == 1
    _, ev = events[0]
    assert isinstance(ev, RotateEvent)
    assert ev.next_file == "binlog.000002"


def test_reader_rejects_bad_magic():
    stream = io.BytesIO(b"BADD" + _event_bytes(0x04, struct.pack("<Q", 1) + b"x"))
    with pytest.raises(MagicHeaderError):
        list(BinlogReader(stream=stream))


def test_reader_can_skip_magic():
    rotate = struct.pack("<Q", 200) + b"binlog.000002"
    stream = io.BytesIO(_event_bytes(0x04, rotate))
    events = list(BinlogReader(stream=stream, require_magic=False))
    assert len(events) == 1


def test_reader_returns_unknown_events_as_raw_bytes():
    stream = io.BytesIO(MAGIC + _event_bytes(0xFE, b"\x00\x01\x02\x03"))
    events = list(BinlogReader(stream=stream))
    assert len(events) == 1
    _, ev = events[0]
    assert ev == b"\x00\x01\x02\x03"


def test_reader_dispatches_multiple_event_types():
    parts = MAGIC
    parts += _event_bytes(0x02, _build_query_payload("mydb", "BEGIN"))
    parts += _event_bytes(0x10, struct.pack("<Q", 99))
    parts += _event_bytes(0x04, struct.pack("<Q", 1) + b"binlog.000002")
    out = list(BinlogReader(stream=io.BytesIO(parts)))
    assert isinstance(out[0][1], QueryEvent)
    assert isinstance(out[1][1], XidEvent)
    assert isinstance(out[2][1], RotateEvent)
    assert out[1][1].xid == 99


def test_reader_rejects_truncated_header():
    stream = io.BytesIO(MAGIC + b"\x00\x01\x02")
    with pytest.raises(ValueError):
        list(BinlogReader(stream=stream))


def test_reader_rejects_truncated_payload():
    h = EventHeader(
        timestamp=0,
        event_type=0x04,
        server_id=1,
        event_size=HEADER_LEN + 100,  # claims 100 bytes of payload
        log_pos=0,
        flags=0,
    )
    # Provide only 20 bytes instead of 100.
    stream = io.BytesIO(MAGIC + h.encode() + b"\x00" * 20)
    with pytest.raises(ValueError):
        list(BinlogReader(stream=stream))
