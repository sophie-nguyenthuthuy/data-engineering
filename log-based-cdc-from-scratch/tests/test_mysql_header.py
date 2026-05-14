"""MySQL event header tests."""

from __future__ import annotations

import struct

import pytest
from hypothesis import given, settings
from hypothesis import strategies as st

from lcdc.mysql.header import HEADER_LEN, EventHeader


def _pack(ts=1, et=2, sid=3, sz=19, lp=120, fl=0):
    return struct.pack("<IBIIIH", ts, et, sid, sz, lp, fl)


def test_header_round_trip():
    h = EventHeader(timestamp=1, event_type=2, server_id=3, event_size=20, log_pos=120, flags=0)
    assert EventHeader.decode(h.encode()) == h


def test_header_payload_size():
    h = EventHeader(timestamp=0, event_type=2, server_id=1, event_size=100, log_pos=120, flags=0)
    assert h.payload_size == 100 - HEADER_LEN


def test_header_rejects_oversize_event_type():
    with pytest.raises(ValueError):
        EventHeader(timestamp=0, event_type=256, server_id=0, event_size=19, log_pos=0, flags=0)


def test_header_rejects_event_size_below_header_len():
    with pytest.raises(ValueError):
        EventHeader(timestamp=0, event_type=2, server_id=0, event_size=10, log_pos=0, flags=0)


def test_header_rejects_negative_fields():
    with pytest.raises(ValueError):
        EventHeader(timestamp=-1, event_type=2, server_id=0, event_size=19, log_pos=0, flags=0)


def test_header_decode_rejects_truncated_buffer():
    with pytest.raises(ValueError):
        EventHeader.decode(b"\x00" * 5)


def test_header_decode_matches_known_bytes():
    h = EventHeader.decode(_pack(ts=1700000000, et=0x04, sid=1, sz=50, lp=200, fl=0))
    assert h.timestamp == 1700000000
    assert h.event_type == 0x04
    assert h.server_id == 1
    assert h.event_size == 50
    assert h.log_pos == 200
    assert h.flags == 0


@settings(max_examples=40, deadline=None)
@given(
    st.integers(0, 2**32 - 1),
    st.integers(0, 255),
    st.integers(0, 2**32 - 1),
    st.integers(HEADER_LEN, 2**32 - 1),
    st.integers(0, 2**32 - 1),
    st.integers(0, 65535),
)
def test_property_header_encode_decode_round_trip(ts, et, sid, sz, lp, fl):
    h = EventHeader(timestamp=ts, event_type=et, server_id=sid, event_size=sz, log_pos=lp, flags=fl)
    assert EventHeader.decode(h.encode()) == h
