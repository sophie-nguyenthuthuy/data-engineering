"""Record + Segment tests."""

from __future__ import annotations

import pytest
from hypothesis import given, settings
from hypothesis import strategies as st

from sire.log.record import HEADER_SIZE, Record, RecordHeader
from sire.log.segment import Segment, SegmentError


def test_record_header_round_trip():
    h = RecordHeader(offset=42, timestamp=1700, key_len=3, value_len=5)
    assert RecordHeader.decode(h.encode()) == h


def test_record_header_rejects_negative_fields():
    with pytest.raises(ValueError):
        RecordHeader(offset=-1, timestamp=0, key_len=0, value_len=0)
    with pytest.raises(ValueError):
        RecordHeader(offset=0, timestamp=0, key_len=-1, value_len=0)


def test_record_header_decode_rejects_short():
    with pytest.raises(ValueError):
        RecordHeader.decode(b"\x00" * (HEADER_SIZE - 1))


def test_record_round_trip():
    r = Record(offset=7, timestamp=12345, key=b"k", value=b"v")
    out, n = Record.decode(r.encode())
    assert out == r
    assert n == HEADER_SIZE + 1 + 1


def test_record_rejects_non_bytes_key():
    with pytest.raises(TypeError):
        Record(offset=0, timestamp=0, key="str", value=b"")  # type: ignore[arg-type]


def test_record_decode_rejects_truncated_payload():
    enc = Record(offset=0, timestamp=0, key=b"abc", value=b"def").encode()
    with pytest.raises(ValueError):
        Record.decode(enc[:-2])  # truncate the value


# ------------------------------------------------------------- Segment


def _rec(offset=0, ts=1000, k=b"k", v=b"v") -> Record:
    return Record(offset=offset, timestamp=ts, key=k, value=v)


def test_segment_append_increments_index():
    s = Segment()
    s.append(_rec(0))
    s.append(_rec(1, ts=1100))
    assert len(s) == 2
    assert s.first_offset() == 0
    assert s.last_offset() == 1


def test_segment_rejects_non_increasing_offset():
    s = Segment()
    s.append(_rec(5))
    with pytest.raises(SegmentError):
        s.append(_rec(5))
    with pytest.raises(SegmentError):
        s.append(_rec(3))


def test_segment_iter_from_zero_emits_all():
    s = Segment()
    records = [_rec(i, ts=1000 + i, k=f"k{i}".encode(), v=f"v{i}".encode()) for i in range(5)]
    for r in records:
        s.append(r)
    assert list(s.iter_from(0)) == records


def test_segment_iter_from_mid_position():
    s = Segment()
    s.append(_rec(0))
    s.append(_rec(1))
    pos1 = s.byte_pos_at_offset(1)
    out = list(s.iter_from(pos1))
    assert [r.offset for r in out] == [1]


def test_segment_byte_pos_at_offset_unknown_raises():
    s = Segment()
    s.append(_rec(0))
    with pytest.raises(SegmentError):
        s.byte_pos_at_offset(99)


def test_segment_byte_pos_after_timestamp_returns_earliest():
    s = Segment()
    s.append(_rec(0, ts=100))
    s.append(_rec(1, ts=200))
    s.append(_rec(2, ts=300))
    pos = s.byte_pos_after_timestamp(150)
    assert pos == s.byte_pos_at_offset(1)


def test_segment_byte_pos_after_timestamp_none_when_all_older():
    s = Segment()
    s.append(_rec(0, ts=100))
    assert s.byte_pos_after_timestamp(500) is None


def test_segment_persist_round_trip(tmp_path):
    s = Segment()
    s.append(_rec(0, k=b"a", v=b"1"))
    s.append(_rec(1, k=b"b", v=b"22"))
    s.write_to(tmp_path / "seg.bin")
    s2 = Segment.read_from(tmp_path / "seg.bin")
    assert s2.byte_size() == s.byte_size()
    assert list(s2.iter_from()) == list(s.iter_from())


def test_segment_from_bytes_rejects_truncation():
    s = Segment()
    s.append(_rec(0, k=b"abc", v=b"defgh"))
    data = s.to_bytes()
    with pytest.raises(SegmentError):
        Segment.from_bytes(data[:-3])


@settings(max_examples=30, deadline=None)
@given(
    st.lists(
        st.tuples(
            st.integers(0, 1_000_000),  # timestamp
            st.binary(min_size=0, max_size=16),  # key
            st.binary(min_size=0, max_size=64),  # value
        ),
        min_size=0,
        max_size=30,
    )
)
def test_property_segment_persist_round_trip(records):
    s = Segment()
    for i, (ts, k, v) in enumerate(records):
        s.append(Record(offset=i, timestamp=ts, key=k, value=v))
    assert list(Segment.from_bytes(s.to_bytes()).iter_from()) == list(s.iter_from())
