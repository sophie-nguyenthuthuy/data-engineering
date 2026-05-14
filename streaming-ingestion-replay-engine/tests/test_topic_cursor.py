"""Topic + Cursor tests."""

from __future__ import annotations

import pytest

from sire.log.cursor import Cursor, EndOfLog
from sire.log.topic import Topic


def test_topic_rejects_empty_name():
    with pytest.raises(ValueError):
        Topic(name="")


def test_topic_rejects_zero_segment_size():
    with pytest.raises(ValueError):
        Topic(name="t", segment_size_records=0)


def test_topic_append_returns_increasing_offsets():
    t = Topic(name="t")
    assert t.append(b"a", b"1", timestamp=10) == 0
    assert t.append(b"b", b"2", timestamp=20) == 1
    assert t.append(b"c", b"3", timestamp=30) == 2
    assert len(t) == 3


def test_topic_rolls_segments_at_capacity():
    t = Topic(name="t", segment_size_records=2)
    for i in range(5):
        t.append(b"k", b"v", timestamp=1000 + i)
    segs = t.segments()
    assert len(segs) == 3
    assert [len(s) for s in segs] == [2, 2, 1]


def test_topic_seek_offset_rejects_negative():
    t = Topic(name="t")
    with pytest.raises(ValueError):
        t.seek_offset(-1)


def test_topic_seek_offset_misses_when_out_of_range():
    t = Topic(name="t")
    t.append(b"k", b"v", timestamp=0)
    assert t.seek_offset(99) is None


def test_topic_seek_offset_finds_record():
    t = Topic(name="t", segment_size_records=2)
    for i in range(5):
        t.append(b"k", b"v", timestamp=1000 + i)
    pos = t.seek_offset(3)
    assert pos is not None
    seg_idx, byte_pos = pos
    assert seg_idx == 1  # 5 records, 2 per segment → offset 3 in seg index 1


def test_topic_seek_timestamp_returns_earliest_match():
    t = Topic(name="t", segment_size_records=2)
    for ts in (100, 200, 300, 400):
        t.append(b"k", b"v", timestamp=ts)
    pos = t.seek_timestamp(250)
    assert pos is not None


def test_topic_seek_timestamp_none_when_all_older():
    t = Topic(name="t")
    t.append(b"k", b"v", timestamp=100)
    assert t.seek_timestamp(1_000_000) is None


def test_topic_iter_from_streams_records():
    t = Topic(name="t", segment_size_records=3)
    for i in range(7):
        t.append(b"k", f"v{i}".encode(), timestamp=1000 + i)
    pos = t.seek_offset(4)
    assert pos is not None
    seg_idx, byte_pos = pos
    records = list(t.iter_from(seg_idx, byte_pos))
    assert [r.offset for r in records] == [4, 5, 6]


# ----------------------------------------------------------------- Cursor


def test_cursor_rejects_invalid_construction():
    t = Topic(name="t")
    with pytest.raises(ValueError):
        Cursor(topic=t, segment_index=-1)
    with pytest.raises(ValueError):
        Cursor(topic=t, byte_pos=-5)


def test_cursor_next_emits_records_then_eol():
    t = Topic(name="t")
    t.append(b"k", b"v", timestamp=0)
    cur = Cursor(topic=t)
    rec = cur.next()
    assert not isinstance(rec, EndOfLog)
    eol = cur.next()
    assert isinstance(eol, EndOfLog)
    assert eol.last_offset == 0


def test_cursor_rewind_resets_iterator():
    t = Topic(name="t")
    t.append(b"k", b"v", timestamp=0)
    t.append(b"k", b"v", timestamp=1)
    cur = Cursor(topic=t)
    cur.next()
    cur.next()
    cur.rewind()
    rec = cur.next()
    assert not isinstance(rec, EndOfLog)
    assert rec.offset == 0
