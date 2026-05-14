"""End-to-end ReplayEngine tests."""

from __future__ import annotations

import json

import pytest

from sire.log.record import Record
from sire.log.topic import Topic
from sire.offsets import OffsetStore
from sire.replay import ReplayEngine
from sire.sinks.collect import CollectingSink
from sire.sinks.file import JsonlFileSink
from sire.transforms.filter import Filter
from sire.transforms.mapper import Mapper


def _topic(n: int = 5, base_ts: int = 1000) -> Topic:
    t = Topic(name="t", segment_size_records=2)
    for i in range(n):
        t.append(key=f"k{i}".encode(), value=f"v{i}".encode(), timestamp=base_ts + i)
    return t


def test_replay_from_beginning_emits_every_record():
    sink = CollectingSink()
    n = ReplayEngine(topic=_topic(5), sink=sink).from_beginning()
    assert n == 5
    assert [r.offset for r in sink.records] == [0, 1, 2, 3, 4]


def test_replay_from_offset_skips_earlier_records():
    sink = CollectingSink()
    n = ReplayEngine(topic=_topic(5), sink=sink).from_offset(2)
    assert n == 3
    assert [r.offset for r in sink.records] == [2, 3, 4]


def test_replay_from_offset_rejects_negative():
    sink = CollectingSink()
    with pytest.raises(ValueError):
        ReplayEngine(topic=_topic(1), sink=sink).from_offset(-1)


def test_replay_from_offset_out_of_range_returns_zero():
    sink = CollectingSink()
    n = ReplayEngine(topic=_topic(2), sink=sink).from_offset(100)
    assert n == 0
    assert sink.records == []


def test_replay_from_timestamp_skips_older_records():
    sink = CollectingSink()
    n = ReplayEngine(topic=_topic(5, base_ts=1000), sink=sink).from_timestamp(1002)
    assert n == 3
    assert sink.records[0].timestamp == 1002


def test_replay_from_timestamp_all_older_returns_zero():
    sink = CollectingSink()
    n = ReplayEngine(topic=_topic(3, base_ts=100), sink=sink).from_timestamp(1_000_000)
    assert n == 0


def test_replay_max_records_caps_output():
    sink = CollectingSink()
    n = ReplayEngine(topic=_topic(10), sink=sink).from_beginning(max_records=4)
    assert n == 4
    assert len(sink.records) == 4


def test_replay_applies_mapper_to_every_record():
    upper = Mapper(
        fn=lambda r: Record(
            offset=r.offset, timestamp=r.timestamp, key=r.key, value=r.value.upper()
        )
    )
    sink = CollectingSink()
    ReplayEngine(topic=_topic(3), sink=sink, transform=upper).from_beginning()
    assert all(r.value == r.value.upper() for r in sink.records)


def test_replay_drops_records_via_filter():
    even = Filter(predicate=lambda r: r.offset % 2 == 0)
    sink = CollectingSink()
    n = ReplayEngine(topic=_topic(5), sink=sink, transform=even).from_beginning()
    assert n == 3
    assert [r.offset for r in sink.records] == [0, 2, 4]


def test_replay_from_committed_uses_offset_store(tmp_path):
    store = OffsetStore(path=tmp_path / "offsets.jsonl")
    store.commit(group="g1", topic="t", next_offset=3)
    sink = CollectingSink()
    n = ReplayEngine(topic=_topic(5), sink=sink, offsets=store).from_committed("g1")
    assert n == 2
    assert [r.offset for r in sink.records] == [3, 4]


def test_replay_from_committed_without_store_raises():
    sink = CollectingSink()
    with pytest.raises(RuntimeError):
        ReplayEngine(topic=_topic(1), sink=sink).from_committed("g1")


def test_replay_writes_to_jsonl_file_sink(tmp_path):
    path = tmp_path / "out.jsonl"
    sink = JsonlFileSink(path=path)
    n = ReplayEngine(topic=_topic(3), sink=sink).from_beginning()
    sink.close()
    assert n == 3
    lines = path.read_text().splitlines()
    assert len(lines) == 3
    first = json.loads(lines[0])
    assert first["offset"] == 0
    # key + value are base64-encoded.
    import base64

    assert base64.b64decode(first["key"]) == b"k0"
    assert base64.b64decode(first["value"]) == b"v0"
