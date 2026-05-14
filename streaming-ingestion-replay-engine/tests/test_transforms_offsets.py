"""Transform pipeline + OffsetStore tests."""

from __future__ import annotations

import pytest

from sire.log.record import Record
from sire.offsets import OffsetStore
from sire.transforms.base import SKIP
from sire.transforms.composed import ComposedTransform
from sire.transforms.filter import Filter
from sire.transforms.mapper import Mapper


def _r(offset=0, ts=0, k=b"k", v=b"v") -> Record:
    return Record(offset=offset, timestamp=ts, key=k, value=v)


# ----------------------------------------------------------- transforms


def test_mapper_runs_callable():
    m = Mapper(
        fn=lambda r: Record(offset=r.offset, timestamp=r.timestamp, key=r.key, value=r.value + b"!")
    )
    out = m.apply(_r(0, k=b"k", v=b"hello"))
    assert isinstance(out, Record)
    assert out.value == b"hello!"


def test_mapper_rejects_non_record_return():
    m = Mapper(fn=lambda _r: "not a record")  # type: ignore[arg-type, return-value]
    with pytest.raises(TypeError):
        m.apply(_r())


def test_filter_passes_when_predicate_true():
    f = Filter(predicate=lambda r: r.offset >= 0)
    assert isinstance(f.apply(_r(0)), Record)


def test_filter_skips_when_predicate_false():
    f = Filter(predicate=lambda _r: False)
    assert f.apply(_r(0)) is SKIP


def test_composed_chains_in_order():
    upper = Mapper(
        fn=lambda r: Record(
            offset=r.offset, timestamp=r.timestamp, key=r.key, value=r.value.upper()
        )
    )
    keep = Filter(predicate=lambda r: r.value != b"DROP")
    chain = ComposedTransform(transforms=[upper, keep])
    assert chain.apply(_r(0, v=b"hi")).value == b"HI"
    assert chain.apply(_r(1, v=b"drop")) is SKIP


def test_composed_short_circuits_on_skip():
    counter = {"n": 0}

    class Counting(Mapper):
        def apply(self, record):
            counter["n"] += 1
            return super().apply(record)

    chain = ComposedTransform(
        transforms=[
            Filter(predicate=lambda _r: False),  # drops everything
            Counting(fn=lambda r: r),
        ]
    )
    assert chain.apply(_r()) is SKIP
    assert counter["n"] == 0


# ----------------------------------------------------------- OffsetStore


def test_offset_store_commit_get_round_trip(tmp_path):
    s = OffsetStore(path=tmp_path / "offsets.jsonl")
    s.commit(group="g1", topic="t1", next_offset=10)
    assert s.get(group="g1", topic="t1") == 10
    assert s.get(group="g1", topic="other") == 0


def test_offset_store_persists_across_instances(tmp_path):
    p = tmp_path / "offsets.jsonl"
    OffsetStore(path=p).commit(group="g1", topic="t1", next_offset=42)
    s2 = OffsetStore(path=p)
    assert s2.get(group="g1", topic="t1") == 42


def test_offset_store_rejects_invalid_args(tmp_path):
    s = OffsetStore(path=tmp_path / "o.jsonl")
    with pytest.raises(ValueError):
        s.commit(group="", topic="t", next_offset=0)
    with pytest.raises(ValueError):
        s.commit(group="g", topic="", next_offset=0)
    with pytest.raises(ValueError):
        s.commit(group="g", topic="t", next_offset=-1)


def test_offset_store_all_returns_full_state(tmp_path):
    s = OffsetStore(path=tmp_path / "o.jsonl")
    s.commit(group="g1", topic="t1", next_offset=1)
    s.commit(group="g2", topic="t1", next_offset=5)
    assert s.all() == {("g1", "t1"): 1, ("g2", "t1"): 5}


def test_offset_store_in_memory_only_works():
    s = OffsetStore()
    s.commit(group="g", topic="t", next_offset=3)
    assert s.get(group="g", topic="t") == 3
