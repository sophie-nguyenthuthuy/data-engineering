"""Hot + cold store unit tests."""

from __future__ import annotations

import threading

import pytest

from cfs.store.cold import ColdStore
from cfs.store.hot import HotStore
from cfs.store.version import Version


def test_version_rejects_negative_wall():
    with pytest.raises(ValueError):
        Version(value=1, clock={}, wall=-1.0)


def test_hot_rejects_bad_k():
    with pytest.raises(ValueError):
        HotStore(k=0)


def test_hot_rejects_empty_entity_or_feature():
    h = HotStore()
    with pytest.raises(ValueError):
        h.write("", "f", 1, {}, 1.0)
    with pytest.raises(ValueError):
        h.write("e", "", 1, {}, 1.0)


def test_hot_keeps_last_k_versions():
    h = HotStore(k=3)
    for i in range(10):
        h.write("u1", "f", i, clock={"c": i + 1}, wall=float(i))
    versions = h.versions("u1", "f")
    assert [v.value for v in versions] == [7, 8, 9]


def test_hot_entity_clock_is_pointwise_max():
    h = HotStore(k=5)
    h.write("u1", "f", 1, clock={"a": 1}, wall=1.0)
    h.write("u1", "f", 2, clock={"a": 0, "b": 3}, wall=2.0)
    assert h.entity_clock("u1") == {"a": 1, "b": 3}


def test_hot_entity_clock_unknown_entity_is_empty():
    assert HotStore().entity_clock("nobody") == {}


def test_hot_n_entries_tracks_writes_after_eviction():
    h = HotStore(k=2)
    for i in range(5):
        h.write("u1", "f", i, clock={"c": i + 1}, wall=float(i))
    assert h.n_entries() == 2  # k=2


def test_hot_concurrent_writers_preserve_count():
    """Two threads writing 500 records each → 1000 total observations."""
    h = HotStore(k=10_000)

    def writer(prefix: str) -> None:
        for i in range(500):
            h.write(
                entity="u1",
                feature=f"{prefix}-{i}",
                value=i,
                clock={prefix: i + 1},
                wall=float(i),
            )

    threads = [threading.Thread(target=writer, args=(p,)) for p in ("a", "b")]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    assert h.n_entries() == 1000


def test_cold_rejects_empty_entity_or_feature():
    c = ColdStore()
    with pytest.raises(ValueError):
        c.write("", "f", 1, {}, 1.0)
    with pytest.raises(ValueError):
        c.write("e", "", 1, {}, 1.0)


def test_cold_keeps_all_writes_no_eviction():
    c = ColdStore()
    for i in range(50):
        c.write("u1", "f", i, clock={"c": i + 1}, wall=float(i))
    assert len(c.versions("u1", "f")) == 50


def test_cold_concurrent_writers_no_lost_records():
    c = ColdStore()

    def writer(prefix: str) -> None:
        for i in range(500):
            c.write("u1", f"{prefix}-{i}", i, clock={prefix: i + 1}, wall=float(i))

    threads = [threading.Thread(target=writer, args=(p,)) for p in ("a", "b")]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    assert c.n_entries() == 1000
