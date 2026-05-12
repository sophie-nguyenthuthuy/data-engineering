"""Coherence directory tests."""

from __future__ import annotations

import threading

from disagg.core.page import PageId
from disagg.server.coherence import CoherenceDirectory


def test_register_reader_creates_entry():
    d = CoherenceDirectory(n_shards=4)
    pid = PageId(0, 1)
    d.register_reader(pid, client_id=10, version=5)
    state = d.state(pid)
    assert state is not None
    assert 10 in state.holders
    assert state.writer is None
    assert state.version == 5


def test_two_readers_share_page():
    d = CoherenceDirectory(n_shards=4)
    pid = PageId(0, 1)
    d.register_reader(pid, client_id=10, version=5)
    d.register_reader(pid, client_id=20, version=5)
    state = d.state(pid)
    assert state.holders == {10, 20}


def test_writer_invalidates_others():
    d = CoherenceDirectory(n_shards=4)
    pid = PageId(0, 1)
    d.register_reader(pid, client_id=10, version=5)
    d.register_reader(pid, client_id=20, version=5)
    invalidate = d.register_writer(pid, client_id=30, new_version=6)
    assert set(invalidate) == {10, 20}
    state = d.state(pid)
    assert state.holders == {30}
    assert state.writer == 30
    assert state.version == 6


def test_writer_doesnt_invalidate_itself():
    d = CoherenceDirectory(n_shards=4)
    pid = PageId(0, 1)
    d.register_reader(pid, client_id=10, version=5)
    invalidate = d.register_writer(pid, client_id=10, new_version=6)
    assert invalidate == []


def test_release_drops_holder():
    d = CoherenceDirectory(n_shards=4)
    pid = PageId(0, 1)
    d.register_reader(pid, client_id=10, version=5)
    d.release(pid, client_id=10)
    state = d.state(pid)
    assert 10 not in state.holders


def test_release_clears_writer():
    d = CoherenceDirectory(n_shards=4)
    pid = PageId(0, 1)
    d.register_writer(pid, client_id=10, new_version=1)
    d.release(pid, client_id=10)
    state = d.state(pid)
    assert state.writer is None


def test_shards_distribute_load():
    """Different pages should land in different shards (probabilistic)."""
    d = CoherenceDirectory(n_shards=8)
    for i in range(100):
        d.register_reader(PageId(0, i), client_id=1, version=1)
    seen_shards = set()
    for i in range(100):
        sh_idx = hash(PageId(0, i)) % d.shard_count()
        seen_shards.add(sh_idx)
    # With 100 pages over 8 shards, we should see all 8
    assert len(seen_shards) == 8


def test_concurrent_readers_no_corruption():
    """Many threads registering simultaneously must not corrupt state."""
    d = CoherenceDirectory(n_shards=8)
    pids = [PageId(0, i) for i in range(20)]
    n_threads = 8

    def worker(tid: int) -> None:
        for pid in pids:
            d.register_reader(pid, client_id=tid, version=1)

    threads = [threading.Thread(target=worker, args=(i,)) for i in range(n_threads)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    for pid in pids:
        state = d.state(pid)
        assert state.holders == set(range(n_threads))
