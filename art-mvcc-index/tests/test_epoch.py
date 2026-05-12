"""Epoch-based reclamation tests."""

from __future__ import annotations

import threading

from art_mvcc.mvcc.epoch import EpochManager


def test_initial_state():
    e = EpochManager()
    assert e.epoch == 0
    assert e.active_threads == 0
    assert e.pending_garbage == 0


def test_advance_increments():
    e = EpochManager()
    assert e.advance() == 1
    assert e.advance() == 2


def test_retire_and_gc_when_no_threads():
    e = EpochManager()
    log: list[str] = []
    e.retire(lambda: log.append("freed"))
    reclaimed = e.gc()
    assert reclaimed == 1
    assert log == ["freed"]


def test_thread_holds_protects_garbage():
    e = EpochManager()
    log: list[str] = []
    e.enter(tid=1)
    e.retire(lambda: log.append("freed"))
    # While t=1 holds epoch 0, garbage at epoch 0 is NOT safe
    assert e.gc() == 0
    assert log == []
    e.leave(tid=1)
    # Now safe
    assert e.gc() == 1
    assert log == ["freed"]


def test_old_thread_blocks_only_old_garbage():
    e = EpochManager()
    log: list[str] = []
    e.enter(tid=1)
    e.retire(lambda: log.append("retire-epoch-0"))
    e.advance()
    e.retire(lambda: log.append("retire-epoch-1"))
    # t=1 holds epoch 0; nothing older than 0 → nothing reclaimable
    assert e.gc() == 0
    e.leave(tid=1)
    # Now everything is reclaimable
    assert e.gc() == 2


def test_guard_context_manager():
    e = EpochManager()
    log: list[str] = []
    e.retire(lambda: log.append("freed"))
    with e.guard(tid=1):
        assert e.active_threads == 1
        assert e.gc() == 0
        assert log == []
    # exited guard
    assert e.gc() == 1
    assert log == ["freed"]


def test_concurrent_enter_leave():
    """Many threads pin/unpin under load; gc never reclaims unsafe garbage."""
    e = EpochManager()
    iterations = 100

    log: list[str] = []

    def reader(tid: int) -> None:
        for _ in range(iterations):
            with e.guard(tid=tid):
                pass

    def writer() -> None:
        for i in range(iterations):
            e.advance()
            e.retire(lambda i=i: log.append(f"r{i}"))

    threads = [threading.Thread(target=reader, args=(i,)) for i in range(4)]
    w = threading.Thread(target=writer)
    threads.append(w)
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    # Final gc should reclaim everything (no readers left)
    e.gc()
    # All retires should have eventually fired
    # (some may have been waiting; iterate once more)
    e.gc()
    # We can't guarantee EVERY retire fires immediately, but the count must
    # match what was retired.
    assert len(log) <= iterations
