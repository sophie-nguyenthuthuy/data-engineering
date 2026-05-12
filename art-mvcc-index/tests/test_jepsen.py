"""Jepsen-style chaos tests.

These run concurrent workloads and check global invariants:
  - No lost updates (when using CAS-pattern increment)
  - No dirty reads (uncommitted data never visible)
  - Snapshot reads are internally consistent
"""

from __future__ import annotations

import random
import threading
import time

import pytest

from art_mvcc.mvcc.store import MVCCArt
from art_mvcc.mvcc.tx import TxConflict, begin_tx

pytestmark = pytest.mark.jepsen


def _run_with_deadline(target_threads: list[threading.Thread], deadline_s: float) -> None:
    """Start threads; assert all finish within `deadline_s` (else fail test)."""
    for t in target_threads:
        t.start()
    end = time.monotonic() + deadline_s
    for t in target_threads:
        remaining = max(0.1, end - time.monotonic())
        t.join(timeout=remaining)
    alive = [t for t in target_threads if t.is_alive()]
    assert not alive, f"deadlock or runaway: {len(alive)} threads still alive after {deadline_s}s"


def test_no_lost_updates_with_cas_retry():
    db = MVCCArt()
    db.put(b"counter", 0)

    increments_per_thread = 100
    n_threads = 16

    def worker() -> None:
        for _ in range(increments_per_thread):
            while True:
                t = begin_tx(db)
                cur = t.get(b"counter")
                t.put(b"counter", (cur or 0) + 1)
                try:
                    t.commit()
                    break
                except TxConflict:
                    continue

    threads = [threading.Thread(target=worker) for _ in range(n_threads)]
    _run_with_deadline(threads, deadline_s=20)

    final = db.begin_snapshot().get(b"counter")
    assert final == n_threads * increments_per_thread, \
        f"lost updates: expected {n_threads * increments_per_thread}, got {final}"


def test_no_dirty_reads_under_concurrent_writes():
    """A writer transaction's tentative writes must never be visible to other
    snapshots until commit."""
    db = MVCCArt()
    db.put(b"k", 0)

    n_iterations = 50
    failures: list[int] = []
    stop = threading.Event()

    def writer() -> None:
        try:
            for i in range(n_iterations):
                t = begin_tx(db)
                t.put(b"k", -1)  # tentative value
                time.sleep(0.001)
                t.rollback()
                # then commit a clean value
                db.put(b"k", i)
        finally:
            stop.set()

    def reader() -> None:
        while not stop.is_set():
            v = db.begin_snapshot().get(b"k")
            if v == -1:
                failures.append(v)

    _run_with_deadline([
        threading.Thread(target=writer),
        threading.Thread(target=reader),
    ], deadline_s=10)

    assert failures == [], f"dirty reads observed: {failures}"


def test_snapshots_internally_consistent_across_keys():
    """A snapshot reads multiple keys; the (k1, k2) pair must be from one
    moment in time, never mixing pre-write and post-write states.

    Setup: a writer transfers `amount` from k1 to k2 atomically. The invariant
    is k1 + k2 == initial_sum.
    """
    db = MVCCArt()
    initial = 100
    db.put(b"a", initial)
    db.put(b"b", 0)

    n_transfers = 200
    inconsistencies: list[tuple[int, int]] = []
    stop = threading.Event()

    def writer() -> None:
        try:
            rng = random.Random(0)
            for _ in range(n_transfers):
                while True:
                    t = begin_tx(db)
                    a = t.get(b"a") or 0
                    b = t.get(b"b") or 0
                    if a <= 0:
                        # Refill so the test keeps going.
                        t.put(b"a", initial)
                        t.put(b"b", b - initial)
                        try:
                            t.commit()
                        except TxConflict:
                            continue
                        break
                    amt = rng.randint(1, min(5, a))
                    t.put(b"a", a - amt)
                    t.put(b"b", b + amt)
                    try:
                        t.commit()
                        break
                    except TxConflict:
                        continue
        finally:
            stop.set()

    def reader() -> None:
        while not stop.is_set():
            s = db.begin_snapshot()
            a = s.get(b"a") or 0
            b = s.get(b"b") or 0
            if a + b != initial:
                inconsistencies.append((a, b))

    _run_with_deadline([
        threading.Thread(target=writer),
        threading.Thread(target=reader),
    ], deadline_s=15)

    assert inconsistencies == [], f"snapshot inconsistencies: {inconsistencies[:5]}"


def test_high_contention_disjoint_keys_no_conflicts():
    """Writers each have their own private key — should never conflict."""
    db = MVCCArt()

    def worker(tid: int) -> None:
        for i in range(50):
            t = begin_tx(db)
            t.put(bytes([tid]), i)
            t.commit()

    threads = [threading.Thread(target=worker, args=(i,)) for i in range(16)]
    _run_with_deadline(threads, deadline_s=10)

    s = db.begin_snapshot()
    for tid in range(16):
        assert s.get(bytes([tid])) == 49
