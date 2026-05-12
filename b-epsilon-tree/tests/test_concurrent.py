"""Concurrent put/get safety."""

from __future__ import annotations

import threading

from beps.tree.tree import BEpsilonTree


def test_concurrent_writers_serialise():
    t = BEpsilonTree(node_size=16, epsilon=0.5)
    n_threads = 8
    per_thread = 200

    def worker(tid: int) -> None:
        for i in range(per_thread):
            t.put(f"t{tid:02d}-k{i:04d}".encode(), i)

    threads = [threading.Thread(target=worker, args=(i,)) for i in range(n_threads)]
    for th in threads:
        th.start()
    for th in threads:
        th.join()

    # All n_threads * per_thread keys must be retrievable
    t.flush_all()
    for tid in range(n_threads):
        for i in range(per_thread):
            assert t.get(f"t{tid:02d}-k{i:04d}".encode()) == i


def test_concurrent_readers_consistent_with_writers():
    """Readers see committed writes; no torn reads."""
    t = BEpsilonTree(node_size=16, epsilon=0.5)
    t.put(b"k", 0)

    stop = threading.Event()
    n_iter = 500

    def writer() -> None:
        for i in range(n_iter):
            t.put(b"k", i)
        stop.set()

    bad = []

    def reader() -> None:
        while not stop.is_set():
            v = t.get(b"k")
            # Must be a non-decreasing observation if reads are monotonic
            # (not strictly — concurrent writer may rewind once writes complete,
            # but value must be a sane int <= n_iter)
            if v is not None and not (0 <= v <= n_iter):
                bad.append(v)

    threads = [threading.Thread(target=writer), threading.Thread(target=reader)]
    for th in threads:
        th.start()
    for th in threads:
        th.join(timeout=10)
    assert bad == []
