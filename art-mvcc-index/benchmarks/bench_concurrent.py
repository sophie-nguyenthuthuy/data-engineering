"""Concurrent benchmark: MVCC under N reader + M writer threads."""

from __future__ import annotations

import random
import threading
import time

from art_mvcc.mvcc.store import MVCCArt
from art_mvcc.mvcc.tx import TxConflict, begin_tx


def workload(n_readers: int, n_writers: int, n_keys: int, duration_s: float) -> dict:
    db = MVCCArt()
    for i in range(n_keys):
        db.put(f"k{i:04d}".encode(), 0)

    stop = threading.Event()
    counters = {"reads": 0, "writes": 0, "conflicts": 0}
    lock = threading.Lock()

    def reader() -> None:
        local_reads = 0
        while not stop.is_set():
            s = db.begin_snapshot()
            for i in range(50):
                _ = s.get(f"k{i % n_keys:04d}".encode())
            local_reads += 50
        with lock:
            counters["reads"] += local_reads

    def writer(tid: int) -> None:
        rng = random.Random(tid)
        local_writes = local_conflicts = 0
        while not stop.is_set():
            t = begin_tx(db)
            k = f"k{rng.randint(0, n_keys - 1):04d}".encode()
            cur = t.get(k) or 0
            t.put(k, cur + 1)
            try:
                t.commit()
                local_writes += 1
            except TxConflict:
                local_conflicts += 1
        with lock:
            counters["writes"] += local_writes
            counters["conflicts"] += local_conflicts

    threads = (
        [threading.Thread(target=reader) for _ in range(n_readers)]
        + [threading.Thread(target=writer, args=(i,)) for i in range(n_writers)]
    )
    for t in threads:
        t.start()
    time.sleep(duration_s)
    stop.set()
    for t in threads:
        t.join()

    return {
        "n_readers": n_readers, "n_writers": n_writers, "n_keys": n_keys,
        "duration_s": duration_s,
        "reads": counters["reads"], "writes": counters["writes"],
        "conflicts": counters["conflicts"],
        "read_qps": counters["reads"] / duration_s,
        "write_qps": counters["writes"] / duration_s,
        "conflict_rate": (counters["conflicts"]
                          / max(counters["writes"] + counters["conflicts"], 1)),
    }


def main() -> None:
    print(f"{'readers':>8} {'writers':>8} {'keys':>6} {'reads/s':>10} {'writes/s':>10} {'conflict%':>10}")
    for nr, nw in [(1, 1), (4, 1), (4, 4), (1, 8)]:
        r = workload(n_readers=nr, n_writers=nw, n_keys=128, duration_s=2.0)
        print(f"{r['n_readers']:>8} {r['n_writers']:>8} {r['n_keys']:>6} "
              f"{r['read_qps']:>10,.0f} {r['write_qps']:>10,.0f} "
              f"{r['conflict_rate'] * 100:>10.2f}")


if __name__ == "__main__":
    main()
