"""Demo: 2 clients sharing a remote buffer pool, simulated 50µs network."""
from __future__ import annotations

import random
import time

from src import PageServer, ClientCache, MarkovPrefetcher, PAGE_SIZE


def main():
    server = PageServer(net_latency_us=50, capacity_pages=200)
    c1 = ClientCache(client_id=1, server=server, capacity=20)
    c2 = ClientCache(client_id=2, server=server, capacity=20)

    print("Write 100 pages, then random-read with two clients...")
    for i in range(100):
        c1.write(i, bytes([i % 256]) * PAGE_SIZE)

    # Random reads
    rng = random.Random(0)
    t0 = time.perf_counter()
    for _ in range(500):
        c = c1 if rng.random() < 0.5 else c2
        page_id = rng.randint(0, 99)
        _ = c.read(page_id)
    elapsed = time.perf_counter() - t0

    print(f"\n500 random reads, 50µs simulated network → {elapsed*1000:.1f} ms")
    print(f"  c1: hits={c1.stats['local_hits']}  misses={c1.stats['local_misses']}")
    print(f"  c2: hits={c2.stats['local_hits']}  misses={c2.stats['local_misses']}")
    print(f"  server: reads={server.stats['reads']}  writes={server.stats['writes']}  "
          f"invalidations={server.stats['invalidations']}")

    print("\nSequential workload + Markov prefetcher...")
    p = MarkovPrefetcher()
    c3 = ClientCache(client_id=3, server=server, capacity=20)
    # Train + run
    for _ in range(3):
        for i in range(100):
            p.observe(i)
            c3.read(i)
    c3.stats["local_hits"] = 0
    c3.stats["local_misses"] = 0

    prev = None
    for i in range(100):
        for next_id in p.predict(prev, k=1):
            c3.read(next_id)
        c3.read(i)
        p.observe(i)
        prev = i

    print(f"  Markov accuracy estimate: {p.accuracy_estimate()*100:.1f}%")
    print(f"  c3 with prefetch: hits={c3.stats['local_hits']}  "
          f"misses={c3.stats['local_misses']}")


if __name__ == "__main__":
    main()
