"""Read throughput at various simulated network latencies."""

from __future__ import annotations

import time

from disagg.client.cache import ClientCache
from disagg.core.page import PageId
from disagg.server.page_server import PageServer
from disagg.transport.simulated import SimulatedTransport


def bench(latency_us: float, n_reads: int = 1000, n_unique: int = 100) -> dict:
    server = PageServer(capacity_pages=2 * n_unique)
    transport = SimulatedTransport(server=server, latency_us=latency_us, jitter_us=0.0)
    cache = ClientCache(client_id=1, transport=transport, capacity=n_unique // 2)

    # Pre-populate server with a few pages
    for i in range(n_unique):
        server.read(client_id=0, page_id=PageId(0, i))
    # Reset client stats
    cache.stats.hits = 0
    cache.stats.misses = 0

    start = time.perf_counter()
    for i in range(n_reads):
        cache.read(PageId(0, i % n_unique))
    elapsed = time.perf_counter() - start

    return {
        "latency_us": latency_us,
        "reads": n_reads,
        "ms": elapsed * 1000,
        "qps": n_reads / elapsed,
        "hits": cache.stats.hits,
        "misses": cache.stats.misses,
    }


def main() -> None:
    print(f"{'latency µs':>12} {'reads':>6} {'ms':>10} {'reads/s':>12} {'hits':>6} {'miss':>6}")
    for lat in (0.0, 5.0, 50.0):
        r = bench(latency_us=lat)
        print(f"{r['latency_us']:>12.1f} {r['reads']:>6} {r['ms']:>10.1f} "
              f"{r['qps']:>12,.0f} {r['hits']:>6} {r['misses']:>6}")


if __name__ == "__main__":
    main()
