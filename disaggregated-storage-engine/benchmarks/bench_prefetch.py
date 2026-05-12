"""Prefetcher effectiveness: % cache hits with vs without prefetching."""

from __future__ import annotations

from typing import TYPE_CHECKING

from disagg.client.cache import ClientCache
from disagg.prefetch.markov import MarkovPrefetcher
from disagg.server.page_server import PageServer
from disagg.transport.simulated import SimulatedTransport
from disagg.workload import scan_workload, tpcc_workload, zipf_workload

if TYPE_CHECKING:
    from disagg.core.page import PageId


def bench(workload_name: str, workload_iter) -> dict:
    server = PageServer(capacity_pages=2048)
    transport = SimulatedTransport(server=server, latency_us=0.0)
    cache = ClientCache(client_id=1, transport=transport, capacity=256)
    prefetcher = MarkovPrefetcher()

    # Warmup pass — train prefetcher
    workload = list(workload_iter)
    for pid in workload:
        prefetcher.observe(pid)
        cache.read(pid)
    cache.stats.hits = 0
    cache.stats.misses = 0

    # Run pass with prefetching
    prev: PageId | None = None
    for pid in workload:
        # Prefetch top-1 prediction
        for nxt in prefetcher.predict(prev, k=1):
            cache.read(nxt)
        cache.read(pid)
        prev = pid

    total = cache.stats.hits + cache.stats.misses
    return {
        "workload": workload_name,
        "ops": len(workload),
        "hits": cache.stats.hits,
        "misses": cache.stats.misses,
        "hit_rate": cache.stats.hits / max(total, 1),
        "prefetch_top1_acc": prefetcher.estimate_in_sample_top1_accuracy(),
    }


def main() -> None:
    print(f"{'workload':<14} {'ops':>6} {'hits':>8} {'miss':>6} {'hit%':>6} {'predict acc':>12}")
    workloads = [
        ("scan-1pass",  scan_workload(n_pages=500, n_passes=1)),
        ("scan-3pass",  scan_workload(n_pages=200, n_passes=3)),
        ("zipf",        zipf_workload(n_pages=500, n_ops=1000, alpha=1.2)),
        ("tpcc",        tpcc_workload(n_warehouses=4, n_transactions=300)),
    ]
    for name, it in workloads:
        r = bench(name, it)
        print(f"{r['workload']:<14} {r['ops']:>6} {r['hits']:>8} {r['misses']:>6} "
              f"{r['hit_rate'] * 100:>6.1f} {r['prefetch_top1_acc'] * 100:>12.1f}")


if __name__ == "__main__":
    main()
