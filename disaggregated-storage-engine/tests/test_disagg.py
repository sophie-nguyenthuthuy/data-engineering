import random

from src import PageServer, ClientCache, MarkovPrefetcher, PAGE_SIZE


def test_basic_read_write():
    server = PageServer(net_latency_us=0, capacity_pages=10)
    c1 = ClientCache(client_id=1, server=server, capacity=4)
    page = b"A" * PAGE_SIZE
    c1.write(0, page)
    assert c1.read(0) == page


def test_local_cache_hits():
    server = PageServer(net_latency_us=0, capacity_pages=10)
    c1 = ClientCache(client_id=1, server=server, capacity=4)
    c1.write(0, b"A" * PAGE_SIZE)
    # Reset stats by reading multiple times
    for _ in range(5):
        c1.read(0)
    assert c1.stats["local_hits"] >= 4


def test_write_returns_invalidations():
    server = PageServer(net_latency_us=0, capacity_pages=10)
    c1 = ClientCache(client_id=1, server=server, capacity=4)
    c2 = ClientCache(client_id=2, server=server, capacity=4)
    page = b"V1" + b"\x00" * (PAGE_SIZE - 2)
    c1.write(0, page)
    c2.read(0)  # c2 now holds page 0
    # c1 writes again → c2 should be invalidated
    invalidate = c1.write(0, b"V2" + b"\x00" * (PAGE_SIZE - 2))
    assert 2 in invalidate


def test_coherence_invalidates_stale_copy():
    server = PageServer(net_latency_us=0, capacity_pages=10)
    c1 = ClientCache(client_id=1, server=server, capacity=4)
    c2 = ClientCache(client_id=2, server=server, capacity=4)
    p_v1 = b"V1" + b"\x00" * (PAGE_SIZE - 2)
    c1.write(0, p_v1)
    p_read = c2.read(0)
    assert p_read == p_v1
    # c1 writes new version
    p_v2 = b"V2" + b"\x00" * (PAGE_SIZE - 2)
    invalidate_list = c1.write(0, p_v2)
    # In a real system, server pushes invalidations; we simulate manually.
    for cid in invalidate_list:
        if cid == 2:
            c2.invalidate(0)
    # c2 re-reads → must get v2
    assert c2.read(0) == p_v2


def test_eviction_lru():
    server = PageServer(net_latency_us=0, capacity_pages=3)
    c = ClientCache(client_id=1, server=server, capacity=10)
    for i in range(5):
        c.write(i, bytes([i]) * PAGE_SIZE)
    # Server should have at most 3 pages
    assert len(server._pages) <= 3
    assert server.stats["evictions"] >= 2


def test_markov_prefetcher_learns_patterns():
    p = MarkovPrefetcher()
    # Sequential access pattern
    for _ in range(50):
        for page in range(0, 10):
            p.observe(page)
    # After current page i, next should usually be i+1 (or 0 after 9)
    assert p.predict(5) == [6]
    assert p.predict(9) == [0]
    # In-sample accuracy should be high
    assert p.accuracy_estimate() > 0.9


def test_prefetch_reduces_misses_in_sequential_workload():
    """Simulate sequential reads — prefetcher fetches in advance."""
    server = PageServer(net_latency_us=0, capacity_pages=100)
    c = ClientCache(client_id=1, server=server, capacity=20)
    prefetcher = MarkovPrefetcher()
    # Populate server with 50 pages
    for i in range(50):
        c.write(i, bytes([i % 256]) * PAGE_SIZE)
    # Warm prefetcher on training pass
    for _ in range(3):
        for i in range(50):
            prefetcher.observe(i)
            c.read(i)
    # Reset stats
    c.stats["local_hits"] = 0
    c.stats["local_misses"] = 0

    # Now do a fresh access pass WITH prefetching
    prev = None
    for i in range(50):
        # Prefetch next-predicted before access
        for next_id in prefetcher.predict(prev, k=1):
            c.read(next_id)
        c.read(i)
        prefetcher.observe(i)
        prev = i
    # Most reads should be local hits because prefetcher pre-fetched them
    total = c.stats["local_hits"] + c.stats["local_misses"]
    assert c.stats["local_hits"] / total > 0.4
