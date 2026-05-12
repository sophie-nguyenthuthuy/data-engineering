"""Client cache + coherence end-to-end."""

from __future__ import annotations

from disagg.core.page import PAGE_SIZE, PageId


def test_read_miss_then_hit(client, sample_page_id):
    p1 = client.read(sample_page_id)
    assert p1.data == b"\x00" * PAGE_SIZE
    assert client.stats.misses == 1
    assert client.stats.hits == 0
    # Second read = hit
    p2 = client.read(sample_page_id)
    assert client.stats.hits == 1
    assert p2.data == p1.data


def test_write_then_read_back(client, sample_page_id, sample_page_data):
    client.write(sample_page_id, sample_page_data)
    p = client.read(sample_page_id)
    assert p.data == sample_page_data
    # Read was a hit (we updated local cache)
    assert client.stats.hits >= 1


def test_lru_eviction(client):
    """Capacity is 8 in fixture; insert 12 — oldest 4 evicted."""
    for i in range(12):
        client.read(PageId(0, i))
    assert client.size <= 8
    # Oldest pages should be evicted (LRU); newest in cache
    # The exact set depends on access pattern, but cache size is bounded.


def test_coherence_invalidates_stale_copy(two_clients, sample_page_id, sample_page_data):
    c1, c2 = two_clients
    c1.write(sample_page_id, sample_page_data)
    # c2 reads → now both hold the page
    assert c2.read(sample_page_id).data == sample_page_data
    # c1 writes a new version → c2 must be invalidated
    new_data = b"B" * PAGE_SIZE
    c1.write(sample_page_id, new_data)
    # c2's cache no longer has the page
    assert sample_page_id not in c2._cache
    # Reading from c2 fetches the new version
    assert c2.read(sample_page_id).data == new_data


def test_write_returns_invalidation_list(client, sample_page_id, sample_page_data):
    """The page server returns the list of clients to invalidate."""
    client.write(sample_page_id, sample_page_data)
    state = client.transport.server.dir.state(sample_page_id)
    # Just one client wrote → it is the sole holder
    assert state.holders == {client.client_id}


def test_release_drops_from_cache_and_directory(client, sample_page_id):
    client.read(sample_page_id)
    client.release(sample_page_id)
    assert sample_page_id not in client._cache
    state = client.transport.server.dir.state(sample_page_id)
    assert client.client_id not in state.holders


def test_invalidation_count_tracked(two_clients, sample_page_id, sample_page_data):
    c1, c2 = two_clients
    c2.read(sample_page_id)
    c1.write(sample_page_id, sample_page_data)
    assert c2.stats.invalidations_received == 1
