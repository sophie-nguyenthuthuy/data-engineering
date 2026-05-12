"""Shared fixtures."""

from __future__ import annotations

import pytest

from disagg.client.cache import ClientCache, InvalidationRegistry
from disagg.core.page import PAGE_SIZE, PageId
from disagg.server.page_server import PageServer
from disagg.transport.simulated import SimulatedTransport


@pytest.fixture
def server() -> PageServer:
    return PageServer(capacity_pages=64, n_shards=8)


@pytest.fixture
def transport(server: PageServer) -> SimulatedTransport:
    # Latency=0 for tests; bench files override
    return SimulatedTransport(server=server, latency_us=0.0, jitter_us=0.0)


@pytest.fixture
def registry() -> InvalidationRegistry:
    return InvalidationRegistry()


@pytest.fixture
def client(transport: SimulatedTransport, registry: InvalidationRegistry) -> ClientCache:
    return ClientCache(
        client_id=1,
        transport=transport,
        capacity=8,
        invalidation_registry=registry,
    )


@pytest.fixture
def two_clients(transport: SimulatedTransport, registry: InvalidationRegistry):
    c1 = ClientCache(client_id=1, transport=transport, capacity=8, invalidation_registry=registry)
    c2 = ClientCache(client_id=2, transport=transport, capacity=8, invalidation_registry=registry)
    return c1, c2


@pytest.fixture
def sample_page_data() -> bytes:
    return b"A" * PAGE_SIZE


@pytest.fixture
def sample_page_id() -> PageId:
    return PageId(tenant=0, page_no=42)
