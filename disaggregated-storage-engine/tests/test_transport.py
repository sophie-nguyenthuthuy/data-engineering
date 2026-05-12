"""Transport layer tests."""

from __future__ import annotations

import time

import pytest

from disagg.core.page import PAGE_SIZE, PageId
from disagg.server.page_server import PageServer
from disagg.transport.simulated import SimulatedTransport, TransportError


def test_call_invokes_server_dispatch():
    server = PageServer(capacity_pages=8)
    t = SimulatedTransport(server=server, latency_us=0.0)
    page = t.call("read", client_id=1, page_id=PageId(0, 1))
    assert page is not None
    assert len(page.data) == PAGE_SIZE


def test_latency_injection_observable():
    server = PageServer(capacity_pages=8)
    t = SimulatedTransport(server=server, latency_us=500.0, jitter_us=0.0)
    start = time.perf_counter()
    for _ in range(10):
        t.call("read", client_id=1, page_id=PageId(0, 1))
    elapsed = time.perf_counter() - start
    # 10 calls × 500us = 5ms minimum
    assert elapsed >= 0.005


def test_drop_rate_raises():
    server = PageServer(capacity_pages=8)
    t = SimulatedTransport(server=server, latency_us=0.0, drop_rate=1.0)
    with pytest.raises(TransportError):
        t.call("read", client_id=1, page_id=PageId(0, 1))


def test_stats_tracked():
    server = PageServer(capacity_pages=8)
    t = SimulatedTransport(server=server, latency_us=0.0)
    for _ in range(5):
        t.call("read", client_id=1, page_id=PageId(0, 1))
    s = t.stats()
    assert s["n_calls"] == 5


def test_unknown_op_raises():
    server = PageServer(capacity_pages=8)
    t = SimulatedTransport(server=server, latency_us=0.0)
    with pytest.raises(ValueError):
        t.call("frobnicate")
