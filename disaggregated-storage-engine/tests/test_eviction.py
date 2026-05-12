"""LRU evictor tests."""

from __future__ import annotations

import pytest

from disagg.core.page import PageId
from disagg.server.eviction import LRUEvictor


def test_capacity_validation():
    with pytest.raises(ValueError):
        LRUEvictor(capacity=0)


def test_touch_inserts():
    e = LRUEvictor(capacity=3)
    e.touch(PageId(0, 1))
    e.touch(PageId(0, 2))
    assert len(e) == 2


def test_evict_when_over_capacity():
    e = LRUEvictor(capacity=3)
    for i in range(5):
        e.touch(PageId(0, i))
    evicted = e.evict_if_needed()
    # 2 should be evicted (oldest)
    assert len(evicted) == 2
    assert evicted[0].page_no == 0
    assert evicted[1].page_no == 1
    assert len(e) == 3


def test_touch_promotes_recency():
    e = LRUEvictor(capacity=3)
    for i in range(3):
        e.touch(PageId(0, i))
    # Touch the oldest → it becomes newest
    e.touch(PageId(0, 0))
    # Add one more → page 1 should be evicted (now the oldest)
    e.touch(PageId(0, 3))
    evicted = e.evict_if_needed()
    assert PageId(0, 1) in evicted
    assert PageId(0, 0) not in evicted


def test_remove_explicit():
    e = LRUEvictor(capacity=3)
    pid = PageId(0, 1)
    e.touch(pid)
    e.remove(pid)
    assert pid not in e
