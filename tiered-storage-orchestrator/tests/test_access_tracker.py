"""Tests for the access pattern tracker."""
from __future__ import annotations

import json
import time

import pytest

from tiered_storage.tracking.access_patterns import AccessPatternTracker


def test_record_access_increments_count():
    t = AccessPatternTracker()
    t.record_access("k1")
    t.record_access("k1")
    assert t.get("k1").access_count == 2


def test_ema_freq_updates():
    t = AccessPatternTracker()
    t.record_access("k2")
    stats = t.get("k2")
    assert stats.ema_freq > 0


def test_idle_days_freshly_created():
    t = AccessPatternTracker()
    t.record_access("k3")
    assert t.get("k3").idle_days < 0.01  # just created


def test_keys_idle_for():
    from tiered_storage.tracking.access_patterns import KeyStats
    t = AccessPatternTracker()
    now = time.time()
    t.record_access("recent")
    # Insert a backdated entry directly to guarantee idle_days > 5
    t._stats["old"] = KeyStats(
        key="old",
        access_count=1,
        first_seen=now - 15 * 86400,
        last_accessed=now - 10 * 86400,
        ema_freq=0.1,
    )

    idle = t.keys_idle_for(5.0)
    assert "old" in idle
    # "recent" was just accessed so it must not appear
    assert "recent" not in idle


def test_keys_below_freq():
    t = AccessPatternTracker()
    t.record_access("active")
    t._stats["active"].ema_freq = 10.0
    t._stats["active"].first_seen = time.time() - 2 * 86400

    t.record_access("lazy")
    t._stats["lazy"].ema_freq = 0.01
    t._stats["lazy"].first_seen = time.time() - 2 * 86400

    below = t.keys_below_freq(0.5)
    assert "lazy" in below
    assert "active" not in below


def test_hottest_keys():
    t = AccessPatternTracker()
    for i in range(5):
        t.record_access(f"k{i}")
        t._stats[f"k{i}"].ema_freq = float(i)

    hot = t.hottest_keys(3)
    assert hot[0].key == "k4"
    assert len(hot) == 3


def test_remove():
    t = AccessPatternTracker()
    t.record_access("bye")
    t.remove("bye")
    assert t.get("bye") is None


def test_persist_and_reload(tmp_path):
    path = str(tmp_path / "tracker.json")
    t = AccessPatternTracker(persist_path=path)
    t.record_access("persist_me")
    t._stats["persist_me"].ema_freq = 7.5
    t.save()

    t2 = AccessPatternTracker(persist_path=path)
    stats = t2.get("persist_me")
    assert stats is not None
    assert stats.ema_freq == pytest.approx(7.5)
    assert stats.access_count == 1
