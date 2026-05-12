"""TTL and tombstone compaction tests."""

from __future__ import annotations

import time

import pytest

from ssb.manager import StateBackendManager
from ssb.state.descriptor import TTLConfig
from ssb.state.serializer import TOMBSTONE, encode_key, encode_value, is_tombstone
from ssb.ttl.compactor import TTLCompactor


class TestTTLExpiry:
    def test_value_expires_after_ttl(self, manager):
        """Set a value with 100ms TTL, sleep 200ms, run compactor, assert gone."""
        ttl = TTLConfig(ttl_ms=100, update_on_read=False)
        ctx = manager.get_state_context("op_ttl", "key1")
        state = ctx.get_value_state("v", default=None, ttl=ttl)
        state.set("expiring_value")
        assert state.get() == "expiring_value"

        time.sleep(0.25)

        # After TTL: get should return default without needing compaction
        assert state.get() is None

    def test_compactor_removes_expired_keys(self, manager):
        """Expired keys should be physically deleted by the compactor."""
        ttl = TTLConfig(ttl_ms=100)
        ctx = manager.get_state_context("op_compact", "key1")
        state = ctx.get_value_state("cv", default=None, ttl=ttl)
        state.set("will_be_deleted")

        time.sleep(0.25)

        # Run compactor synchronously
        deleted = manager.compactor.run_once()
        assert deleted >= 1

        # Key should be physically absent from the backend
        cf = "op_compact::cv"
        raw = manager.backend.get(cf, encode_key("key1"))
        assert raw is None

    def test_update_on_read_refreshes_ttl(self, manager):
        """With update_on_read=True, reading before TTL should keep value alive."""
        ttl = TTLConfig(ttl_ms=300, update_on_read=True)
        ctx = manager.get_state_context("op_uor", "key1")
        state = ctx.get_value_state("uor", default=None, ttl=ttl)
        state.set("refreshable")

        time.sleep(0.15)
        # Read refreshes timestamp
        assert state.get() == "refreshable"

        time.sleep(0.15)
        # Still alive because timestamp was refreshed
        assert state.get() == "refreshable"

    def test_value_not_expired_before_ttl(self, manager):
        ttl = TTLConfig(ttl_ms=5000)  # 5 seconds
        ctx = manager.get_state_context("op_noexp", "k")
        state = ctx.get_value_state("nv", default=None, ttl=ttl)
        state.set("still_here")
        assert state.get() == "still_here"

    def test_list_state_ttl(self, manager):
        ttl = TTLConfig(ttl_ms=100)
        ctx = manager.get_state_context("op_list_ttl", "k")
        state = ctx.get_list_state("lv", ttl=ttl)
        state.add(1)
        state.add(2)
        assert state.get() == [1, 2]
        time.sleep(0.25)
        assert state.get() == []

    def test_map_state_ttl(self, manager):
        ttl = TTLConfig(ttl_ms=100)
        ctx = manager.get_state_context("op_map_ttl", "k")
        state = ctx.get_map_state("mv", ttl=ttl)
        state.put("x", 1)
        assert state.get("x") == 1
        time.sleep(0.25)
        assert state.get("x") is None

    def test_reducing_state_ttl(self, manager):
        ttl = TTLConfig(ttl_ms=100)
        ctx = manager.get_state_context("op_red_ttl", "k")
        state = ctx.get_reducing_state("rv", reduce_fn=lambda a, b: a + b, ttl=ttl)
        state.add(10)
        assert state.get() == 10
        time.sleep(0.25)
        assert state.get() is None


class TestTombstones:
    def test_clear_writes_tombstone(self, manager):
        """Calling clear() should write the tombstone byte to the backend."""
        ctx = manager.get_state_context("op_tomb", "key1")
        state = ctx.get_value_state("tv", default=None)
        state.set("some_value")
        state.clear()

        cf = "op_tomb::tv"
        raw = manager.backend.get(cf, encode_key("key1"))
        assert raw is not None
        assert is_tombstone(raw)

    def test_tombstone_causes_get_to_return_default(self, manager):
        ctx = manager.get_state_context("op_tomb2", "key1")
        state = ctx.get_value_state("tv2", default="default_val")
        state.set("value")
        state.clear()
        assert state.get() == "default_val"

    def test_compactor_removes_tombstones(self, manager):
        """Compactor should clean up tombstone entries."""
        cf = "op_tomb3::tv3"
        manager.backend.create_cf(cf)
        # Write a tombstone directly and register CF with TTL
        manager.backend.put(cf, encode_key("k"), TOMBSTONE)

        ttl = TTLConfig(ttl_ms=1)
        manager.compactor.register_cf(cf, ttl)

        time.sleep(0.01)
        deleted = manager.compactor.run_once()
        assert deleted >= 1
        assert manager.backend.get(cf, encode_key("k")) is None

    def test_map_state_clear_writes_tombstones(self, manager):
        """MapState.clear() should write tombstones for all entries."""
        ctx = manager.get_state_context("op_map_tomb", "key1")
        state = ctx.get_map_state("mt")
        state.put("a", 1)
        state.put("b", 2)
        state.clear()
        assert list(state.items()) == []


class TestCompactorDaemon:
    def test_daemon_thread_runs(self, manager):
        """The compactor thread should be alive after manager.start()."""
        assert manager.compactor._thread is not None
        assert manager.compactor._thread.is_alive()

    def test_register_unregister(self, manager):
        ttl = TTLConfig(ttl_ms=1000)
        manager.compactor.register_cf("test_cf", ttl)
        assert "test_cf" in manager.compactor._cf_ttl
        manager.compactor.unregister_cf("test_cf")
        assert "test_cf" not in manager.compactor._cf_ttl
