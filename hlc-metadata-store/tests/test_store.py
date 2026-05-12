"""Tests for MetadataStore — write, read, causal_get."""
from __future__ import annotations

import threading
import time

import pytest

from hlc_store.clock import HybridLogicalClock
from hlc_store.store import MetadataStore
from hlc_store.timestamp import HLCTimestamp


def make_store(drift_ms: int = 0, node_id: str = "test") -> MetadataStore:
    clk = HybridLogicalClock(node_id=node_id, drift_ms=drift_ms)
    return MetadataStore(clk)


class TestPutGet:
    def test_put_returns_timestamp(self):
        store = make_store()
        ts = store.put("k", "v")
        assert isinstance(ts, HLCTimestamp)
        assert ts > HLCTimestamp(0, 0)

    def test_get_returns_value_and_ts(self):
        store = make_store()
        ts = store.put("config", {"host": "db01"})
        result = store.get("config")
        assert result is not None
        value, got_ts = result
        assert value == {"host": "db01"}
        assert got_ts == ts

    def test_get_missing_key(self):
        store = make_store()
        assert store.get("missing") is None

    def test_overwrite_returns_newer_ts(self):
        store = make_store()
        ts1 = store.put("k", "v1")
        ts2 = store.put("k", "v2")
        assert ts2 > ts1
        value, _ = store.get("k")
        assert value == "v2"

    def test_timestamps_monotone_across_keys(self):
        store = make_store()
        tss = [store.put(f"k{i}", i) for i in range(50)]
        for a, b in zip(tss, tss[1:]):
            assert b > a


class TestReplication:
    def test_put_with_remote_ts_advances_past_remote(self):
        store = make_store()
        remote_ts = HLCTimestamp(wall_ms=9_999_999, logical=7)
        ts = store.put("k", "v", remote_ts=remote_ts)
        assert ts > remote_ts

    def test_replication_preserves_order(self):
        """Replicated write timestamp must exceed the source timestamp."""
        source_clk = HybridLogicalClock("source")
        source_store = MetadataStore(source_clk)
        replica_clk = HybridLogicalClock("replica", drift_ms=-500)
        replica_store = MetadataStore(replica_clk)

        source_ts = source_store.put("schema", "v1")
        replica_ts = replica_store.put("schema", "v1", remote_ts=source_ts)

        assert replica_ts > source_ts


class TestCausalGet:
    def test_causal_get_waits_for_write(self):
        store = make_store()

        write_ts: list[HLCTimestamp] = []

        def delayed_write():
            time.sleep(0.05)
            ts = store.put("flag", True)
            write_ts.append(ts)

        t = threading.Thread(target=delayed_write)
        t.start()

        ts_before = HLCTimestamp(int(time.time() * 1000) + 10, 0)
        result = store.causal_get("flag", after=ts_before, timeout_s=2.0)
        t.join()

        assert result is not None
        value, _ = result
        assert value is True

    def test_causal_get_returns_none_on_timeout(self):
        store = make_store()
        future_ts = HLCTimestamp(wall_ms=9_999_999_999, logical=0)
        result = store.causal_get("k", after=future_ts, timeout_s=0.05)
        assert result is None
