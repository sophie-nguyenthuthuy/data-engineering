"""Online store tests — uses fakeredis for hermetic execution."""
from __future__ import annotations

import time
import unittest.mock as mock
from unittest.mock import MagicMock, patch

import fakeredis
import pytest

from feature_store.online.redis_store import OnlineStore, _encode, _decode, _make_key


@pytest.fixture()
def store(tmp_path):
    """OnlineStore backed by fakeredis — no real Redis required."""
    fake = fakeredis.FakeRedis(decode_responses=False)
    s = OnlineStore.__new__(OnlineStore)
    s._client = fake
    s._pipeline_batch_size = 100
    return s


class TestKeySchema:
    def test_key_format(self):
        assert _make_key("user_features", "u123") == "fs:user_features:u123"

    def test_encode_decode_roundtrip(self):
        original = {"score": 0.95, "count": 42, "label": "vip"}
        assert _decode(_encode(original)) == original


class TestOnlineStoreWrite:
    def test_put_and_get(self, store):
        store.put("user_features", "u1", {"score": 0.9}, ttl_seconds=3600)
        result = store.get("user_features", "u1")
        assert result == {"score": 0.9}

    def test_missing_key_returns_none(self, store):
        assert store.get("user_features", "no_such_entity") is None

    def test_put_batch(self, store):
        records = [(f"u{i}", {"v": i}) for i in range(50)]
        store.put_batch("grp", records, ttl_seconds=60)
        for i in range(50):
            assert store.get("grp", f"u{i}") == {"v": i}

    def test_overwrite(self, store):
        store.put("g", "e1", {"x": 1})
        store.put("g", "e1", {"x": 2})
        assert store.get("g", "e1") == {"x": 2}


class TestBatchRead:
    def test_mget_returns_all(self, store):
        store.put("g", "a", {"v": 1})
        store.put("g", "b", {"v": 2})
        result = store.get_batch("g", ["a", "b", "c"])
        assert result["a"] == {"v": 1}
        assert result["b"] == {"v": 2}
        assert result["c"] is None

    def test_multi_group(self, store):
        store.put("users", "u1", {"score": 0.5})
        store.put("items", "i1", {"pop": 0.8})
        result = store.get_multi_group([("users", "u1"), ("items", "i1"), ("items", "i99")])
        assert result[("users", "u1")] == {"score": 0.5}
        assert result[("items", "i1")] == {"pop": 0.8}
        assert result[("items", "i99")] is None


class TestLatency:
    def test_single_get_under_1ms_fakeredis(self, store):
        store.put("g", "e", {"x": 1.0})
        t0 = time.perf_counter()
        for _ in range(100):
            store.get("g", "e")
        elapsed_ms = (time.perf_counter() - t0) * 1000 / 100
        # fakeredis is in-process; should be well under 1ms per call
        assert elapsed_ms < 1.0, f"avg get latency {elapsed_ms:.2f}ms exceeds 1ms"

    def test_batch_100_entities_under_5ms_fakeredis(self, store):
        ids = [f"u{i}" for i in range(100)]
        for i, eid in enumerate(ids):
            store.put("g", eid, {"v": i})
        t0 = time.perf_counter()
        store.get_batch("g", ids)
        elapsed_ms = (time.perf_counter() - t0) * 1000
        assert elapsed_ms < 5.0, f"batch get latency {elapsed_ms:.2f}ms exceeds 5ms"
