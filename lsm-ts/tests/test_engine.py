"""Integration tests for the full LSM engine."""
import tempfile
from pathlib import Path

import pytest
from lsm import LSMEngine, TSKey, TSValue, DataPoint


def _key(metric: str, ts: int, tags: dict | None = None) -> TSKey:
    return TSKey.make(metric, tags or {"host": "h1"}, ts)


def _val(v: float) -> TSValue:
    return TSValue(value=v)


# ---------------------------------------------------------------------------
# Basic CRUD
# ---------------------------------------------------------------------------

def test_put_and_get(tmp_path):
    with LSMEngine(tmp_path, wal_enabled=False) as eng:
        k = _key("cpu", 1_000_000_000)
        eng.put(k, _val(75.5))
        assert eng.get(k) == _val(75.5)


def test_missing_key_returns_none(tmp_path):
    with LSMEngine(tmp_path, wal_enabled=False) as eng:
        assert eng.get(_key("missing", 0)) is None


def test_overwrite(tmp_path):
    with LSMEngine(tmp_path, wal_enabled=False) as eng:
        k = _key("cpu", 1)
        eng.put(k, _val(1.0))
        eng.put(k, _val(2.0))
        assert eng.get(k) == _val(2.0)


def test_delete(tmp_path):
    with LSMEngine(tmp_path, wal_enabled=False) as eng:
        k = _key("cpu", 1)
        eng.put(k, _val(99.0))
        eng.delete(k)
        assert eng.get(k) is None


# ---------------------------------------------------------------------------
# Flush to SSTable
# ---------------------------------------------------------------------------

def test_survives_memtable_flush(tmp_path):
    with LSMEngine(tmp_path, memtable_size_mb=1, wal_enabled=False) as eng:
        keys = [_key("temp", i * 1_000_000_000) for i in range(5000)]
        for k in keys:
            eng.put(k, _val(23.5))
        for k in keys[:100]:
            assert eng.get(k) == _val(23.5)


# ---------------------------------------------------------------------------
# Range scan
# ---------------------------------------------------------------------------

def test_range_scan(tmp_path):
    with LSMEngine(tmp_path, wal_enabled=False) as eng:
        for i in range(100):
            eng.put(_key("humidity", i * 1_000_000_000), _val(float(i)))
        results = list(eng.scan("humidity", {"host": "h1"}, 10_000_000_000, 20_000_000_000))
        assert len(results) == 10
        assert all(r.key.metric == "humidity" for r in results)
        ts_list = [r.key.timestamp_ns for r in results]
        assert ts_list == sorted(ts_list)


# ---------------------------------------------------------------------------
# WAL recovery
# ---------------------------------------------------------------------------

def test_wal_recovery(tmp_path):
    k = _key("pressure", 42_000_000_000)
    # Write without closing cleanly (simulate crash by not calling close)
    eng = LSMEngine(tmp_path, wal_enabled=True)
    eng.put(k, _val(1013.25))
    eng._wal.flush()
    # Don't call close() — simulate crash
    del eng

    # Reopen and recover
    with LSMEngine(tmp_path, wal_enabled=True) as eng2:
        result = eng2.get(k)
    assert result == _val(1013.25)


# ---------------------------------------------------------------------------
# Multi-metric isolation
# ---------------------------------------------------------------------------

def test_metrics_isolated(tmp_path):
    with LSMEngine(tmp_path, wal_enabled=False) as eng:
        eng.put(_key("cpu", 1), _val(80.0))
        eng.put(_key("mem", 1), _val(4096.0))
        assert eng.get(_key("cpu", 1)) == _val(80.0)
        assert eng.get(_key("mem", 1)) == _val(4096.0)
        assert eng.get(_key("disk", 1)) is None


# ---------------------------------------------------------------------------
# Write batch
# ---------------------------------------------------------------------------

def test_write_batch(tmp_path):
    with LSMEngine(tmp_path, wal_enabled=False) as eng:
        points = [
            DataPoint(_key("co2", i * 1_000_000_000), _val(400.0 + i))
            for i in range(200)
        ]
        eng.write_batch(points)
        for p in points:
            assert eng.get(p.key) == p.value


# ---------------------------------------------------------------------------
# Stats
# ---------------------------------------------------------------------------

def test_stats(tmp_path):
    with LSMEngine(tmp_path, wal_enabled=False) as eng:
        eng.put(_key("x", 1), _val(1.0))
        s = eng.stats()
    assert "memtable_entries" in s
