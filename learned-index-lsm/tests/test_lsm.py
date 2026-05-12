"""Integration tests for the LSM engine."""
import pytest
from lsm_learned.lsm.engine import LSMEngine


@pytest.mark.parametrize("strategy", ["rmi", "btree"])
def test_put_and_get(strategy, tmp_path):
    with LSMEngine(tmp_path / strategy, index_strategy=strategy, memtable_capacity=500) as eng:
        for i in range(0, 1000, 2):
            eng.put(i, i * 10)
        for i in range(0, 1000, 2):
            assert eng.get(i) == i * 10, f"key {i} not found"


@pytest.mark.parametrize("strategy", ["rmi", "btree"])
def test_missing_key_returns_none(strategy, tmp_path):
    with LSMEngine(tmp_path / strategy, index_strategy=strategy) as eng:
        eng.put(1, 100)
        assert eng.get(2) is None


def test_overwrite(tmp_path):
    with LSMEngine(tmp_path, memtable_capacity=10) as eng:
        eng.put(42, 1)
        eng.put(42, 2)
        assert eng.get(42) == 2


def test_flush_and_read(tmp_path):
    with LSMEngine(tmp_path, memtable_capacity=100) as eng:
        for i in range(200):
            eng.put(i, i)
        # Force a second flush
        eng.flush()
        for i in range(200):
            assert eng.get(i) == i


def test_scan(tmp_path):
    with LSMEngine(tmp_path) as eng:
        for i in range(0, 100, 5):
            eng.put(i, i)
        eng.flush()
        result = eng.scan(10, 40)
        keys = [k for k, _ in result]
        assert keys == list(range(10, 45, 5))


def test_stats(tmp_path):
    with LSMEngine(tmp_path, memtable_capacity=50) as eng:
        for i in range(100):
            eng.put(i, i)
        stats = eng.stats()
        assert stats["total_writes"] == 100
        assert "index_strategy" in stats


def test_compaction_triggered(tmp_path):
    with LSMEngine(tmp_path, memtable_capacity=10) as eng:
        # Fill enough to trigger L0 compaction (threshold=8 flushes)
        for i in range(90):
            eng.put(i, i)
        eng.flush()
        # After compaction there should be L1 tables
        s = eng.stats()
        assert s["l1_tables"] >= 1 or s["l0_tables"] < 8
