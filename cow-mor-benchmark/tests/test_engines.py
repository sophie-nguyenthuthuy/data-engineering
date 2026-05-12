"""Tests for CoW and MoR engines."""

import tempfile
from pathlib import Path

import pytest
import pyarrow as pa

from cow_mor_bench.data.generator import generate_orders, generate_update_batch
from cow_mor_bench.engines.cow import CopyOnWriteEngine
from cow_mor_bench.engines.mor import MergeOnReadEngine


@pytest.fixture
def tmp_path_fresh(tmp_path):
    return str(tmp_path)


def _cow(tmp_path: str, schema: str = "orders") -> CopyOnWriteEngine:
    p = Path(tmp_path) / "cow"
    p.mkdir()
    return CopyOnWriteEngine(str(p), schema)


def _mor(tmp_path: str, schema: str = "orders") -> MergeOnReadEngine:
    p = Path(tmp_path) / "mor"
    p.mkdir()
    return MergeOnReadEngine(str(p), schema)


class TestCopyOnWrite:
    def test_create_and_full_scan(self, tmp_path):
        engine = _cow(str(tmp_path))
        data = generate_orders(1000)
        wr = engine.create_table(data)
        assert wr.rows_written == 1000
        rr = engine.full_scan()
        assert rr.rows_returned == 1000

    def test_insert(self, tmp_path):
        engine = _cow(str(tmp_path))
        engine.create_table(generate_orders(500))
        more = generate_orders(200, start_id=501)
        wr = engine.insert(more)
        assert wr.rows_written == 200
        rr = engine.full_scan()
        assert rr.rows_returned == 700

    def test_update_rewrites_files(self, tmp_path):
        engine = _cow(str(tmp_path))
        data = generate_orders(500)
        engine.create_table(data)
        updates = generate_update_batch(data, 0.1, "orders")
        wr = engine.update(updates)
        assert wr.files_rewritten > 0

    def test_delete(self, tmp_path):
        engine = _cow(str(tmp_path))
        data = generate_orders(500)
        engine.create_table(data)
        engine.delete([1, 2, 3, 4, 5])
        rr = engine.full_scan()
        assert rr.rows_returned == 495

    def test_point_lookup(self, tmp_path):
        engine = _cow(str(tmp_path))
        engine.create_table(generate_orders(500))
        rr = engine.point_lookup(1)
        assert rr.rows_returned == 1

    def test_range_scan(self, tmp_path):
        engine = _cow(str(tmp_path))
        engine.create_table(generate_orders(500))
        rr = engine.range_scan(1, 100)
        assert rr.rows_returned == 100

    def test_compact_reduces_file_count(self, tmp_path):
        engine = _cow(str(tmp_path))
        engine.create_table(generate_orders(500))
        # Insert to create more files
        for i in range(5):
            engine.insert(generate_orders(50, start_id=500 + i * 50 + 1))
        stats_before = engine.stats()
        engine.compact()
        stats_after = engine.stats()
        assert stats_after.data_file_count <= stats_before.data_file_count

    def test_stats(self, tmp_path):
        engine = _cow(str(tmp_path))
        engine.create_table(generate_orders(500))
        s = engine.stats()
        assert s.data_file_count >= 1
        assert s.total_data_bytes > 0
        assert s.delta_file_count == 0


class TestMergeOnRead:
    def test_create_and_full_scan(self, tmp_path):
        engine = _mor(str(tmp_path))
        data = generate_orders(1000)
        wr = engine.create_table(data)
        assert wr.rows_written == 1000
        rr = engine.full_scan()
        assert rr.rows_returned == 1000

    def test_insert_uses_delta(self, tmp_path):
        engine = _mor(str(tmp_path))
        engine.create_table(generate_orders(500))
        wr = engine.insert(generate_orders(100, start_id=501))
        assert wr.files_rewritten == 0  # MoR never rewrites base files for insert
        s = engine.stats()
        assert s.delta_file_count >= 1

    def test_update_uses_delta(self, tmp_path):
        engine = _mor(str(tmp_path))
        data = generate_orders(500)
        engine.create_table(data)
        updates = generate_update_batch(data, 0.1, "orders")
        wr = engine.update(updates)
        assert wr.files_rewritten == 0
        assert wr.files_written == 1

    def test_delete_uses_delta(self, tmp_path):
        engine = _mor(str(tmp_path))
        engine.create_table(generate_orders(500))
        engine.delete([1, 2, 3])
        rr = engine.full_scan()
        # After merge-on-read, deleted rows should not appear
        assert rr.rows_returned == 497

    def test_compact_absorbs_deltas(self, tmp_path):
        engine = _mor(str(tmp_path))
        data = generate_orders(500)
        engine.create_table(data)
        for i in range(5):
            engine.insert(generate_orders(20, start_id=500 + i * 20 + 1))
        s_before = engine.stats()
        assert s_before.delta_file_count >= 5
        engine.compact()
        s_after = engine.stats()
        assert s_after.delta_file_count == 0

    def test_delta_files_merged_in_read(self, tmp_path):
        engine = _mor(str(tmp_path))
        engine.create_table(generate_orders(500))
        engine.insert(generate_orders(50, start_id=501))
        engine.insert(generate_orders(50, start_id=551))
        rr = engine.full_scan()
        assert rr.delta_files_merged >= 2
