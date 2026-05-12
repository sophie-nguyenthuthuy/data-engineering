import tempfile
from pathlib import Path

import pytest
from lsm.sstable import SSTableWriter, SSTableReader
from lsm.types import TSKey, TSValue


def _make_entries(n: int) -> list[tuple[bytes, bytes]]:
    entries = []
    for i in range(n):
        key = TSKey.make("metric", {"id": f"{i:04d}"}, i * 1_000_000_000).encode()
        val = TSValue(value=float(i)).encode()
        entries.append((key, val))
    return sorted(entries)


def test_write_and_read(tmp_path):
    path = tmp_path / "test.sst"
    entries = _make_entries(100)
    writer = SSTableWriter(path, compress=False)
    for k, v in entries:
        writer.add(k, v)
    reader = writer.finish()

    for k, v in entries:
        result = reader.get(k)
        assert result == v, f"Mismatch for key {k!r}"


def test_bloom_filter_reduces_reads(tmp_path):
    path = tmp_path / "bloom.sst"
    entries = _make_entries(1000)
    writer = SSTableWriter(path, compress=False)
    for k, v in entries:
        writer.add(k, v)
    reader = writer.finish()

    absent_key = TSKey.make("other_metric", {}, 9999).encode()
    assert not reader.may_contain(absent_key)


def test_scan_range(tmp_path):
    path = tmp_path / "scan.sst"
    entries = _make_entries(200)
    writer = SSTableWriter(path)
    for k, v in entries:
        writer.add(k, v)
    reader = writer.finish()

    start = entries[50][0]
    end = entries[100][0]
    scanned = list(reader.scan(start, end))
    assert len(scanned) == 50
    assert all(start <= k < end for k, _ in scanned)


def test_tombstone_round_trip(tmp_path):
    path = tmp_path / "tomb.sst"
    writer = SSTableWriter(path, compress=False)
    key = TSKey.make("m", {}, 1).encode()
    writer.add(key, None)  # tombstone
    reader = writer.finish()
    result = reader.get(key)
    assert result is None  # tombstone returns None


def test_compressed_equals_uncompressed(tmp_path):
    entries = _make_entries(500)
    p_raw = tmp_path / "raw.sst"
    p_lz4 = tmp_path / "lz4.sst"

    for path, compress in [(p_raw, False), (p_lz4, True)]:
        w = SSTableWriter(path, compress=compress)
        for k, v in entries:
            w.add(k, v)
        w.finish()

    r_raw = SSTableReader(p_raw, compress=False)
    r_lz4 = SSTableReader(p_lz4, compress=True)
    for k, v in entries:
        assert r_raw.get(k) == r_lz4.get(k)
