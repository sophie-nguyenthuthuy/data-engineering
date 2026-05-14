"""Format round-trip + benchmark tests."""

from __future__ import annotations

import pytest

from pova.bench import run_benchmark
from pova.columnar.column import Column, ColumnType
from pova.columnar.schema import Schema
from pova.formats.avro_like import avro_read, avro_write
from pova.formats.orc_like import orc_read, orc_write
from pova.formats.parquet_like import parquet_read, parquet_write


def _make_table(n: int = 50):
    schema = Schema(
        fields=(
            ("id", ColumnType.INT64),
            ("category", ColumnType.STRING),
            ("amount", ColumnType.FLOAT64),
        )
    )
    columns = [
        Column("id", ColumnType.INT64, tuple(range(n))),
        Column("category", ColumnType.STRING, tuple(["A", "B", "C", "A"][i % 4] for i in range(n))),
        Column("amount", ColumnType.FLOAT64, tuple(float(i) * 1.5 for i in range(n))),
    ]
    return schema, columns


# --------------------------------------------------------- Parquet-like


def test_parquet_round_trip():
    schema, cols = _make_table(100)
    buf = parquet_write(schema, cols, row_group_size=25)
    schema_back, cols_back = parquet_read(buf)
    assert schema_back == schema
    for a, b in zip(cols, cols_back, strict=True):
        assert a.values == b.values


def test_parquet_rejects_bad_magic():
    with pytest.raises(ValueError):
        parquet_read(b"BADDmagic")


def test_parquet_row_group_size_validated():
    schema, cols = _make_table(10)
    with pytest.raises(ValueError):
        parquet_write(schema, cols, row_group_size=0)


def test_parquet_with_nulls_round_trip():
    schema = Schema(fields=(("x", ColumnType.INT64),))
    cols = [Column("x", ColumnType.INT64, (1, None, 2, None, 3))]
    buf = parquet_write(schema, cols, row_group_size=2)
    _, cols_back = parquet_read(buf)
    assert cols_back[0].values == cols[0].values


# ----------------------------------------------------------- ORC-like


def test_orc_round_trip():
    schema, cols = _make_table(100)
    buf = orc_write(schema, cols, stripe_size=25)
    _, cols_back = orc_read(buf)
    for a, b in zip(cols, cols_back, strict=True):
        assert a.values == b.values


def test_orc_rejects_bad_magic():
    with pytest.raises(ValueError):
        orc_read(b"BADmagic")


def test_orc_stripe_size_validated():
    schema, cols = _make_table(10)
    with pytest.raises(ValueError):
        orc_write(schema, cols, stripe_size=0)


# ---------------------------------------------------------- Avro-like


def test_avro_round_trip():
    schema, cols = _make_table(50)
    buf = avro_write(schema, cols)
    _, cols_back = avro_read(buf)
    for a, b in zip(cols, cols_back, strict=True):
        assert a.values == b.values


def test_avro_rejects_bad_magic():
    with pytest.raises(ValueError):
        avro_read(b"NOPE")


def test_avro_round_trip_with_nulls():
    schema = Schema(fields=(("v", ColumnType.STRING),))
    cols = [Column("v", ColumnType.STRING, ("a", None, "b"))]
    buf = avro_write(schema, cols)
    _, cols_back = avro_read(buf)
    assert cols_back[0].values == cols[0].values


# ----------------------------------------------------------- Benchmark


def test_benchmark_round_trip_succeeds_for_all_formats():
    schema, cols = _make_table(100)
    result = run_benchmark(schema, cols)
    formats = {r.name for r in result.results}
    assert formats == {"parquet", "orc", "avro"}
    assert result.n_rows == 100


def test_benchmark_reports_best_compression_winner():
    schema, cols = _make_table(200)
    result = run_benchmark(schema, cols)
    best = result.best_compression()
    assert best in ("parquet", "orc", "avro")


def test_benchmark_has_non_zero_byte_counts():
    schema, cols = _make_table(50)
    result = run_benchmark(schema, cols)
    for r in result.results:
        assert r.bytes_written > 0
        assert r.write_seconds >= 0
        assert r.read_seconds >= 0
