"""parquet-vs-orc-vs-avro-lab — mini columnar-format implementations + benchmark."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

__version__ = "0.1.0"

if TYPE_CHECKING:
    from pova.bench import BenchmarkResult, FormatBench, run_benchmark
    from pova.columnar.column import Column, ColumnType
    from pova.columnar.schema import Schema
    from pova.encoding.delta import delta_decode, delta_encode
    from pova.encoding.dictionary import dictionary_decode, dictionary_encode
    from pova.encoding.plain import plain_decode, plain_encode
    from pova.encoding.rle import rle_decode, rle_encode
    from pova.formats.avro_like import avro_read, avro_write
    from pova.formats.orc_like import orc_read, orc_write
    from pova.formats.parquet_like import parquet_read, parquet_write
    from pova.pushdown import Predicate, can_skip_row_group
    from pova.stats.column import ColumnStats


_LAZY: dict[str, tuple[str, str]] = {
    "ColumnType": ("pova.columnar.column", "ColumnType"),
    "Column": ("pova.columnar.column", "Column"),
    "Schema": ("pova.columnar.schema", "Schema"),
    "plain_encode": ("pova.encoding.plain", "plain_encode"),
    "plain_decode": ("pova.encoding.plain", "plain_decode"),
    "rle_encode": ("pova.encoding.rle", "rle_encode"),
    "rle_decode": ("pova.encoding.rle", "rle_decode"),
    "dictionary_encode": ("pova.encoding.dictionary", "dictionary_encode"),
    "dictionary_decode": ("pova.encoding.dictionary", "dictionary_decode"),
    "delta_encode": ("pova.encoding.delta", "delta_encode"),
    "delta_decode": ("pova.encoding.delta", "delta_decode"),
    "ColumnStats": ("pova.stats.column", "ColumnStats"),
    "Predicate": ("pova.pushdown", "Predicate"),
    "can_skip_row_group": ("pova.pushdown", "can_skip_row_group"),
    "parquet_write": ("pova.formats.parquet_like", "parquet_write"),
    "parquet_read": ("pova.formats.parquet_like", "parquet_read"),
    "orc_write": ("pova.formats.orc_like", "orc_write"),
    "orc_read": ("pova.formats.orc_like", "orc_read"),
    "avro_write": ("pova.formats.avro_like", "avro_write"),
    "avro_read": ("pova.formats.avro_like", "avro_read"),
    "run_benchmark": ("pova.bench", "run_benchmark"),
    "BenchmarkResult": ("pova.bench", "BenchmarkResult"),
    "FormatBench": ("pova.bench", "FormatBench"),
}


def __getattr__(name: str) -> Any:
    if name in _LAZY:
        from importlib import import_module

        m, attr = _LAZY[name]
        return getattr(import_module(m), attr)
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")


__all__ = [
    "BenchmarkResult",
    "Column",
    "ColumnStats",
    "ColumnType",
    "FormatBench",
    "Predicate",
    "Schema",
    "__version__",
    "avro_read",
    "avro_write",
    "can_skip_row_group",
    "delta_decode",
    "delta_encode",
    "dictionary_decode",
    "dictionary_encode",
    "orc_read",
    "orc_write",
    "parquet_read",
    "parquet_write",
    "plain_decode",
    "plain_encode",
    "rle_decode",
    "rle_encode",
    "run_benchmark",
]
