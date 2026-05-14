"""Format benchmark — write, read, measure."""

from __future__ import annotations

import time
from dataclasses import dataclass
from typing import TYPE_CHECKING

from pova.formats.avro_like import avro_read, avro_write
from pova.formats.orc_like import orc_read, orc_write
from pova.formats.parquet_like import parquet_read, parquet_write

if TYPE_CHECKING:
    from collections.abc import Callable

    from pova.columnar.column import Column
    from pova.columnar.schema import Schema


@dataclass(frozen=True, slots=True)
class FormatBench:
    """One format's benchmark result."""

    name: str
    write_seconds: float
    read_seconds: float
    bytes_written: int


@dataclass(frozen=True, slots=True)
class BenchmarkResult:
    """Per-format benchmark across the same input."""

    n_rows: int
    results: tuple[FormatBench, ...]

    def by_format(self) -> dict[str, FormatBench]:
        return {r.name: r for r in self.results}

    def best_compression(self) -> str:
        return min(self.results, key=lambda r: r.bytes_written).name

    def fastest_read(self) -> str:
        return min(self.results, key=lambda r: r.read_seconds).name

    def fastest_write(self) -> str:
        return min(self.results, key=lambda r: r.write_seconds).name


def _time(
    write_fn: Callable[[], bytes],
    read_fn: Callable[[bytes], object],
    *,
    clock: Callable[[], float],
) -> tuple[float, float, bytes]:
    t0 = clock()
    payload = write_fn()
    write_t = clock() - t0
    t1 = clock()
    read_fn(payload)
    read_t = clock() - t1
    return write_t, read_t, payload


def run_benchmark(
    schema: Schema,
    columns: list[Column],
    *,
    clock: Callable[[], float] | None = None,
) -> BenchmarkResult:
    """Encode + decode the same data into all three formats."""
    schema.validate(columns)
    n_rows = len(columns[0])
    cl = clock or time.perf_counter
    out: list[FormatBench] = []
    pwt, prt, pbuf = _time(
        lambda: parquet_write(schema, columns), lambda b: parquet_read(b), clock=cl
    )
    out.append(FormatBench("parquet", pwt, prt, len(pbuf)))
    owt, ort, obuf = _time(lambda: orc_write(schema, columns), lambda b: orc_read(b), clock=cl)
    out.append(FormatBench("orc", owt, ort, len(obuf)))
    awt, art, abuf = _time(lambda: avro_write(schema, columns), lambda b: avro_read(b), clock=cl)
    out.append(FormatBench("avro", awt, art, len(abuf)))
    return BenchmarkResult(n_rows=n_rows, results=tuple(out))


__all__ = ["BenchmarkResult", "FormatBench", "run_benchmark"]
