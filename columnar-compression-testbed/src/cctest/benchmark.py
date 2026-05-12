"""Benchmark harness: run all codecs against a column and print a comparison table."""
from __future__ import annotations

import logging
from typing import Optional

import numpy as np

from .codecs import ALL_CODECS, BenchmarkResult, Codec

logger = logging.getLogger(__name__)

_HDR = f"{'Codec':<20} {'Ratio':>7} {'Saving':>8} {'Enc ms':>8} {'Dec ms':>8} {'OK?':>5}"
_SEP = "-" * len(_HDR)


def run_column_benchmark(
    column: np.ndarray,
    codecs: Optional[list[Codec]] = None,
    rounds: int = 5,
    label: str = "",
) -> list[BenchmarkResult]:
    """Benchmark every applicable codec against *column*.  Returns sorted results."""
    codecs = codecs if codecs is not None else ALL_CODECS
    applicable = [c for c in codecs if c.supports_dtype(column.dtype)]

    results: list[BenchmarkResult] = []
    for codec in applicable:
        try:
            bm = codec.benchmark(column, rounds=rounds)
            results.append(bm)
        except Exception as exc:
            logger.warning("Codec %s error: %s", codec.name, exc)

    results.sort(key=lambda r: -r.ratio)
    return results


def print_benchmark(results: list[BenchmarkResult], label: str = "") -> None:
    title = f"  Benchmark: {label}" if label else "  Benchmark"
    print(f"\n{title}")
    print(_SEP)
    print(_HDR)
    print(_SEP)
    for r in results:
        ok = "YES" if r.lossless else "NO "
        print(
            f"{r.codec_name:<20} {r.ratio:>7.2f}x {r.space_saving*100:>7.1f}% "
            f"{r.encode_ms:>8.2f} {r.decode_ms:>8.2f} {ok:>5}"
        )
    print(_SEP)


def run_table_benchmark(
    table: dict[str, np.ndarray],
    codecs: Optional[list[Codec]] = None,
    rounds: int = 5,
) -> dict[str, list[BenchmarkResult]]:
    """Run per-column benchmarks for every column in *table*."""
    return {
        name: run_column_benchmark(col, codecs=codecs, rounds=rounds, label=name)
        for name, col in table.items()
    }
