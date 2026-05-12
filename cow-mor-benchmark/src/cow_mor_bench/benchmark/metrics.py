"""Metric collection helpers and report formatters."""

from __future__ import annotations

from dataclasses import dataclass

import humanize

from cow_mor_bench.benchmark.runner import BenchmarkResult, BenchmarkSuite


@dataclass
class MetricRow:
    metric: str
    cow_value: str
    mor_value: str
    winner: str


def compare(result: BenchmarkResult) -> list[MetricRow]:
    ct = result.cow_trace
    mt = result.mor_trace
    cs = result.cow_final_stats
    ms = result.mor_final_stats

    def _fmt_ms(s: float) -> str:
        return f"{s * 1000:.1f} ms"

    def _winner(cow_val: float, mor_val: float, lower_is_better: bool = True) -> str:
        if lower_is_better:
            return "CoW" if cow_val < mor_val else "MoR"
        return "CoW" if cow_val > mor_val else "MoR"

    rows = [
        MetricRow(
            "Total write time",
            f"{ct.total_write_s:.3f}s",
            f"{mt.total_write_s:.3f}s",
            _winner(ct.total_write_s, mt.total_write_s),
        ),
        MetricRow(
            "Total read time",
            f"{ct.total_read_s:.3f}s",
            f"{mt.total_read_s:.3f}s",
            _winner(ct.total_read_s, mt.total_read_s),
        ),
        MetricRow(
            "Total compact time",
            f"{ct.total_compact_s:.3f}s",
            f"{mt.total_compact_s:.3f}s",
            _winner(ct.total_compact_s, mt.total_compact_s),
        ),
        MetricRow(
            "p50 write latency",
            _fmt_ms(ct.p50_write_ms / 1000),
            _fmt_ms(mt.p50_write_ms / 1000),
            _winner(ct.p50_write_ms, mt.p50_write_ms),
        ),
        MetricRow(
            "p95 write latency",
            _fmt_ms(ct.p95_write_ms / 1000),
            _fmt_ms(mt.p95_write_ms / 1000),
            _winner(ct.p95_write_ms, mt.p95_write_ms),
        ),
        MetricRow(
            "p50 read latency",
            _fmt_ms(ct.p50_read_ms / 1000),
            _fmt_ms(mt.p50_read_ms / 1000),
            _winner(ct.p50_read_ms, mt.p50_read_ms),
        ),
        MetricRow(
            "p95 read latency",
            _fmt_ms(ct.p95_read_ms / 1000),
            _fmt_ms(mt.p95_read_ms / 1000),
            _winner(ct.p95_read_ms, mt.p95_read_ms),
        ),
        MetricRow(
            "Data files on disk",
            str(cs.data_file_count),
            str(ms.data_file_count),
            "tie" if cs.data_file_count == ms.data_file_count else (
                "CoW" if cs.data_file_count < ms.data_file_count else "MoR"
            ),
        ),
        MetricRow(
            "Delta files on disk",
            str(cs.delta_file_count),
            str(ms.delta_file_count),
            "CoW" if cs.delta_file_count <= ms.delta_file_count else "MoR",
        ),
        MetricRow(
            "Total disk usage",
            humanize.naturalsize(cs.total_data_bytes + cs.total_delta_bytes),
            humanize.naturalsize(ms.total_data_bytes + ms.total_delta_bytes),
            _winner(
                cs.total_data_bytes + cs.total_delta_bytes,
                ms.total_data_bytes + ms.total_delta_bytes,
            ),
        ),
        MetricRow(
            "Avg delta files/read",
            "0",
            f"{mt.avg_delta_files_per_read:.1f}",
            "CoW",
        ),
        MetricRow(
            "Space amplification",
            f"{cs.amplification_ratio:.2f}x",
            f"{ms.amplification_ratio:.2f}x",
            _winner(cs.amplification_ratio, ms.amplification_ratio),
        ),
    ]
    return rows
