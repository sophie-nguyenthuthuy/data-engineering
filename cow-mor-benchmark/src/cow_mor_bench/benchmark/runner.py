"""Benchmark runner — drives both engines with identical workloads and collects metrics."""

from __future__ import annotations

import shutil
import tempfile
from dataclasses import dataclass, field
from pathlib import Path

from cow_mor_bench.engines.cow import CopyOnWriteEngine
from cow_mor_bench.engines.mor import MergeOnReadEngine
from cow_mor_bench.engines.base import TableStats
from cow_mor_bench.workload.generator import WorkloadGenerator, WorkloadTrace
from cow_mor_bench.workload.patterns import WorkloadProfile
from cow_mor_bench.compaction.model import estimate_compaction_cost, ClusterConfig, DEFAULT_CLUSTER


@dataclass
class BenchmarkResult:
    profile_name: str
    schema_name: str
    table_size: int
    n_ops: int
    cow_trace: WorkloadTrace
    mor_trace: WorkloadTrace
    cow_final_stats: TableStats
    mor_final_stats: TableStats
    compact_every: int | None = None

    # Computed deltas
    read_speedup_cow: float = 0.0    # cow_read_s / mor_read_s  (<1 = CoW faster)
    write_speedup_mor: float = 0.0   # mor_write_s / cow_write_s (<1 = MoR faster)
    space_overhead_mor: float = 0.0  # mor total bytes / cow total bytes


@dataclass
class BenchmarkSuite:
    results: list[BenchmarkResult] = field(default_factory=list)

    def add(self, result: BenchmarkResult) -> None:
        result.read_speedup_cow = (
            result.cow_trace.total_read_s
            / max(result.mor_trace.total_read_s, 1e-9)
        )
        result.write_speedup_mor = (
            result.mor_trace.total_write_s
            / max(result.cow_trace.total_write_s, 1e-9)
        )
        cow_bytes = (
            result.cow_final_stats.total_data_bytes
            + result.cow_final_stats.total_delta_bytes
        )
        mor_bytes = (
            result.mor_final_stats.total_data_bytes
            + result.mor_final_stats.total_delta_bytes
        )
        result.space_overhead_mor = mor_bytes / max(cow_bytes, 1)
        self.results.append(result)


def run_benchmark(
    profile: WorkloadProfile,
    schema_name: str = "orders",
    table_size: int = 20_000,
    n_ops: int = 60,
    compact_every: int | None = None,
    seed: int = 42,
    tmp_root: str | None = None,
) -> BenchmarkResult:
    """Run both CoW and MoR engines on the same workload profile."""

    root = Path(tmp_root) if tmp_root else Path(tempfile.mkdtemp(prefix="cow_mor_bench_"))
    cow_path = str(root / "cow" / profile.name / schema_name)
    mor_path = str(root / "mor" / profile.name / schema_name)
    Path(cow_path).mkdir(parents=True, exist_ok=True)
    Path(mor_path).mkdir(parents=True, exist_ok=True)

    cow_engine = CopyOnWriteEngine(cow_path, schema_name)
    mor_engine = MergeOnReadEngine(mor_path, schema_name)

    cow_gen = WorkloadGenerator(
        cow_engine, profile, schema_name,
        table_size=table_size, n_ops=n_ops,
        compact_every=compact_every, seed=seed,
    )
    mor_gen = WorkloadGenerator(
        mor_engine, profile, schema_name,
        table_size=table_size, n_ops=n_ops,
        compact_every=compact_every, seed=seed,
    )

    cow_trace = cow_gen.run()
    mor_trace = mor_gen.run()

    cow_stats = cow_engine.stats()
    mor_stats = mor_engine.stats()

    return BenchmarkResult(
        profile_name=profile.name,
        schema_name=schema_name,
        table_size=table_size,
        n_ops=n_ops,
        cow_trace=cow_trace,
        mor_trace=mor_trace,
        cow_final_stats=cow_stats,
        mor_final_stats=mor_stats,
        compact_every=compact_every,
    )
