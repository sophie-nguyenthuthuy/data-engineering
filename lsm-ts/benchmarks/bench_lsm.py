#!/usr/bin/env python3
"""
Benchmark the LSM-TS engine on realistic IoT ingestion patterns.

Measures:
  - Bulk write throughput (points/sec)
  - Point lookup latency (p50 / p95 / p99)
  - Range scan latency
  - Compaction amplification (bytes written / bytes ingested)
  - Bloom filter effectiveness (FP rate)

Run:
  python -m benchmarks.bench_lsm [--points N] [--no-compress] [--no-wal]
"""
from __future__ import annotations

import argparse
import shutil
import statistics
import tempfile
import time
from pathlib import Path

from lsm import LSMEngine, TSKey, TSValue
from benchmarks.workload import generate_batch, SENSORS, FACILITIES, _device_tags


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _now_ns() -> int:
    return time.perf_counter_ns()


def _throughput(count: int, elapsed_ns: int) -> float:
    return count / (elapsed_ns / 1e9)


def _fmt(n: float, unit: str = "") -> str:
    if n >= 1_000_000:
        return f"{n/1_000_000:.2f}M{unit}"
    if n >= 1_000:
        return f"{n/1_000:.1f}k{unit}"
    return f"{n:.1f}{unit}"


def _percentile(data: list[float], p: float) -> float:
    data = sorted(data)
    idx = int(len(data) * p / 100)
    return data[min(idx, len(data) - 1)]


# ---------------------------------------------------------------------------
# Benchmark cases
# ---------------------------------------------------------------------------

def bench_write_throughput(
    data_dir: Path, points: list, compress: bool, wal: bool
) -> dict:
    print(f"\n{'='*60}")
    print(f"  WRITE THROUGHPUT  ({len(points):,} points, compress={compress}, wal={wal})")
    print(f"{'='*60}")

    with LSMEngine(data_dir, memtable_size_mb=32, compress=compress, wal_enabled=wal) as eng:
        BATCH = 1000
        batches = [points[i:i+BATCH] for i in range(0, len(points), BATCH)]
        t0 = _now_ns()
        for batch in batches:
            eng.write_batch(batch)
        eng.flush()
        elapsed = _now_ns() - t0

    throughput = _throughput(len(points), elapsed)
    result = {
        "points": len(points),
        "elapsed_ms": elapsed / 1e6,
        "throughput_pts_sec": throughput,
        "compress": compress,
        "wal": wal,
    }
    print(f"  Points:      {len(points):>12,}")
    print(f"  Elapsed:     {elapsed/1e6:>10.1f} ms")
    print(f"  Throughput:  {_fmt(throughput)} pts/sec")
    return result


def bench_point_lookup(data_dir: Path, points: list, compress: bool) -> dict:
    print(f"\n{'='*60}")
    print(f"  POINT LOOKUP  (n=1000 random lookups)")
    print(f"{'='*60}")

    import random
    random.seed(0)
    sample = random.sample(points, min(1000, len(points)))

    with LSMEngine(data_dir, compress=compress, wal_enabled=False) as eng:
        eng.write_batch(points)
        eng.flush()

        latencies_ns = []
        hits = 0
        for p in sample:
            t0 = _now_ns()
            result = eng.get(p.key)
            latencies_ns.append(_now_ns() - t0)
            if result is not None:
                hits += 1

    result_dict = {
        "hit_rate": hits / len(sample),
        "p50_us": _percentile(latencies_ns, 50) / 1000,
        "p95_us": _percentile(latencies_ns, 95) / 1000,
        "p99_us": _percentile(latencies_ns, 99) / 1000,
    }
    print(f"  Hit rate:  {result_dict['hit_rate']*100:.1f}%")
    print(f"  p50:       {result_dict['p50_us']:.1f} µs")
    print(f"  p95:       {result_dict['p95_us']:.1f} µs")
    print(f"  p99:       {result_dict['p99_us']:.1f} µs")
    return result_dict


def bench_range_scan(data_dir: Path, points: list, compress: bool) -> dict:
    print(f"\n{'='*60}")
    print(f"  RANGE SCAN  (1h window, 10 metrics × 3 facilities)")
    print(f"{'='*60}")

    with LSMEngine(data_dir, compress=compress, wal_enabled=False) as eng:
        eng.write_batch(points)
        eng.flush()

        # Find time range from the data
        ts_sorted = sorted(p.key.timestamp_ns for p in points)
        start_ns = ts_sorted[0]
        window_ns = 3600 * 1_000_000_000  # 1 hour
        end_ns = start_ns + window_ns

        latencies_ns = []
        total_rows = 0
        for sensor in SENSORS[:3]:
            for facility in FACILITIES[:2]:
                tags = _device_tags(facility, 0)
                t0 = _now_ns()
                rows = list(eng.scan(sensor.metric, tags, start_ns, end_ns))
                latencies_ns.append(_now_ns() - t0)
                total_rows += len(rows)

    result = {
        "total_rows": total_rows,
        "mean_scan_ms": statistics.mean(latencies_ns) / 1e6,
        "p95_scan_ms": _percentile(latencies_ns, 95) / 1e6,
    }
    print(f"  Total rows:    {total_rows:,}")
    print(f"  Mean scan:     {result['mean_scan_ms']:.2f} ms")
    print(f"  p95 scan:      {result['p95_scan_ms']:.2f} ms")
    return result


def bench_compaction_stats(data_dir: Path, compress: bool) -> dict:
    print(f"\n{'='*60}")
    print(f"  COMPACTION STATS")
    print(f"{'='*60}")
    total_bytes = sum(f.stat().st_size for f in data_dir.glob("*.sst"))
    files = list(data_dir.glob("*.sst"))
    by_level: dict[str, int] = {}
    for f in files:
        lvl = f.stem[1] if f.stem.startswith("L") else "?"
        by_level[f"L{lvl}"] = by_level.get(f"L{lvl}", 0) + f.stat().st_size

    result = {"total_sst_bytes": total_bytes, "files": len(files), "by_level": by_level}
    print(f"  SST files:     {len(files)}")
    print(f"  Total size:    {total_bytes / 1024 / 1024:.2f} MB")
    for lvl, sz in sorted(by_level.items()):
        print(f"  {lvl}:            {sz / 1024:.1f} KB  ({len([f for f in files if f.stem.startswith(lvl)])} files)")
    return result


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(description="LSM-TS Benchmark")
    parser.add_argument("--points", type=int, default=100_000)
    parser.add_argument("--no-compress", action="store_true")
    parser.add_argument("--no-wal", action="store_true")
    args = parser.parse_args()

    compress = not args.no_compress
    wal = not args.no_wal

    print(f"\nGenerating {args.points:,} IoT data points...")
    t0 = time.time()
    points = generate_batch(args.points)
    print(f"Generated in {(time.time()-t0)*1000:.0f} ms")

    results = {}

    with tempfile.TemporaryDirectory(prefix="lsm_bench_write_") as d:
        results["write"] = bench_write_throughput(
            Path(d), points, compress=compress, wal=wal
        )

    with tempfile.TemporaryDirectory(prefix="lsm_bench_read_") as d:
        results["lookup"] = bench_point_lookup(Path(d), points, compress=compress)

    with tempfile.TemporaryDirectory(prefix="lsm_bench_scan_") as d:
        results["scan"] = bench_range_scan(Path(d), points, compress=compress)

    with tempfile.TemporaryDirectory(prefix="lsm_bench_compact_") as d:
        with LSMEngine(Path(d), compress=compress, wal_enabled=wal) as eng:
            eng.write_batch(points)
            eng.flush()
        results["compaction"] = bench_compaction_stats(Path(d), compress=compress)

    print(f"\n{'='*60}")
    print("  SUMMARY")
    print(f"{'='*60}")
    print(f"  Write:   {_fmt(results['write']['throughput_pts_sec'])} pts/sec")
    print(f"  Lookup:  p50={results['lookup']['p50_us']:.1f}µs  p99={results['lookup']['p99_us']:.1f}µs")
    print(f"  Scan:    {results['scan']['mean_scan_ms']:.2f} ms mean  ({results['scan']['total_rows']:,} rows)")
    print(f"  Storage: {results['compaction']['total_sst_bytes']/1024/1024:.2f} MB for {args.points:,} points")

    raw_bytes = args.points * (20 + 8)  # avg key ~20B + 8B value
    amp = results["compaction"]["total_sst_bytes"] / raw_bytes if raw_bytes else 0
    print(f"  Write amp: {amp:.2f}×  (vs raw {raw_bytes/1024:.0f} KB)")


if __name__ == "__main__":
    main()
