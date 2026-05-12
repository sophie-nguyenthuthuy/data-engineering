"""Compaction cost model.

Models the I/O, CPU, and time cost of compaction under different strategies.
Produces break-even analysis: after how many mutations does compaction become
cheaper than the accumulated read-amplification cost?
"""

from __future__ import annotations

from dataclasses import dataclass

import numpy as np


@dataclass
class CompactionCostEstimate:
    # Direct compaction costs
    bytes_read_for_compaction: int
    bytes_written_after_compaction: int
    estimated_compaction_s: float

    # Accumulated read overhead before compaction
    read_amplification_factor: float    # ratio of bytes read vs ideal (no delta)
    delta_merge_overhead_s: float       # cumulative extra time spent merging deltas

    # Break-even analysis
    break_even_ops: int                 # mutations before compaction pays off
    roi_ratio: float                    # (saved_read_time) / compaction_cost

    # Recommendation
    should_compact_now: bool
    reason: str


@dataclass
class ClusterConfig:
    """Hardware/cluster parameters for cost estimation."""
    disk_read_mb_s: float = 500.0       # sequential read throughput MB/s
    disk_write_mb_s: float = 300.0      # sequential write throughput MB/s
    cpu_sort_mb_s: float = 800.0        # sort/merge throughput MB/s (in-memory)
    merge_overhead_factor: float = 1.3  # extra CPU cost for row-level merge


DEFAULT_CLUSTER = ClusterConfig()


def model_cow_write_cost(
    data_bytes: int,
    update_fraction: float,
    cluster: ClusterConfig = DEFAULT_CLUSTER,
) -> float:
    """Estimate CoW write cost: read affected files + write all of them back."""
    affected_bytes = data_bytes * update_fraction
    read_s = affected_bytes / (cluster.disk_read_mb_s * 1024 * 1024)
    write_s = affected_bytes / (cluster.disk_write_mb_s * 1024 * 1024)
    return read_s + write_s


def model_mor_read_cost(
    data_bytes: int,
    delta_bytes: int,
    n_delta_files: int,
    cluster: ClusterConfig = DEFAULT_CLUSTER,
) -> float:
    """Estimate MoR read cost: scan base + read all deltas + merge."""
    base_read_s = data_bytes / (cluster.disk_read_mb_s * 1024 * 1024)
    delta_read_s = delta_bytes / (cluster.disk_read_mb_s * 1024 * 1024)
    # File open overhead is significant for many small delta files
    file_overhead_s = n_delta_files * 0.002
    merge_s = (delta_bytes * cluster.merge_overhead_factor) / (cluster.cpu_sort_mb_s * 1024 * 1024)
    return base_read_s + delta_read_s + file_overhead_s + merge_s


def model_compaction_cost(
    data_bytes: int,
    delta_bytes: int,
    cluster: ClusterConfig = DEFAULT_CLUSTER,
) -> float:
    """Estimate one-time compaction I/O cost."""
    total_input = data_bytes + delta_bytes
    read_s = total_input / (cluster.disk_read_mb_s * 1024 * 1024)
    sort_s = total_input / (cluster.cpu_sort_mb_s * 1024 * 1024)
    write_s = data_bytes / (cluster.disk_write_mb_s * 1024 * 1024)  # output ≈ data_bytes
    return read_s + sort_s + write_s


def estimate_compaction_cost(
    data_bytes: int,
    delta_bytes: int,
    n_delta_files: int,
    read_ops_per_hour: float,
    write_ops_per_hour: float,
    cluster: ClusterConfig = DEFAULT_CLUSTER,
) -> CompactionCostEstimate:
    """Full compaction cost + break-even analysis."""

    # Cost of one compaction run
    compact_s = model_compaction_cost(data_bytes, delta_bytes, cluster)

    # Overhead per read when deltas exist
    ideal_read_s = data_bytes / (cluster.disk_read_mb_s * 1024 * 1024)
    actual_read_s = model_mor_read_cost(data_bytes, delta_bytes, n_delta_files, cluster)
    delta_overhead_per_read_s = max(0.0, actual_read_s - ideal_read_s)

    # Read amplification
    amp = actual_read_s / max(ideal_read_s, 1e-9)

    # How many reads until saved time equals compaction cost?
    if delta_overhead_per_read_s > 0:
        break_even_reads = int(compact_s / delta_overhead_per_read_s)
    else:
        break_even_reads = int(1e9)  # never

    # Reads that will happen in ~1 hour
    reads_in_window = read_ops_per_hour
    savings_s = reads_in_window * delta_overhead_per_read_s
    roi = savings_s / max(compact_s, 1e-9)

    should_compact = roi > 1.0 or (n_delta_files > 20 and amp > 1.5)
    if should_compact:
        reason = (
            f"Compaction saves {savings_s:.2f}s of read overhead vs {compact_s:.2f}s cost "
            f"(ROI {roi:.1f}x) over {reads_in_window:.0f} reads/hour"
        )
    else:
        reason = (
            f"Compaction cost ({compact_s:.2f}s) not yet justified — "
            f"only {savings_s:.2f}s of projected read savings (ROI {roi:.2f}x)"
        )

    return CompactionCostEstimate(
        bytes_read_for_compaction=data_bytes + delta_bytes,
        bytes_written_after_compaction=data_bytes,
        estimated_compaction_s=compact_s,
        read_amplification_factor=amp,
        delta_merge_overhead_s=delta_overhead_per_read_s,
        break_even_ops=break_even_reads,
        roi_ratio=roi,
        should_compact_now=should_compact,
        reason=reason,
    )


def build_amplification_curve(
    data_bytes: int,
    bytes_per_delta_file: int,
    max_delta_files: int = 50,
    cluster: ClusterConfig = DEFAULT_CLUSTER,
) -> list[dict]:
    """Return a series mapping delta file count → read amplification factor."""
    curve = []
    ideal = data_bytes / (cluster.disk_read_mb_s * 1024 * 1024)
    for n in range(0, max_delta_files + 1):
        delta_bytes = n * bytes_per_delta_file
        actual = model_mor_read_cost(data_bytes, delta_bytes, n, cluster)
        curve.append({
            "delta_files": n,
            "amplification": actual / max(ideal, 1e-9),
            "extra_latency_ms": (actual - ideal) * 1000,
        })
    return curve
