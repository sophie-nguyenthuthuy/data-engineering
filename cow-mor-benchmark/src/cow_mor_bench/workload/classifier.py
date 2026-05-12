"""Query pattern classifier — infers workload class from observed operation traces."""

from __future__ import annotations

from dataclasses import dataclass

import numpy as np

from cow_mor_bench.workload.patterns import WorkloadClass, WorkloadProfile
from cow_mor_bench.workload.generator import WorkloadTrace, OperationRecord


@dataclass
class ClassificationResult:
    predicted_class: WorkloadClass
    confidence: float
    feature_vector: dict[str, float]
    reasoning: list[str]


def classify_trace(trace: WorkloadTrace) -> ClassificationResult:
    """Rule-based classifier that extracts features from a WorkloadTrace."""
    ops = trace.operations
    total = max(len(ops), 1)

    write_ops = [o for o in ops if o.op in ("insert", "update", "delete")]
    read_ops = [o for o in ops if o.op in ("full_scan", "point_lookup", "range_scan")]
    insert_ops = [o for o in ops if o.op == "insert"]
    update_ops = [o for o in ops if o.op == "update"]
    delete_ops = [o for o in ops if o.op == "delete"]
    scan_ops = [o for o in ops if o.op == "full_scan"]
    point_ops = [o for o in ops if o.op == "point_lookup"]

    n_writes = len(write_ops)
    n_reads = len(read_ops)
    write_ratio = n_writes / total
    read_ratio = n_reads / total
    scan_ratio = len(scan_ops) / max(n_reads, 1)
    point_ratio = len(point_ops) / max(n_reads, 1)
    delete_ratio = len(delete_ops) / max(n_writes, 1)
    update_ratio = len(update_ops) / max(n_writes, 1)
    insert_ratio = len(insert_ops) / max(n_writes, 1)

    avg_write_rows = np.mean([o.rows for o in write_ops]) if write_ops else 0.0
    avg_delta_files = trace.avg_delta_files_per_read

    features = {
        "write_ratio": write_ratio,
        "read_ratio": read_ratio,
        "scan_ratio_of_reads": scan_ratio,
        "point_ratio_of_reads": point_ratio,
        "delete_ratio_of_writes": delete_ratio,
        "update_ratio_of_writes": update_ratio,
        "insert_ratio_of_writes": insert_ratio,
        "avg_write_rows": float(avg_write_rows),
        "avg_delta_files_per_read": avg_delta_files,
    }

    reasoning: list[str] = []
    scores: dict[WorkloadClass, float] = {c: 0.0 for c in WorkloadClass}

    # OLAP: read-dominant + full scans dominate
    if read_ratio > 0.6:
        scores[WorkloadClass.OLAP_HEAVY] += 2.0
        reasoning.append(f"Read-dominant: {read_ratio:.0%} of ops are reads")
    if scan_ratio > 0.5:
        scores[WorkloadClass.OLAP_HEAVY] += 1.5
        reasoning.append(f"Full scans dominate reads: {scan_ratio:.0%}")

    # OLTP: write-dominant + small writes + point reads
    if write_ratio > 0.6:
        scores[WorkloadClass.OLTP_HEAVY] += 2.0
        reasoning.append(f"Write-dominant: {write_ratio:.0%} of ops are writes")
    if avg_write_rows < 500:
        scores[WorkloadClass.OLTP_HEAVY] += 1.0
        reasoning.append(f"Small write batches: avg {avg_write_rows:.0f} rows/op")
    if point_ratio > 0.5:
        scores[WorkloadClass.OLTP_HEAVY] += 1.0
        reasoning.append(f"Point reads dominate: {point_ratio:.0%} of reads")

    # Streaming: insert-heavy, small batches
    if insert_ratio > 0.7 and write_ratio > 0.6:
        scores[WorkloadClass.STREAMING_INGEST] += 2.5
        reasoning.append(f"Insert-heavy streaming: {insert_ratio:.0%} of writes are inserts")
    if avg_write_rows < 1000 and insert_ratio > 0.5:
        scores[WorkloadClass.STREAMING_INGEST] += 1.0

    # Batch update: large writes, high update ratio
    if avg_write_rows > 10_000:
        scores[WorkloadClass.BATCH_UPDATE] += 2.0
        reasoning.append(f"Large batch writes: avg {avg_write_rows:.0f} rows/op")
    if update_ratio > 0.5 and avg_write_rows > 5_000:
        scores[WorkloadClass.BATCH_UPDATE] += 1.5
        reasoning.append(f"Bulk updates: {update_ratio:.0%} of writes")

    # CDC: balanced insert/update/delete + small batches
    if delete_ratio > 0.1 and update_ratio > 0.3 and insert_ratio > 0.2:
        scores[WorkloadClass.CDC] += 2.0
        reasoning.append("Mix of inserts/updates/deletes — CDC-like pattern")
    if avg_delta_files > 5:
        scores[WorkloadClass.CDC] += 0.5
        reasoning.append(f"High delta file accumulation per read: {avg_delta_files:.1f}")

    # Mixed: nothing strongly dominant
    if max(scores.values()) < 1.5:
        scores[WorkloadClass.MIXED] += 2.0
        reasoning.append("No dominant pattern — classified as mixed workload")

    best_class = max(scores, key=lambda c: scores[c])
    total_score = sum(scores.values())
    confidence = scores[best_class] / max(total_score, 1.0)

    if not reasoning:
        reasoning.append("Balanced workload without strong patterns")

    return ClassificationResult(
        predicted_class=best_class,
        confidence=confidence,
        feature_vector=features,
        reasoning=reasoning,
    )


def classify_custom(
    write_ratio: float,
    update_fraction_of_table: float,
    avg_batch_rows: int,
    full_scan_ratio: float,
    point_read_ratio: float,
) -> ClassificationResult:
    """Classify a workload from user-supplied parameters without running a trace.

    Derives insert/update/delete split from write_ratio and update_fraction:
    high update_fraction → update-heavy; otherwise insert-heavy (streaming).
    """
    from cow_mor_bench.workload.generator import WorkloadTrace, OperationRecord

    n = 100
    trace = WorkloadTrace(profile_name="custom", engine_strategy="n/a", schema_name="n/a")

    write_count = int(n * write_ratio)
    read_count = n - write_count
    full_scan_count = int(read_count * full_scan_ratio)
    point_count = int(read_count * point_read_ratio)
    range_count = read_count - full_scan_count - point_count

    # Derive write mix from update_fraction_of_table:
    # low fraction (<0.05) → mostly inserts; high fraction → mostly updates
    if update_fraction_of_table < 0.05:
        insert_frac, update_frac, delete_frac = 0.80, 0.12, 0.08
    elif update_fraction_of_table < 0.15:
        insert_frac, update_frac, delete_frac = 0.45, 0.45, 0.10
    else:
        insert_frac, update_frac, delete_frac = 0.10, 0.75, 0.15

    for _ in range(int(write_count * update_frac)):
        trace.operations.append(OperationRecord(
            op="update", duration_s=0.01, rows=avg_batch_rows, bytes_io=1024, files=1,
        ))
    for _ in range(int(write_count * insert_frac)):
        trace.operations.append(OperationRecord(
            op="insert", duration_s=0.005, rows=avg_batch_rows, bytes_io=512, files=1,
        ))
    for _ in range(max(1, int(write_count * delete_frac))):
        trace.operations.append(OperationRecord(
            op="delete", duration_s=0.005, rows=0, bytes_io=256, files=1,
        ))
    for _ in range(full_scan_count):
        trace.operations.append(OperationRecord(
            op="full_scan", duration_s=0.5, rows=10000, bytes_io=5_000_000, files=5, delta_files=3,
        ))
    for _ in range(point_count):
        trace.operations.append(OperationRecord(
            op="point_lookup", duration_s=0.01, rows=1, bytes_io=10_000, files=1, delta_files=1,
        ))
    for _ in range(range_count):
        trace.operations.append(OperationRecord(
            op="range_scan", duration_s=0.1, rows=500, bytes_io=50_000, files=2, delta_files=2,
        ))

    trace.summarise()
    return classify_trace(trace)
