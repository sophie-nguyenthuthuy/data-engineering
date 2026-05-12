"""Strategy recommender.

Combines workload classification, compaction cost modelling, and benchmark
results to produce a per-table recommendation: CoW, MoR, or MoR+compact.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum

from cow_mor_bench.benchmark.runner import BenchmarkResult
from cow_mor_bench.compaction.model import (
    ClusterConfig,
    CompactionCostEstimate,
    DEFAULT_CLUSTER,
    estimate_compaction_cost,
)
from cow_mor_bench.workload.classifier import ClassificationResult, WorkloadClass


class Recommendation(str, Enum):
    COW = "copy_on_write"
    MOR = "merge_on_read"
    MOR_WITH_COMPACTION = "merge_on_read_with_compaction"


@dataclass
class StrategyRecommendation:
    table_name: str
    recommended: Recommendation
    confidence: float
    write_latency_winner: str
    read_latency_winner: str
    space_winner: str
    compaction_cost: CompactionCostEstimate | None
    reasoning: list[str]
    alternatives: list[str] = field(default_factory=list)

    def summary(self) -> str:
        lines = [
            f"Table: {self.table_name}",
            f"Recommendation: {self.recommended.value.upper()} (confidence {self.confidence:.0%})",
            "",
            "Reasoning:",
        ]
        for r in self.reasoning:
            lines.append(f"  • {r}")
        if self.alternatives:
            lines.append("")
            lines.append("Alternatives:")
            for a in self.alternatives:
                lines.append(f"  – {a}")
        if self.compaction_cost:
            cc = self.compaction_cost
            lines += [
                "",
                "Compaction model:",
                f"  Amplification factor : {cc.read_amplification_factor:.2f}x",
                f"  Estimated compact cost: {cc.estimated_compaction_s:.3f}s",
                f"  ROI of compaction    : {cc.roi_ratio:.1f}x",
                f"  Compact now?         : {'YES' if cc.should_compact_now else 'no'}",
                f"  {cc.reason}",
            ]
        return "\n".join(lines)


def recommend(
    result: BenchmarkResult,
    classification: ClassificationResult,
    table_name: str = "table",
    read_ops_per_hour: float = 100.0,
    cluster: ClusterConfig = DEFAULT_CLUSTER,
) -> StrategyRecommendation:
    ct = result.cow_trace
    mt = result.mor_trace
    ms = result.mor_final_stats

    write_winner = "CoW" if ct.total_write_s < mt.total_write_s else "MoR"
    read_winner = "CoW" if ct.total_read_s < mt.total_read_s else "MoR"
    cow_bytes = result.cow_final_stats.total_data_bytes
    mor_bytes = ms.total_data_bytes + ms.total_delta_bytes
    space_winner = "CoW" if cow_bytes < mor_bytes else "MoR"

    compaction_cost = estimate_compaction_cost(
        data_bytes=ms.total_data_bytes,
        delta_bytes=ms.total_delta_bytes,
        n_delta_files=ms.delta_file_count,
        read_ops_per_hour=read_ops_per_hour,
        write_ops_per_hour=max(ct.write_ops, 1) * 3600 / 1,
        cluster=cluster,
    )

    reasoning: list[str] = []
    alternatives: list[str] = []

    # Use classification to drive the primary recommendation
    wc = classification.predicted_class

    if wc == WorkloadClass.OLAP_HEAVY:
        rec = Recommendation.COW
        reasoning.append("OLAP-heavy workload: reads dominate — CoW's pre-merged files win")
        reasoning.append(
            f"Read time: CoW {ct.total_read_s:.3f}s vs MoR {mt.total_read_s:.3f}s"
        )
        alternatives.append(
            "MoR + aggressive compaction if ingest rate is also high (>10k rows/s)"
        )

    elif wc == WorkloadClass.STREAMING_INGEST:
        rec = Recommendation.MOR
        reasoning.append("Streaming ingest: high insert rate favours MoR's append-only writes")
        reasoning.append(
            f"Write time: MoR {mt.total_write_s:.3f}s vs CoW {ct.total_write_s:.3f}s"
        )
        if compaction_cost.should_compact_now:
            rec = Recommendation.MOR_WITH_COMPACTION
            reasoning.append(
                f"Compaction recommended: ROI {compaction_cost.roi_ratio:.1f}x — "
                "delta files are hurting reads"
            )
        alternatives.append("CoW if downstream consumers run frequent full-table scans")

    elif wc == WorkloadClass.OLTP_HEAVY:
        rec = Recommendation.MOR
        reasoning.append("OLTP-heavy: high-frequency small writes favour MoR's O(delta) writes")
        reasoning.append(
            f"Write time: MoR {mt.total_write_s:.3f}s vs CoW {ct.total_write_s:.3f}s"
        )
        if ms.delta_file_count > 15:
            rec = Recommendation.MOR_WITH_COMPACTION
            reasoning.append(
                f"Delta file accumulation ({ms.delta_file_count} files) is degrading read latency"
            )
        alternatives.append("CoW if point-read SLA is strict and compaction budget is limited")

    elif wc == WorkloadClass.BATCH_UPDATE:
        rec = Recommendation.COW
        reasoning.append(
            "Batch update: large writes rewrite many files in both strategies; "
            "CoW produces cleaner read paths for subsequent analytics"
        )
        reasoning.append(
            f"p95 read latency: CoW {ct.p95_read_ms:.1f}ms vs MoR {mt.p95_read_ms:.1f}ms"
        )
        alternatives.append("MoR if write throughput is the bottleneck (pipeline SLA)")

    elif wc == WorkloadClass.CDC:
        rec = Recommendation.MOR_WITH_COMPACTION
        reasoning.append("CDC workload: continuous small mutations → MoR is cheaper to write")
        reasoning.append("Scheduled compaction required to control read amplification")
        reasoning.append(
            f"Read amplification without compaction: {compaction_cost.read_amplification_factor:.2f}x"
        )
        alternatives.append(
            "CoW if the change capture rate is low enough (<1% table per hour)"
        )

    else:  # MIXED
        # Let the numbers decide
        if ct.total_read_s < mt.total_read_s and ct.total_write_s < mt.total_write_s:
            rec = Recommendation.COW
            reasoning.append("Mixed workload: CoW wins on both reads and writes in this profile")
        elif mt.total_write_s < ct.total_write_s * 0.7:
            if compaction_cost.should_compact_now:
                rec = Recommendation.MOR_WITH_COMPACTION
                reasoning.append(
                    "Mixed workload: MoR writes are significantly faster; "
                    "compaction keeps reads competitive"
                )
            else:
                rec = Recommendation.MOR
                reasoning.append("Mixed workload: MoR write advantage outweighs read overhead")
        else:
            rec = Recommendation.COW
            reasoning.append(
                "Mixed workload: no clear MoR advantage — CoW's simpler read path preferred"
            )
        alternatives.append("Profile under peak write load and peak read load separately")

    # Override: if MoR read latency is > 3x CoW, always flag compaction
    if (
        rec == Recommendation.MOR
        and mt.total_read_s > ct.total_read_s * 3
    ):
        rec = Recommendation.MOR_WITH_COMPACTION
        reasoning.append(
            f"MoR read overhead is {mt.total_read_s / max(ct.total_read_s, 1e-9):.1f}x CoW — "
            "compaction is critical"
        )

    confidence = min(0.95, classification.confidence + 0.1)

    return StrategyRecommendation(
        table_name=table_name,
        recommended=rec,
        confidence=confidence,
        write_latency_winner=write_winner,
        read_latency_winner=read_winner,
        space_winner=space_winner,
        compaction_cost=compaction_cost,
        reasoning=reasoning,
        alternatives=alternatives,
    )


def recommend_from_params(
    write_ratio: float,
    update_fraction_of_table: float,
    avg_batch_rows: int,
    full_scan_ratio: float,
    point_read_ratio: float,
    data_gb: float = 10.0,
    read_ops_per_hour: float = 100.0,
    table_name: str = "table",
    cluster: ClusterConfig = DEFAULT_CLUSTER,
) -> StrategyRecommendation:
    """Lightweight recommendation without running a full benchmark."""
    from cow_mor_bench.workload.classifier import classify_custom
    from cow_mor_bench.compaction.model import estimate_compaction_cost

    cls = classify_custom(
        write_ratio=write_ratio,
        update_fraction_of_table=update_fraction_of_table,
        avg_batch_rows=avg_batch_rows,
        full_scan_ratio=full_scan_ratio,
        point_read_ratio=point_read_ratio,
    )

    data_bytes = int(data_gb * 1024**3)
    # Estimate delta accumulation: ~10% of data_bytes per write_ops bucket
    delta_bytes = int(data_bytes * write_ratio * update_fraction_of_table * 0.5)
    n_delta_files = max(1, int(write_ratio * 50))

    compaction_cost = estimate_compaction_cost(
        data_bytes=data_bytes,
        delta_bytes=delta_bytes,
        n_delta_files=n_delta_files,
        read_ops_per_hour=read_ops_per_hour,
        write_ops_per_hour=write_ratio * 3600,
        cluster=cluster,
    )

    wc = cls.predicted_class
    reasoning = list(cls.reasoning)
    alternatives: list[str] = []

    if wc == WorkloadClass.OLAP_HEAVY:
        rec = Recommendation.COW
        reasoning.insert(0, "OLAP-heavy pattern — CoW eliminates per-read delta merging")
    elif wc in (WorkloadClass.OLTP_HEAVY, WorkloadClass.CDC, WorkloadClass.STREAMING_INGEST):
        if compaction_cost.should_compact_now:
            rec = Recommendation.MOR_WITH_COMPACTION
        else:
            rec = Recommendation.MOR
        reasoning.insert(0, f"{wc.value} — MoR's append writes are cheaper per operation")
    elif wc == WorkloadClass.BATCH_UPDATE:
        rec = Recommendation.COW
        reasoning.insert(0, "Batch update — CoW keeps reads clean after large mutations")
    else:
        rec = Recommendation.MOR_WITH_COMPACTION if compaction_cost.should_compact_now else Recommendation.COW
        reasoning.insert(0, "Mixed workload — defaulting to CoW for simpler read semantics")

    return StrategyRecommendation(
        table_name=table_name,
        recommended=rec,
        confidence=cls.confidence,
        write_latency_winner="MoR (estimated)",
        read_latency_winner="CoW (estimated)",
        space_winner="CoW (estimated)",
        compaction_cost=compaction_cost,
        reasoning=reasoning,
        alternatives=alternatives,
    )
