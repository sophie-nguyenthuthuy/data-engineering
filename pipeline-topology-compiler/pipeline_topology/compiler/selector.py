"""Target selection: given a PipelineSpec's SLA, choose Spark / Flink / dbt."""
from __future__ import annotations

from enum import Enum

from ..dsl.ir import PipelineSpec
from ..dsl.types import SLA, TransformType


class Target(str, Enum):
    SPARK = "spark"
    FLINK = "flink"
    DBT = "dbt"


class SelectionReason:
    def __init__(self, target: Target, reason: str, latency_s: int, size_gb: float) -> None:
        self.target = target
        self.reason = reason
        self.latency_s = latency_s
        self.size_gb = size_gb

    def __repr__(self) -> str:
        return (
            f"SelectionReason(target={self.target.value}, latency={self.latency_s}s, "
            f"size={self.size_gb:.1f}GB, reason={self.reason!r})"
        )


_FLINK_LATENCY_THRESHOLD_S = 300       # < 5 minutes → Flink
_SPARK_LATENCY_THRESHOLD_S = 3600      # 5min–60min → Spark
_SPARK_SIZE_THRESHOLD_GB = 100.0       # > 100 GB forces Spark even for long SLAs
_STREAMING_TRANSFORMS = {TransformType.WINDOW}


def _has_streaming_transforms(spec: PipelineSpec) -> bool:
    return any(n.transform_type in _STREAMING_TRANSFORMS for n in spec.nodes.values())


def select_target(spec: PipelineSpec) -> SelectionReason:
    sla = spec.sla
    latency = sla.latency_seconds()
    size_gb = sla.dataset_size_gb()
    has_streaming = _has_streaming_transforms(spec)

    if has_streaming and latency < _SPARK_LATENCY_THRESHOLD_S:
        return SelectionReason(
            Target.FLINK,
            "pipeline contains windowed/streaming transforms with sub-hour latency SLA",
            latency,
            size_gb,
        )

    if latency < _FLINK_LATENCY_THRESHOLD_S:
        return SelectionReason(
            Target.FLINK,
            f"latency SLA {latency}s is below {_FLINK_LATENCY_THRESHOLD_S}s threshold",
            latency,
            size_gb,
        )

    if latency < _SPARK_LATENCY_THRESHOLD_S:
        return SelectionReason(
            Target.SPARK,
            f"latency SLA {latency}s is between {_FLINK_LATENCY_THRESHOLD_S}s–{_SPARK_LATENCY_THRESHOLD_S}s",
            latency,
            size_gb,
        )

    if size_gb > _SPARK_SIZE_THRESHOLD_GB:
        return SelectionReason(
            Target.SPARK,
            f"dataset size {size_gb:.1f}GB exceeds {_SPARK_SIZE_THRESHOLD_GB}GB threshold",
            latency,
            size_gb,
        )

    return SelectionReason(
        Target.DBT,
        f"batch pipeline (latency={latency}s, size={size_gb:.1f}GB) maps to SQL/dbt",
        latency,
        size_gb,
    )
