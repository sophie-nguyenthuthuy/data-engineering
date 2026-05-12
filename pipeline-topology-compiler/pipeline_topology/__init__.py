"""Pipeline Topology Compiler — DSL → Spark / Flink / dbt with equivalence proofs."""

from .dsl import (
    Aggregation,
    AggFunction,
    FieldSchema,
    FieldType,
    JoinType,
    PipelineSpec,
    Schema,
    SLA,
    TransformNode,
    TransformType,
    extract_spec,
    field,
    parse_yaml,
    pipeline,
    sink,
    source,
    transform,
)
from .compiler import Target, optimize, select_target
from .targets import CompiledArtifact, DbtTarget, FlinkTarget, SparkTarget, get_target
from .checker import EquivalenceReport, EquivalenceStatus, Violation, check_equivalence

__version__ = "0.1.0"

__all__ = [
    # DSL
    "parse_yaml", "extract_spec", "pipeline", "source", "transform", "sink", "field",
    "PipelineSpec", "TransformNode", "Schema", "FieldSchema", "FieldType",
    "SLA", "TransformType", "JoinType", "AggFunction", "Aggregation",
    # Compiler
    "select_target", "optimize", "Target",
    # Targets
    "get_target", "SparkTarget", "FlinkTarget", "DbtTarget", "CompiledArtifact",
    # Checker
    "check_equivalence", "EquivalenceReport", "EquivalenceStatus", "Violation",
]
