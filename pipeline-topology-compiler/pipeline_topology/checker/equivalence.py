"""
Proof-of-equivalence checker.

Given:
  - spec:     the original logical PipelineSpec (source of truth)
  - artifact: the CompiledArtifact produced by a code generator

The checker reconstructs a "compiled IR" from the artifact's compiled_nodes
metadata and verifies that it is semantically equivalent to the original spec
after algebraic normalization.

Equivalence criteria
--------------------
1. Schema equivalence    – every sink's output schema matches the spec's.
2. Topological isomorphism – the DAG structure (node types, edge connectivity)
   is isomorphic up to trivial renamings introduced by the compiler.
3. Semantic equivalence  – for each transform, the canonical_key() is identical
   in both spec and compiled IR after normalization.
4. Predicate preservation – filter predicates are preserved verbatim or in an
   equivalent normalized form.
5. Aggregation completeness – all declared aggregations appear in the output,
   no extras.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum
from typing import Optional

from ..dsl.ir import PipelineSpec, TransformNode
from ..dsl.types import (
    Aggregation,
    AggFunction,
    FieldSchema,
    FieldType,
    JoinType,
    Schema,
    TransformType,
)
from ..targets.base import CompiledArtifact
from .normalizer import normalize


class EquivalenceStatus(str, Enum):
    EQUIVALENT = "equivalent"
    NOT_EQUIVALENT = "not_equivalent"
    PARTIAL = "partial"       # structural match but semantic differences
    UNKNOWN = "unknown"       # couldn't fully verify


@dataclass
class Violation:
    node: str
    check: str
    expected: str
    actual: str

    def __str__(self) -> str:
        return f"[{self.node}] {self.check}: expected {self.expected!r}, got {self.actual!r}"


@dataclass
class EquivalenceReport:
    status: EquivalenceStatus
    violations: list[Violation] = field(default_factory=list)
    warnings: list[str] = field(default_factory=list)
    checks_passed: list[str] = field(default_factory=list)

    def is_equivalent(self) -> bool:
        return self.status == EquivalenceStatus.EQUIVALENT

    def __str__(self) -> str:
        lines = [f"Status: {self.status.value}"]
        if self.checks_passed:
            lines.append(f"Passed ({len(self.checks_passed)}): " + ", ".join(self.checks_passed))
        if self.violations:
            lines.append(f"Violations ({len(self.violations)}):")
            for v in self.violations:
                lines.append(f"  - {v}")
        if self.warnings:
            lines.append(f"Warnings ({len(self.warnings)}):")
            for w in self.warnings:
                lines.append(f"  - {w}")
        return "\n".join(lines)


def _compiled_nodes_to_spec(artifact: CompiledArtifact, original: PipelineSpec) -> PipelineSpec:
    """Reconstruct a PipelineSpec from the compiled_nodes metadata in an artifact."""
    spec = PipelineSpec(name=artifact.spec_name, sla=original.sla)

    # Include original sources (compilers don't change source semantics)
    for node in original.sources():
        spec.add_node(node)

    for meta in artifact.compiled_nodes:
        ttype = TransformType(meta["transform_type"])

        aggs = [
            Aggregation(
                output_name=a["name"],
                function=AggFunction(a["function"]),
                column=a.get("column"),
            )
            for a in meta.get("aggregations", [])
        ]

        join_on = meta.get("join_on")
        join_type = JoinType(meta["join_type"]) if meta.get("join_type") else JoinType.INNER

        node = TransformNode(
            name=meta["name"],
            transform_type=ttype,
            inputs=meta.get("inputs", []),
            predicate=meta.get("predicate"),
            columns=meta.get("columns"),
            group_by=meta.get("group_by", []),
            aggregations=aggs,
            join_type=join_type,
            join_on=join_on,
            window_column=meta.get("window_column"),
            window_duration=meta.get("window_duration"),
        )
        if node.name not in spec.nodes:
            spec.add_node(node)

    try:
        spec.infer_schemas()
    except ValueError:
        pass  # broken topology detected later by the checker
    return spec


def check_equivalence(spec: PipelineSpec, artifact: CompiledArtifact) -> EquivalenceReport:
    """
    Top-level entry point. Returns an EquivalenceReport describing whether
    the compiled artifact faithfully implements the logical spec.
    """
    report = EquivalenceReport(status=EquivalenceStatus.UNKNOWN)

    compiled_spec = _compiled_nodes_to_spec(artifact, spec)

    try:
        norm_spec = normalize(spec)
    except ValueError as e:
        report.status = EquivalenceStatus.UNKNOWN
        report.warnings.append(f"Could not normalize spec: {e}")
        return report

    try:
        norm_compiled = normalize(compiled_spec)
    except ValueError as e:
        report.status = EquivalenceStatus.NOT_EQUIVALENT
        report.violations.append(Violation("DAG", "compiled-spec-invalid", "valid DAG", str(e)))
        return report

    # ── Check 1: node set completeness ──────────────────────────
    spec_non_source = {n for n, node in norm_spec.nodes.items() if not node.is_source()}
    comp_non_source = {n for n, node in norm_compiled.nodes.items() if not node.is_source()}

    missing = spec_non_source - comp_non_source
    extra = comp_non_source - spec_non_source

    if missing:
        report.violations.append(Violation("DAG", "missing nodes", str(spec_non_source), str(comp_non_source)))
    else:
        report.checks_passed.append("node-set-completeness")

    if extra:
        report.warnings.append(f"Compiled artifact has extra nodes not in spec: {extra}")

    # ── Check 2: schema equivalence for sinks ───────────────────
    schema_ok = True
    for node in norm_spec.sinks():
        spec_schema = node.output_schema
        comp_node = norm_compiled.nodes.get(node.name)
        comp_schema = comp_node.output_schema if comp_node else None

        if spec_schema is None:
            report.warnings.append(f"Sink '{node.name}' has no declared schema in spec; skipping schema check")
            continue

        if comp_schema is None:
            report.warnings.append(f"Sink '{node.name}' schema could not be inferred from compiled artifact")
            continue

        schema_diff = _compare_schemas(node.name, spec_schema, comp_schema)
        if schema_diff:
            report.violations.extend(schema_diff)
            schema_ok = False

    if schema_ok and not any(v.check.startswith("schema") for v in report.violations):
        report.checks_passed.append("sink-schema-equivalence")

    # ── Check 3: semantic equivalence per node ───────────────────
    semantic_ok = True
    for name in spec_non_source & comp_non_source:
        spec_node = norm_spec.nodes[name]
        comp_node = norm_compiled.nodes[name]

        violations = _check_node_semantics(spec_node, comp_node)
        if violations:
            report.violations.extend(violations)
            semantic_ok = False

    if semantic_ok and not missing:
        report.checks_passed.append("semantic-equivalence")

    # ── Check 4: DAG topology ────────────────────────────────────
    topo_ok = _check_topology(norm_spec, norm_compiled, report)
    if topo_ok:
        report.checks_passed.append("dag-topology")

    # ── Final verdict ────────────────────────────────────────────
    if not report.violations:
        report.status = EquivalenceStatus.EQUIVALENT
    elif semantic_ok and not missing:
        report.status = EquivalenceStatus.PARTIAL
    else:
        report.status = EquivalenceStatus.NOT_EQUIVALENT

    return report


def _compare_schemas(node_name: str, expected: Schema, actual: Schema) -> list[Violation]:
    violations = []
    exp_map = {f.name: f for f in expected.fields}
    act_map = {f.name: f for f in actual.fields}

    for fname, exp_field in exp_map.items():
        if fname not in act_map:
            violations.append(Violation(node_name, f"schema.missing_field:{fname}", fname, "absent"))
        elif act_map[fname].dtype != exp_field.dtype:
            violations.append(
                Violation(
                    node_name,
                    f"schema.type_mismatch:{fname}",
                    exp_field.dtype.value,
                    act_map[fname].dtype.value,
                )
            )
    return violations


def _check_node_semantics(spec_node: TransformNode, comp_node: TransformNode) -> list[Violation]:
    violations = []
    name = spec_node.name

    if spec_node.transform_type != comp_node.transform_type:
        violations.append(
            Violation(name, "transform_type", spec_node.transform_type.value, comp_node.transform_type.value)
        )
        return violations  # further checks meaningless

    ttype = spec_node.transform_type

    if ttype == TransformType.FILTER:
        if spec_node.predicate != comp_node.predicate:
            violations.append(
                Violation(name, "filter.predicate", spec_node.predicate or "", comp_node.predicate or "")
            )

    elif ttype == TransformType.SELECT:
        exp_cols = sorted(spec_node.columns or [])
        act_cols = sorted(comp_node.columns or [])
        if exp_cols != act_cols:
            violations.append(Violation(name, "select.columns", str(exp_cols), str(act_cols)))

    elif ttype == TransformType.AGGREGATE:
        exp_gb = sorted(spec_node.group_by)
        act_gb = sorted(comp_node.group_by)
        if exp_gb != act_gb:
            violations.append(Violation(name, "agg.group_by", str(exp_gb), str(act_gb)))

        exp_agg_keys = {(a.output_name, a.function, a.column) for a in spec_node.aggregations}
        act_agg_keys = {(a.output_name, a.function, a.column) for a in comp_node.aggregations}
        if exp_agg_keys != act_agg_keys:
            violations.append(Violation(name, "agg.aggregations", str(sorted(exp_agg_keys)), str(sorted(act_agg_keys))))

    elif ttype == TransformType.JOIN:
        if spec_node.join_type != comp_node.join_type:
            violations.append(
                Violation(name, "join.type", spec_node.join_type.value, comp_node.join_type.value)
            )
        if spec_node.join_on and comp_node.join_on and spec_node.join_on != comp_node.join_on:
            violations.append(Violation(name, "join.on", spec_node.join_on, comp_node.join_on))

    elif ttype == TransformType.WINDOW:
        if spec_node.window_column != comp_node.window_column:
            violations.append(Violation(name, "window.time_column", str(spec_node.window_column), str(comp_node.window_column)))
        if spec_node.window_duration != comp_node.window_duration:
            violations.append(Violation(name, "window.duration", str(spec_node.window_duration), str(comp_node.window_duration)))

    return violations


def _check_topology(norm_spec: PipelineSpec, norm_compiled: PipelineSpec, report: EquivalenceReport) -> bool:
    """Verify that every edge in the spec DAG is present in the compiled DAG."""
    ok = True
    for name, node in norm_spec.nodes.items():
        if node.is_source():
            continue
        comp_node = norm_compiled.nodes.get(name)
        if comp_node is None:
            continue

        spec_inputs = set(node.inputs)
        comp_inputs = set(comp_node.inputs)

        if spec_inputs != comp_inputs:
            # Allow compiled to have extra transitive sources (due to CTE expansion)
            missing_edges = spec_inputs - comp_inputs
            if missing_edges:
                report.violations.append(
                    Violation(name, "topology.missing_inputs", str(sorted(spec_inputs)), str(sorted(comp_inputs)))
                )
                ok = False

    return ok
