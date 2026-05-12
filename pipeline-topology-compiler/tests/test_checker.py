"""Tests for the proof-of-equivalence checker."""
import pytest
from pipeline_topology.dsl import parse_yaml
from pipeline_topology.targets import SparkTarget, DbtTarget, FlinkTarget
from pipeline_topology.checker import (
    check_equivalence,
    EquivalenceStatus,
    normalize,
)
from pipeline_topology.checker.normalizer import _canonical_predicate
from pipeline_topology.targets.base import CompiledArtifact
from pipeline_topology.compiler.selector import Target


SIMPLE_YAML = """
pipeline:
  name: checker_test
  sla:
    max_latency: "6h"
    dataset_size: "50gb"
  sources:
    - name: events
      schema:
        - {name: id,     type: string,  nullable: false}
        - {name: value,  type: double}
        - {name: category, type: string}
  transforms:
    - name: filtered
      type: filter
      inputs: [events]
      predicate: "value > 0"
    - name: by_cat
      type: aggregate
      inputs: [filtered]
      group_by: [category]
      aggregations:
        - {name: total, function: sum, column: value}
        - {name: cnt,   function: count, column: id}
  sink:
    name: cat_summary
    input: by_cat
"""


@pytest.fixture
def spec():
    return parse_yaml(SIMPLE_YAML)


# ── Positive cases ─────────────────────────────────────────────────

def test_spark_artifact_is_equivalent(spec):
    artifact = SparkTarget().generate(spec)
    report = check_equivalence(spec, artifact)
    assert report.is_equivalent(), f"Expected EQUIVALENT, got:\n{report}"


def test_dbt_artifact_is_equivalent(spec):
    artifact = DbtTarget().generate(spec)
    report = check_equivalence(spec, artifact)
    assert report.is_equivalent(), f"Expected EQUIVALENT, got:\n{report}"


def test_flink_artifact_is_equivalent(spec):
    artifact = FlinkTarget().generate(spec)
    report = check_equivalence(spec, artifact)
    assert report.is_equivalent(), f"Expected EQUIVALENT, got:\n{report}"


def test_equivalence_passes_list_nonempty(spec):
    artifact = SparkTarget().generate(spec)
    report = check_equivalence(spec, artifact)
    assert len(report.checks_passed) > 0


# ── Negative cases ─────────────────────────────────────────────────

def test_tampered_predicate_fails():
    spec = parse_yaml(SIMPLE_YAML)
    artifact = SparkTarget().generate(spec)
    # Tamper with the compiled predicate
    for node in artifact.compiled_nodes:
        if node["name"] == "filtered":
            node["predicate"] = "value > 999"  # wrong threshold
    report = check_equivalence(spec, artifact)
    assert not report.is_equivalent()
    violation_checks = {v.check for v in report.violations}
    assert any("predicate" in c for c in violation_checks)


def test_missing_aggregation_fails():
    spec = parse_yaml(SIMPLE_YAML)
    artifact = SparkTarget().generate(spec)
    # Remove one aggregation from compiled metadata
    for node in artifact.compiled_nodes:
        if node["name"] == "by_cat" and node["aggregations"]:
            node["aggregations"] = node["aggregations"][:1]  # keep only one
    report = check_equivalence(spec, artifact)
    assert not report.is_equivalent()


def test_missing_node_fails():
    spec = parse_yaml(SIMPLE_YAML)
    artifact = SparkTarget().generate(spec)
    # Remove a node from compiled metadata
    artifact.compiled_nodes = [n for n in artifact.compiled_nodes if n["name"] != "filtered"]
    report = check_equivalence(spec, artifact)
    assert not report.is_equivalent()


def test_wrong_group_by_fails():
    spec = parse_yaml(SIMPLE_YAML)
    artifact = SparkTarget().generate(spec)
    for node in artifact.compiled_nodes:
        if node["name"] == "by_cat":
            node["group_by"] = ["id"]  # wrong column
    report = check_equivalence(spec, artifact)
    assert not report.is_equivalent()


# ── Normalizer tests ───────────────────────────────────────────────

def test_normalizer_sorts_and_clauses():
    pred1 = "b < 2 AND a > 1"
    pred2 = "a > 1 AND b < 2"
    assert _canonical_predicate(pred1) == _canonical_predicate(pred2)


def test_normalizer_collapses_whitespace():
    pred = "  a   >  1  "
    assert "  " not in _canonical_predicate(pred)


def test_normalizer_removes_identity_selects():
    yaml_text = """
pipeline:
  name: norm_test
  sla: {}
  sources:
    - name: s
      schema:
        - {name: a, type: string}
        - {name: b, type: integer}
  transforms:
    - name: identity_sel
      type: select
      inputs: [s]
      columns: [a, b]
    - name: f
      type: filter
      inputs: [identity_sel]
      predicate: "a != ''"
  sink:
    name: out
    input: f
"""
    spec = parse_yaml(yaml_text)
    normed = normalize(spec)
    # Identity select should be collapsed
    assert "identity_sel" not in normed.nodes
    assert normed.nodes["f"].inputs == ["s"]


def test_normalizer_sorts_aggregations():
    yaml_text = """
pipeline:
  name: agg_order_test
  sla: {}
  sources:
    - name: s
      schema:
        - {name: cat, type: string}
        - {name: v,   type: double}
  transforms:
    - name: agg
      type: aggregate
      inputs: [s]
      group_by: [cat]
      aggregations:
        - {name: z_last,  function: sum,   column: v}
        - {name: a_first, function: count, column: v}
  sink:
    name: out
    input: agg
"""
    spec = parse_yaml(yaml_text)
    normed = normalize(spec)
    agg_names = [a.output_name for a in normed.nodes["agg"].aggregations]
    assert agg_names == sorted(agg_names)


# ── Full round-trip ────────────────────────────────────────────────

def test_round_trip_spark():
    yaml_text = """
pipeline:
  name: rt_spark
  sla:
    max_latency: "30m"
    dataset_size: "200gb"
  sources:
    - name: raw
      schema:
        - {name: id,  type: string,  nullable: false}
        - {name: val, type: double}
        - {name: cat, type: string}
  transforms:
    - name: pos
      type: filter
      inputs: [raw]
      predicate: "val > 0"
    - name: summary
      type: aggregate
      inputs: [pos]
      group_by: [cat]
      aggregations:
        - {name: total, function: sum, column: val}
  sink:
    name: out
    input: summary
"""
    spec = parse_yaml(yaml_text)
    artifact = SparkTarget().generate(spec)
    report = check_equivalence(spec, artifact)
    assert report.is_equivalent(), str(report)
