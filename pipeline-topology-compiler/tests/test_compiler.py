"""Tests for target selection and DAG optimizer."""
import pytest
from pipeline_topology.dsl import parse_yaml, SLA
from pipeline_topology.compiler import select_target, optimize, Target


BATCH_YAML = """
pipeline:
  name: batch_test
  sla:
    max_latency: "8h"
    dataset_size: "50gb"
  sources:
    - name: s
      schema:
        - {name: id, type: string}
        - {name: v,  type: double}
  transforms:
    - name: f
      type: filter
      inputs: [s]
      predicate: "v > 0"
  sink:
    name: out
    input: f
"""

STREAMING_YAML = """
pipeline:
  name: stream_test
  sla:
    max_latency: "30s"
    dataset_size: "2gb"
  sources:
    - name: events
      schema:
        - {name: id,  type: string}
        - {name: ts,  type: timestamp}
        - {name: val, type: double}
  transforms:
    - name: filtered
      type: filter
      inputs: [events]
      predicate: "val > 0"
  sink:
    name: out
    input: filtered
"""

LARGE_BATCH_YAML = """
pipeline:
  name: large_batch
  sla:
    max_latency: "4h"
    dataset_size: "500gb"
  sources:
    - name: s
      schema:
        - {name: id, type: string}
  transforms:
    - name: f
      type: filter
      inputs: [s]
      predicate: "id != ''"
  sink:
    name: out
    input: f
"""


def test_select_dbt_for_small_batch():
    spec = parse_yaml(BATCH_YAML)
    reason = select_target(spec)
    assert reason.target == Target.DBT


def test_select_flink_for_realtime():
    spec = parse_yaml(STREAMING_YAML)
    reason = select_target(spec)
    assert reason.target == Target.FLINK


def test_select_spark_for_large_dataset():
    spec = parse_yaml(LARGE_BATCH_YAML)
    reason = select_target(spec)
    assert reason.target == Target.SPARK


def test_reason_has_explanation():
    spec = parse_yaml(BATCH_YAML)
    reason = select_target(spec)
    assert reason.reason
    assert reason.latency_s > 0
    assert reason.size_gb > 0


def test_optimizer_removes_identity_select():
    yaml_text = """
pipeline:
  name: opt_test
  sla:
    max_latency: "6h"
    dataset_size: "10gb"
  sources:
    - name: s
      schema:
        - {name: a, type: string}
        - {name: b, type: integer}
  transforms:
    - name: passthru
      type: select
      inputs: [s]
      columns: [a, b]
    - name: filtered
      type: filter
      inputs: [passthru]
      predicate: "a != ''"
  sink:
    name: out
    input: filtered
"""
    spec = parse_yaml(yaml_text)
    assert "passthru" in spec.nodes  # identity select exists before optimization

    optimized = optimize(spec)
    # The identity SELECT should be eliminated
    assert "passthru" not in optimized.nodes
    # The filter should now point directly to the source
    assert optimized.nodes["filtered"].inputs == ["s"]


def test_optimizer_merges_consecutive_selects():
    yaml_text = """
pipeline:
  name: merge_select_test
  sla:
    max_latency: "6h"
    dataset_size: "10gb"
  sources:
    - name: s
      schema:
        - {name: a, type: string}
        - {name: b, type: integer}
        - {name: c, type: double}
  transforms:
    - name: sel1
      type: select
      inputs: [s]
      columns: [a, b, c]
    - name: sel2
      type: select
      inputs: [sel1]
      columns: [a, b]
  sink:
    name: out
    input: sel2
"""
    spec = parse_yaml(yaml_text)
    optimized = optimize(spec)
    # sel1 is identity and gets removed; sel2 now points to s
    remaining_selects = [n for n in optimized.nodes.values() if n.transform_type.value == "select"]
    for sel in remaining_selects:
        assert "s" in sel.inputs or sel.name == "out"


def test_optimizer_preserves_semantics():
    spec = parse_yaml(BATCH_YAML)
    optimized = optimize(spec)
    # Topology order should still be valid
    order = optimized.topological_order()
    assert len(order) >= 2  # at least source + something
