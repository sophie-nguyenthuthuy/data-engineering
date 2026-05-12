"""Tests for code generation backends."""
import pytest
from pipeline_topology.dsl import parse_yaml
from pipeline_topology.targets import SparkTarget, FlinkTarget, DbtTarget
from pipeline_topology.compiler import Target


PIPELINE_YAML = """
pipeline:
  name: codegen_test
  sla:
    max_latency: "6h"
    dataset_size: "50gb"
  sources:
    - name: orders
      schema:
        - {name: order_id,   type: string,  nullable: false}
        - {name: customer_id,type: string,  nullable: false}
        - {name: amount,     type: double}
        - {name: status,     type: string}
    - name: customers
      schema:
        - {name: customer_id, type: string, nullable: false}
        - {name: region,      type: string}
  transforms:
    - name: active
      type: filter
      inputs: [orders]
      predicate: "status == 'completed'"
    - name: enriched
      type: join
      inputs: [active, customers]
      join_type: inner
      on: "active.customer_id == customers.customer_id"
    - name: by_region
      type: aggregate
      inputs: [enriched]
      group_by: [region]
      aggregations:
        - {name: count, function: count, column: order_id}
        - {name: revenue, function: sum, column: amount}
  sink:
    name: regional_summary
    input: by_region
"""


@pytest.fixture
def spec():
    return parse_yaml(PIPELINE_YAML)


# ── Spark ──────────────────────────────────────────────────────────

def test_spark_generates_files(spec):
    artifact = SparkTarget().generate(spec)
    assert artifact.target == Target.SPARK
    assert len(artifact.files) == 1
    fname = list(artifact.files.keys())[0]
    assert fname.endswith("_spark.py")


def test_spark_code_has_spark_session(spec):
    artifact = SparkTarget().generate(spec)
    code = list(artifact.files.values())[0]
    assert "SparkSession" in code


def test_spark_code_has_filter(spec):
    artifact = SparkTarget().generate(spec)
    code = list(artifact.files.values())[0]
    assert "status == 'completed'" in code


def test_spark_code_has_join(spec):
    artifact = SparkTarget().generate(spec)
    code = list(artifact.files.values())[0]
    assert ".join(" in code


def test_spark_code_has_aggregate(spec):
    artifact = SparkTarget().generate(spec)
    code = list(artifact.files.values())[0]
    assert ".groupBy(" in code
    assert ".agg(" in code


def test_spark_compiled_nodes_metadata(spec):
    artifact = SparkTarget().generate(spec)
    node_names = {n["name"] for n in artifact.compiled_nodes}
    assert "active" in node_names
    assert "enriched" in node_names
    assert "by_region" in node_names


# ── Flink ──────────────────────────────────────────────────────────

def test_flink_generates_files(spec):
    artifact = FlinkTarget().generate(spec)
    assert artifact.target == Target.FLINK
    assert len(artifact.files) == 1
    fname = list(artifact.files.keys())[0]
    assert fname.endswith("_flink.py")


def test_flink_code_has_stream_env(spec):
    artifact = FlinkTarget().generate(spec)
    code = list(artifact.files.values())[0]
    assert "StreamExecutionEnvironment" in code


def test_flink_code_has_filter(spec):
    artifact = FlinkTarget().generate(spec)
    code = list(artifact.files.values())[0]
    assert "status == 'completed'" in code


def test_flink_code_has_join_sql(spec):
    artifact = FlinkTarget().generate(spec)
    code = list(artifact.files.values())[0]
    assert "JOIN" in code.upper()


# ── dbt ────────────────────────────────────────────────────────────

def test_dbt_generates_model_files(spec):
    artifact = DbtTarget().generate(spec)
    assert artifact.target == Target.DBT
    model_files = [f for f in artifact.files if f.startswith("models/") and f.endswith(".sql")]
    # Should have models for each non-source node
    assert len(model_files) >= 4  # active, enriched, by_region, regional_summary


def test_dbt_generates_schema_yml(spec):
    artifact = DbtTarget().generate(spec)
    assert "models/schema.yml" in artifact.files
    schema_yml = artifact.files["models/schema.yml"]
    assert "models" in schema_yml
    assert "active" in schema_yml


def test_dbt_generates_sources_yml(spec):
    artifact = DbtTarget().generate(spec)
    assert "models/sources.yml" in artifact.files
    sources_yml = artifact.files["models/sources.yml"]
    assert "orders" in sources_yml
    assert "customers" in sources_yml


def test_dbt_generates_dbt_project_yml(spec):
    artifact = DbtTarget().generate(spec)
    assert "dbt_project.yml" in artifact.files
    proj = artifact.files["dbt_project.yml"]
    assert "codegen_test" in proj


def test_dbt_filter_uses_where(spec):
    artifact = DbtTarget().generate(spec)
    active_sql = artifact.files["models/active.sql"]
    assert "where" in active_sql.lower()
    assert "status == 'completed'" in active_sql


def test_dbt_aggregate_uses_group_by(spec):
    artifact = DbtTarget().generate(spec)
    agg_sql = artifact.files["models/by_region.sql"]
    assert "group by" in agg_sql.lower()
    assert "region" in agg_sql
    assert "SUM" in agg_sql or "sum" in agg_sql


def test_dbt_join_uses_ref(spec):
    artifact = DbtTarget().generate(spec)
    join_sql = artifact.files["models/enriched.sql"]
    assert "ref(" in join_sql
    assert "JOIN" in join_sql.upper()


def test_artifact_write_to(spec, tmp_path):
    artifact = DbtTarget().generate(spec)
    artifact.write_to(tmp_path)
    assert (tmp_path / "models" / "active.sql").exists()
    assert (tmp_path / "models" / "schema.yml").exists()
    assert (tmp_path / "dbt_project.yml").exists()
