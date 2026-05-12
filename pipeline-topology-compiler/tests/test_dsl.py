"""Tests for DSL parsing (YAML and decorator)."""
import pytest
from pipeline_topology.dsl import (
    parse_yaml,
    extract_spec,
    pipeline,
    source,
    transform,
    sink,
    field,
    Schema,
    SLA,
    TransformType,
    JoinType,
    AggFunction,
)


BATCH_YAML = """
pipeline:
  name: test_batch
  sla:
    max_latency: "6h"
    dataset_size: "80gb"
  sources:
    - name: orders
      schema:
        - {name: order_id,   type: string,  nullable: false}
        - {name: customer_id,type: string,  nullable: false}
        - {name: amount,     type: double,  nullable: false}
        - {name: status,     type: string}
  transforms:
    - name: active
      type: filter
      inputs: [orders]
      predicate: "status == 'active'"
    - name: totals
      type: aggregate
      inputs: [active]
      group_by: [customer_id]
      aggregations:
        - {name: order_count, function: count, column: order_id}
        - {name: revenue,     function: sum,   column: amount}
  sink:
    name: customer_totals
    input: totals
"""


def test_yaml_parses_sources():
    spec = parse_yaml(BATCH_YAML)
    assert "orders" in spec.nodes
    assert spec.nodes["orders"].transform_type == TransformType.SOURCE
    schema = spec.nodes["orders"].output_schema
    assert schema is not None
    assert schema.get_field("order_id").nullable is False


def test_yaml_parses_filter():
    spec = parse_yaml(BATCH_YAML)
    node = spec.nodes["active"]
    assert node.transform_type == TransformType.FILTER
    assert node.predicate == "status == 'active'"
    assert node.inputs == ["orders"]


def test_yaml_parses_aggregate():
    spec = parse_yaml(BATCH_YAML)
    node = spec.nodes["totals"]
    assert node.transform_type == TransformType.AGGREGATE
    assert node.group_by == ["customer_id"]
    assert len(node.aggregations) == 2
    agg_names = {a.output_name for a in node.aggregations}
    assert agg_names == {"order_count", "revenue"}


def test_yaml_parses_sink():
    spec = parse_yaml(BATCH_YAML)
    assert "customer_totals" in spec.nodes
    assert spec.nodes["customer_totals"].is_sink()
    assert spec.nodes["customer_totals"].inputs == ["totals"]


def test_topological_order():
    spec = parse_yaml(BATCH_YAML)
    order = [n.name for n in spec.topological_order()]
    assert order.index("orders") < order.index("active")
    assert order.index("active") < order.index("totals")
    assert order.index("totals") < order.index("customer_totals")


def test_schema_inference_filter():
    spec = parse_yaml(BATCH_YAML)
    # filter should inherit schema from source
    filter_node = spec.nodes["active"]
    assert filter_node.output_schema is not None
    assert filter_node.output_schema.get_field("amount") is not None


def test_schema_inference_aggregate():
    spec = parse_yaml(BATCH_YAML)
    agg_node = spec.nodes["totals"]
    assert agg_node.output_schema is not None
    field_names = agg_node.output_schema.field_names()
    assert "customer_id" in field_names
    assert "order_count" in field_names
    assert "revenue" in field_names


def test_cycle_detection():
    cyclic = """
pipeline:
  name: cyclic
  sources:
    - name: s
      schema:
        - {name: id, type: string}
  transforms:
    - name: a
      type: filter
      inputs: [b]
      predicate: "id != ''"
    - name: b
      type: filter
      inputs: [a]
      predicate: "id != ''"
  sink:
    name: out
    input: b
"""
    with pytest.raises(ValueError, match="cycle"):
        parse_yaml(cyclic)


def test_sla_parsing():
    spec = parse_yaml(BATCH_YAML)
    assert spec.sla.latency_seconds() == 6 * 3600
    assert abs(spec.sla.dataset_size_gb() - 80.0) < 0.1


# ── Decorator DSL tests ──────────────────────────────────────────

def test_decorator_pipeline_builds_spec():
    @pipeline(name="deco_test", sla=SLA(max_latency="1h", dataset_size="10gb"))
    class MyPipeline:
        @source(schema=Schema([
            field("id", "string", nullable=False),
            field("val", "double"),
        ]))
        def raw(self): ...

        @transform(inputs=["raw"])
        def filtered(self, raw):
            return raw.filter("val > 0")

        @sink(input="filtered")
        def output(self, filtered): ...

    spec = extract_spec(MyPipeline)
    assert spec.name == "deco_test"
    assert "raw" in spec.nodes
    assert "filtered" in spec.nodes
    assert "output" in spec.nodes
    assert spec.nodes["filtered"].transform_type == TransformType.FILTER
    assert spec.nodes["filtered"].predicate == "val > 0"


def test_decorator_join():
    @pipeline(name="join_test", sla=SLA())
    class JoinPipe:
        @source(schema=Schema([field("id", "string"), field("v", "double")]))
        def left(self): ...

        @source(schema=Schema([field("id", "string"), field("label", "string")]))
        def right(self): ...

        @transform(inputs=["left", "right"])
        def joined(self, left, right):
            return left.join(right, on="id", how="inner")

        @sink(input="joined")
        def out(self, joined): ...

    spec = extract_spec(JoinPipe)
    join_node = spec.nodes["joined"]
    assert join_node.transform_type == TransformType.JOIN
    assert join_node.join_type == JoinType.INNER


def test_decorator_aggregate():
    @pipeline(name="agg_test", sla=SLA())
    class AggPipe:
        @source(schema=Schema([
            field("cat", "string"),
            field("amount", "double"),
        ]))
        def data(self): ...

        @transform(inputs=["data"])
        def agged(self, data):
            return data.groupby(["cat"]).agg(total=("amount", "sum"), cnt=("amount", "count"))

        @sink(input="agged")
        def out(self, agged): ...

    spec = extract_spec(AggPipe)
    agg_node = spec.nodes["agged"]
    assert agg_node.transform_type == TransformType.AGGREGATE
    assert agg_node.group_by == ["cat"]
    names = {a.output_name for a in agg_node.aggregations}
    assert names == {"total", "cnt"}
