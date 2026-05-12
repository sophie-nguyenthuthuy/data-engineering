"""Parse a YAML pipeline definition into a PipelineSpec IR."""
from __future__ import annotations

from pathlib import Path
from typing import Any

import yaml

from .ir import PipelineSpec, TransformNode
from .types import (
    Aggregation,
    AggFunction,
    FieldSchema,
    FieldType,
    JoinType,
    Schema,
    SLA,
    TransformType,
)


def _parse_schema(raw: list[dict]) -> Schema:
    fields = []
    for entry in raw:
        fields.append(
            FieldSchema(
                name=entry["name"],
                dtype=FieldType.from_str(entry["type"]),
                nullable=entry.get("nullable", True),
            )
        )
    return Schema(fields)


def _parse_sla(raw: dict) -> SLA:
    return SLA(
        max_latency=str(raw.get("max_latency", "24h")),
        dataset_size=str(raw.get("dataset_size", "10gb")),
    )


def _parse_source(name: str, raw: dict) -> TransformNode:
    schema = _parse_schema(raw["schema"]) if "schema" in raw else None
    return TransformNode(
        name=name,
        transform_type=TransformType.SOURCE,
        output_schema=schema,
        location=raw.get("location"),
        format=raw.get("format"),
        options=raw.get("options", {}),
    )


def _parse_transform(raw: dict) -> TransformNode:
    name = raw["name"]
    ttype = TransformType(raw["type"].lower())
    inputs = raw.get("inputs", [])
    if isinstance(inputs, str):
        inputs = [inputs]

    node = TransformNode(name=name, transform_type=ttype, inputs=inputs)

    if ttype == TransformType.FILTER:
        node.predicate = raw["predicate"]

    elif ttype == TransformType.SELECT:
        node.columns = raw["columns"]

    elif ttype == TransformType.MAP:
        node.expressions = raw.get("expressions", {})
        if "output_schema" in raw:
            node.output_schema = _parse_schema(raw["output_schema"])

    elif ttype == TransformType.JOIN:
        node.join_type = JoinType(raw.get("join_type", "inner").lower())
        node.join_on = raw.get("on")
        node.join_left_key = raw.get("left_key")
        node.join_right_key = raw.get("right_key")

    elif ttype == TransformType.AGGREGATE:
        node.group_by = raw.get("group_by", [])
        for agg_raw in raw.get("aggregations", []):
            node.aggregations.append(
                Aggregation(
                    output_name=agg_raw["name"],
                    function=AggFunction(agg_raw["function"].lower()),
                    column=agg_raw.get("column"),
                )
            )

    elif ttype == TransformType.UNION:
        pass  # inputs already set

    elif ttype == TransformType.WINDOW:
        node.window_column = raw.get("time_column")
        node.window_duration = raw.get("window_duration")
        node.slide_duration = raw.get("slide_duration")
        node.group_by = raw.get("group_by", [])
        for agg_raw in raw.get("aggregations", []):
            node.aggregations.append(
                Aggregation(
                    output_name=agg_raw["name"],
                    function=AggFunction(agg_raw["function"].lower()),
                    column=agg_raw.get("column"),
                )
            )

    if "output_schema" in raw and node.output_schema is None:
        node.output_schema = _parse_schema(raw["output_schema"])

    return node


def _parse_sink(raw: dict) -> TransformNode:
    name = raw["name"]
    inputs = raw.get("input", raw.get("inputs", []))
    if isinstance(inputs, str):
        inputs = [inputs]
    return TransformNode(
        name=name,
        transform_type=TransformType.SINK,
        inputs=inputs,
        location=raw.get("location"),
        format=raw.get("format"),
        options=raw.get("options", {}),
    )


def parse_yaml(source: str | Path) -> PipelineSpec:
    """Load a YAML file or string into a PipelineSpec."""
    if isinstance(source, Path) or (isinstance(source, str) and "\n" not in source and Path(source).exists()):
        text = Path(source).read_text()
    else:
        text = source

    raw: dict[str, Any] = yaml.safe_load(text)
    pipeline_raw = raw.get("pipeline", raw)

    spec = PipelineSpec(
        name=pipeline_raw["name"],
        description=pipeline_raw.get("description", ""),
        sla=_parse_sla(pipeline_raw.get("sla", {})),
    )

    for src_raw in pipeline_raw.get("sources", []):
        node = _parse_source(src_raw["name"], src_raw)
        spec.add_node(node)

    for t_raw in pipeline_raw.get("transforms", []):
        node = _parse_transform(t_raw)
        spec.add_node(node)

    sink_raw = pipeline_raw.get("sink")
    if sink_raw:
        if isinstance(sink_raw, list):
            for s in sink_raw:
                spec.add_node(_parse_sink(s))
        else:
            spec.add_node(_parse_sink(sink_raw))

    for s_raw in pipeline_raw.get("sinks", []):
        spec.add_node(_parse_sink(s_raw))

    spec.infer_schemas()
    spec.validate()
    return spec
