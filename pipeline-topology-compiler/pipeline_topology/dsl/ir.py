from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Optional

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


@dataclass
class TransformNode:
    """A single node in the pipeline DAG."""

    name: str
    transform_type: TransformType
    inputs: list[str] = field(default_factory=list)
    output_schema: Optional[Schema] = None

    # TYPE-SPECIFIC PARAMS
    # filter
    predicate: Optional[str] = None
    # select
    columns: Optional[list[str]] = None
    # join
    join_type: JoinType = JoinType.INNER
    join_on: Optional[str] = None
    join_left_key: Optional[str] = None
    join_right_key: Optional[str] = None
    # aggregate
    group_by: list[str] = field(default_factory=list)
    aggregations: list[Aggregation] = field(default_factory=list)
    # map
    expressions: dict[str, str] = field(default_factory=dict)
    # window
    window_column: Optional[str] = None
    window_duration: Optional[str] = None
    slide_duration: Optional[str] = None
    # source/sink
    location: Optional[str] = None
    format: Optional[str] = None
    options: dict[str, Any] = field(default_factory=dict)

    def is_source(self) -> bool:
        return self.transform_type == TransformType.SOURCE

    def is_sink(self) -> bool:
        return self.transform_type == TransformType.SINK

    def validate(self) -> None:
        if self.transform_type == TransformType.FILTER and not self.predicate:
            raise ValueError(f"Filter node '{self.name}' must have a predicate")
        if self.transform_type == TransformType.JOIN and len(self.inputs) != 2:
            raise ValueError(f"Join node '{self.name}' must have exactly 2 inputs, got {len(self.inputs)}")
        if self.transform_type == TransformType.AGGREGATE and not self.aggregations:
            raise ValueError(f"Aggregate node '{self.name}' must have at least one aggregation")
        if not self.is_source() and not self.inputs:
            raise ValueError(f"Non-source node '{self.name}' must have at least one input")

    def canonical_key(self) -> str:
        """Stable key for equivalence comparison, independent of variable naming."""
        parts = [self.transform_type.value]
        if self.predicate:
            parts.append(f"pred:{self.predicate}")
        if self.columns:
            parts.append(f"cols:{sorted(self.columns)}")
        if self.group_by:
            parts.append(f"groupby:{sorted(self.group_by)}")
        for agg in sorted(self.aggregations, key=lambda a: a.output_name):
            parts.append(f"agg:{agg.function.value}({agg.column})->{agg.output_name}")
        if self.join_on:
            parts.append(f"join:{self.join_type.value}:{self.join_on}")
        return "|".join(parts)

    def __repr__(self) -> str:
        return f"Node({self.name}, {self.transform_type.value}, inputs={self.inputs})"


@dataclass
class PipelineSpec:
    """The logical pipeline specification — the canonical IR."""

    name: str
    description: str = ""
    sla: SLA = field(default_factory=SLA)
    nodes: dict[str, TransformNode] = field(default_factory=dict)

    def add_node(self, node: TransformNode) -> None:
        if node.name in self.nodes:
            raise ValueError(f"Duplicate node name: {node.name!r}")
        self.nodes[node.name] = node

    def sources(self) -> list[TransformNode]:
        return [n for n in self.nodes.values() if n.is_source()]

    def sinks(self) -> list[TransformNode]:
        return [n for n in self.nodes.values() if n.is_sink()]

    def topological_order(self) -> list[TransformNode]:
        """Kahn's algorithm — raises on cycles."""
        in_degree: dict[str, int] = {name: 0 for name in self.nodes}
        dependents: dict[str, list[str]] = {name: [] for name in self.nodes}

        for name, node in self.nodes.items():
            for inp in node.inputs:
                if inp not in self.nodes:
                    raise ValueError(f"Node '{name}' references unknown input '{inp}'")
                in_degree[name] += 1
                dependents[inp].append(name)

        queue = [name for name, deg in in_degree.items() if deg == 0]
        order: list[TransformNode] = []

        while queue:
            name = queue.pop(0)
            order.append(self.nodes[name])
            for dep in dependents[name]:
                in_degree[dep] -= 1
                if in_degree[dep] == 0:
                    queue.append(dep)

        if len(order) != len(self.nodes):
            raise ValueError("Pipeline DAG contains a cycle")

        return order

    def validate(self) -> None:
        for node in self.nodes.values():
            node.validate()
        self.topological_order()

    def infer_schemas(self) -> None:
        """Best-effort schema inference for nodes that don't declare output_schema."""
        for node in self.topological_order():
            if node.output_schema is not None:
                continue
            if node.is_source():
                continue  # sources must declare their schema

            input_schemas = []
            for inp_name in node.inputs:
                inp = self.nodes[inp_name]
                if inp.output_schema:
                    input_schemas.append(inp.output_schema)

            if not input_schemas:
                continue

            primary = input_schemas[0]

            if node.transform_type == TransformType.FILTER:
                node.output_schema = primary

            elif node.transform_type == TransformType.SELECT and node.columns:
                try:
                    node.output_schema = primary.project(node.columns)
                except ValueError:
                    pass

            elif node.transform_type == TransformType.AGGREGATE:
                agg_fields = [
                    FieldSchema(col, FieldType.LONG, False)
                    for col in node.group_by
                    if primary.get_field(col)
                ]
                for agg in node.aggregations:
                    base_type = FieldType.LONG
                    if agg.column and primary.get_field(agg.column):
                        base = primary.get_field(agg.column)
                        if agg.function in (AggFunction.SUM, AggFunction.AVG):
                            base_type = base.dtype if base.dtype.is_numeric() else FieldType.DOUBLE
                        elif agg.function == AggFunction.COUNT:
                            base_type = FieldType.LONG
                        else:
                            base_type = base.dtype
                    nullable = agg.function not in (AggFunction.COUNT,)
                    agg_fields.append(FieldSchema(agg.output_name, base_type, nullable))
                node.output_schema = Schema(agg_fields)

            elif node.transform_type == TransformType.JOIN and len(input_schemas) == 2:
                node.output_schema = input_schemas[0].merge(input_schemas[1])

            elif node.transform_type == TransformType.UNION:
                node.output_schema = primary

            elif node.transform_type == TransformType.SINK:
                node.output_schema = primary

    def __repr__(self) -> str:
        return f"PipelineSpec(name={self.name!r}, nodes={list(self.nodes.keys())})"
