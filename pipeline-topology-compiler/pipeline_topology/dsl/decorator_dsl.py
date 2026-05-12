"""Python decorator-based DSL for declaring pipelines."""
from __future__ import annotations

import functools
import inspect
from typing import Any, Callable, Optional, Type

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


def field(name: str, dtype: str, nullable: bool = True) -> FieldSchema:
    return FieldSchema(name=name, dtype=FieldType.from_str(dtype), nullable=nullable)


# ──────────────────────────────────────────────────────────────
# Fluent transform builder — returned by decorator-DSL methods
# ──────────────────────────────────────────────────────────────

class _TransformBuilder:
    """Fluent builder that records operations on 'virtual' DataFrames."""

    def __init__(self, name: str, inputs: list[str], ttype: TransformType) -> None:
        self._name = name
        self._inputs = inputs
        self._ttype = ttype
        self._predicate: Optional[str] = None
        self._columns: Optional[list[str]] = None
        self._join_type: JoinType = JoinType.INNER
        self._join_on: Optional[str] = None
        self._group_by: list[str] = []
        self._aggregations: list[Aggregation] = []
        self._expressions: dict[str, str] = {}
        self._window_col: Optional[str] = None
        self._window_dur: Optional[str] = None
        self._slide_dur: Optional[str] = None

    def filter(self, predicate: str) -> "_TransformBuilder":
        self._ttype = TransformType.FILTER
        self._predicate = predicate
        return self

    def select(self, *columns: str) -> "_TransformBuilder":
        self._ttype = TransformType.SELECT
        self._columns = list(columns)
        return self

    def join(self, other: "_TransformBuilder", on: str, how: str = "inner") -> "_TransformBuilder":
        self._ttype = TransformType.JOIN
        self._inputs = [self._inputs[0], other._name]
        self._join_on = on
        self._join_type = JoinType(how.lower())
        return self

    def groupby(self, keys: list[str]) -> "_AggBuilder":
        self._group_by = keys
        return _AggBuilder(self)

    def window(self, time_column: str, duration: str, slide: Optional[str] = None) -> "_WindowBuilder":
        self._ttype = TransformType.WINDOW
        self._window_col = time_column
        self._window_dur = duration
        self._slide_dur = slide
        return _WindowBuilder(self)

    def add_column(self, name: str, expr: str) -> "_TransformBuilder":
        self._ttype = TransformType.MAP
        self._expressions[name] = expr
        return self

    def _build_node(self) -> TransformNode:
        return TransformNode(
            name=self._name,
            transform_type=self._ttype,
            inputs=self._inputs,
            predicate=self._predicate,
            columns=self._columns,
            join_type=self._join_type,
            join_on=self._join_on,
            group_by=self._group_by,
            aggregations=self._aggregations,
            expressions=self._expressions,
            window_column=self._window_col,
            window_duration=self._window_dur,
            slide_duration=self._slide_dur,
        )


class _AggBuilder:
    def __init__(self, parent: _TransformBuilder) -> None:
        self._parent = parent

    def agg(self, **kwargs: tuple[str, str]) -> _TransformBuilder:
        """Usage: .agg(total=("amount", "sum"), cnt=("id", "count"))"""
        self._parent._ttype = TransformType.AGGREGATE
        for out_name, (col, func) in kwargs.items():
            self._parent._aggregations.append(
                Aggregation(output_name=out_name, function=AggFunction(func.lower()), column=col)
            )
        return self._parent


class _WindowBuilder:
    def __init__(self, parent: _TransformBuilder) -> None:
        self._parent = parent

    def agg(self, **kwargs: tuple[str, str]) -> _TransformBuilder:
        for out_name, (col, func) in kwargs.items():
            self._parent._aggregations.append(
                Aggregation(output_name=out_name, function=AggFunction(func.lower()), column=col)
            )
        return self._parent


# ──────────────────────────────────────────────────────────────
# Class-level decorators
# ──────────────────────────────────────────────────────────────

def source(schema: Schema, location: Optional[str] = None, format: Optional[str] = None, **options: Any):
    """Mark a method as a pipeline source."""
    def decorator(fn: Callable) -> Callable:
        fn._ptc_type = "source"
        fn._ptc_schema = schema
        fn._ptc_location = location
        fn._ptc_format = format
        fn._ptc_options = options
        return fn
    return decorator


def transform(inputs: list[str]):
    """Mark a method as a pipeline transform. The method body builds the transformation."""
    def decorator(fn: Callable) -> Callable:
        fn._ptc_type = "transform"
        fn._ptc_inputs = inputs
        return fn
    return decorator


def sink(input: str, location: Optional[str] = None, format: Optional[str] = None, **options: Any):
    """Mark a method as a pipeline sink."""
    def decorator(fn: Callable) -> Callable:
        fn._ptc_type = "sink"
        fn._ptc_input = input
        fn._ptc_location = location
        fn._ptc_format = format
        fn._ptc_options = options
        return fn
    return decorator


def pipeline(name: Optional[str] = None, sla: Optional[SLA] = None, description: str = ""):
    """Class decorator that compiles the decorated class into a PipelineSpec."""
    def decorator(cls: Type) -> Type:
        pipeline_name = name or cls.__name__
        spec = PipelineSpec(
            name=pipeline_name,
            description=description or (cls.__doc__ or "").strip(),
            sla=sla or SLA(),
        )

        members = list(inspect.getmembers(cls, predicate=inspect.isfunction))
        for method_name, method in members:
            ptc_type = getattr(method, "_ptc_type", None)
            if ptc_type is None:
                continue

            if ptc_type == "source":
                node = TransformNode(
                    name=method_name,
                    transform_type=TransformType.SOURCE,
                    output_schema=method._ptc_schema,
                    location=method._ptc_location,
                    format=method._ptc_format,
                    options=method._ptc_options,
                )
                spec.add_node(node)

            elif ptc_type == "sink":
                node = TransformNode(
                    name=method_name,
                    transform_type=TransformType.SINK,
                    inputs=[method._ptc_input],
                    location=method._ptc_location,
                    format=method._ptc_format,
                    options=method._ptc_options,
                )
                spec.add_node(node)

            elif ptc_type == "transform":
                input_names: list[str] = method._ptc_inputs
                builders = {inp: _TransformBuilder(inp, [inp], TransformType.FILTER) for inp in input_names}
                result: Optional[_TransformBuilder] = method(None, **builders)

                if result is not None:
                    compiled = result._build_node()
                    node = TransformNode(
                        name=method_name,
                        transform_type=compiled.transform_type,
                        inputs=compiled.inputs if compiled.inputs != [method_name] else input_names,
                        predicate=compiled.predicate,
                        columns=compiled.columns,
                        join_type=compiled.join_type,
                        join_on=compiled.join_on,
                        group_by=compiled.group_by,
                        aggregations=compiled.aggregations,
                        expressions=compiled.expressions,
                        window_column=compiled.window_column,
                        window_duration=compiled.window_duration,
                        slide_duration=compiled.slide_duration,
                    )
                    if not node.inputs:
                        node.inputs = input_names
                else:
                    node = TransformNode(
                        name=method_name,
                        transform_type=TransformType.MAP,
                        inputs=input_names,
                    )
                spec.add_node(node)

        spec.infer_schemas()
        spec.validate()
        cls._ptc_spec = spec
        return cls

    return decorator


def extract_spec(cls: Type) -> PipelineSpec:
    """Extract the compiled PipelineSpec from a @pipeline-decorated class."""
    if not hasattr(cls, "_ptc_spec"):
        raise TypeError(f"{cls.__name__} is not decorated with @pipeline")
    return cls._ptc_spec
