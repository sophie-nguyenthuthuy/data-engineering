"""cdc-debezium-postgres-kafka — type-safe CDC event toolkit."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

__version__ = "0.1.0"

if TYPE_CHECKING:
    from cdc.dlq.router import DLQDecision, DLQReason, DLQRouter
    from cdc.events.envelope import DebeziumEnvelope, Op, SourceInfo
    from cdc.events.parse import ParseError, parse_envelope
    from cdc.pipeline import Pipeline, PipelineResult
    from cdc.schema.avro import generate_avro_schema, postgres_to_avro
    from cdc.transforms.base import Transform
    from cdc.transforms.flatten import FlattenAfter
    from cdc.transforms.mask_pii import MaskPII
    from cdc.transforms.rename import RenameColumns


_LAZY: dict[str, tuple[str, str]] = {
    "Op": ("cdc.events.envelope", "Op"),
    "SourceInfo": ("cdc.events.envelope", "SourceInfo"),
    "DebeziumEnvelope": ("cdc.events.envelope", "DebeziumEnvelope"),
    "parse_envelope": ("cdc.events.parse", "parse_envelope"),
    "ParseError": ("cdc.events.parse", "ParseError"),
    "Transform": ("cdc.transforms.base", "Transform"),
    "FlattenAfter": ("cdc.transforms.flatten", "FlattenAfter"),
    "MaskPII": ("cdc.transforms.mask_pii", "MaskPII"),
    "RenameColumns": ("cdc.transforms.rename", "RenameColumns"),
    "DLQRouter": ("cdc.dlq.router", "DLQRouter"),
    "DLQReason": ("cdc.dlq.router", "DLQReason"),
    "DLQDecision": ("cdc.dlq.router", "DLQDecision"),
    "Pipeline": ("cdc.pipeline", "Pipeline"),
    "PipelineResult": ("cdc.pipeline", "PipelineResult"),
    "postgres_to_avro": ("cdc.schema.avro", "postgres_to_avro"),
    "generate_avro_schema": ("cdc.schema.avro", "generate_avro_schema"),
}


def __getattr__(name: str) -> Any:
    if name in _LAZY:
        from importlib import import_module

        m, attr = _LAZY[name]
        return getattr(import_module(m), attr)
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")


__all__ = [
    "DLQDecision",
    "DLQReason",
    "DLQRouter",
    "DebeziumEnvelope",
    "FlattenAfter",
    "MaskPII",
    "Op",
    "ParseError",
    "Pipeline",
    "PipelineResult",
    "RenameColumns",
    "SourceInfo",
    "Transform",
    "__version__",
    "generate_avro_schema",
    "parse_envelope",
    "postgres_to_avro",
]
