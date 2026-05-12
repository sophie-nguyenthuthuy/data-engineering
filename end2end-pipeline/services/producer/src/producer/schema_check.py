"""Pre-flight compatibility check against the Schema Registry.

Run at producer startup. Fails fast if the local Avro schema would break
existing consumers under the subject's configured compatibility level.
"""

from __future__ import annotations

from confluent_kafka.schema_registry import Schema, SchemaRegistryClient
from confluent_kafka.schema_registry.error import SchemaRegistryError


class SchemaIncompatibleError(RuntimeError):
    """Raised when the local schema is not compatible with the latest registered schema."""


def subject_for(topic: str) -> str:
    """TopicNameStrategy (the Confluent default): ``<topic>-value``."""
    return f"{topic}-value"


def ensure_compatible(sr: SchemaRegistryClient, subject: str, schema_str: str) -> None:
    """Raise ``SchemaIncompatibleError`` if the local schema is incompatible.

    - If the subject doesn't exist yet, this returns silently — the next
      produce will register the schema.
    - If ``test_compatibility`` returns ``False``, we raise.
    - Network / auth / other SR errors propagate unchanged.
    """
    schema = Schema(schema_str, "AVRO")
    try:
        compatible = sr.test_compatibility(subject, schema)
    except SchemaRegistryError as exc:
        # 40401 = subject not found. Treat as "first registration".
        http_404 = getattr(exc, "http_status_code", None) == 404
        err_40401 = getattr(exc, "error_code", None) == 40401
        if http_404 or err_40401:
            return
        raise
    if not compatible:
        raise SchemaIncompatibleError(
            f"local schema is not compatible with the latest schema registered under "
            f"subject '{subject}'. Either update consumers first, or evolve the schema "
            f"in a backward-compatible way (add fields with defaults; do not rename/remove)."
        )
