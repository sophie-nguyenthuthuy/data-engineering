from __future__ import annotations

from typing import Any

from pydantic import BaseModel, Field

from src.registry.models import CompatibilityMode, SchemaType


class RegisterSchemaRequest(BaseModel):
    schema_definition: dict[str, Any] = Field(..., description="JSON Schema definition")
    schema_type: SchemaType = SchemaType.JSON
    metadata: dict[str, Any] = Field(default_factory=dict)


class CheckCompatibilityRequest(BaseModel):
    schema_definition: dict[str, Any]
    mode: CompatibilityMode | None = None


class SetConfigRequest(BaseModel):
    compatibility: CompatibilityMode


class DSLMigrationRequest(BaseModel):
    dsl_source: str = Field(..., description="YAML or JSON DSL migration definition")


class ReplayRequest(BaseModel):
    events: list[dict[str, Any]] = Field(..., description="List of {event_id, schema_version, payload}")
    target_version: int
    validate: bool = True


class MigratePayloadRequest(BaseModel):
    payload: dict[str, Any]
    from_version: int
    to_version: int
