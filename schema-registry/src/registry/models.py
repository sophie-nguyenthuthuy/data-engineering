from __future__ import annotations

import hashlib
import json
from datetime import datetime
from enum import Enum
from typing import Any

from pydantic import BaseModel, Field, model_validator


class CompatibilityMode(str, Enum):
    NONE = "NONE"
    BACKWARD = "BACKWARD"
    FORWARD = "FORWARD"
    FULL = "FULL"
    BACKWARD_TRANSITIVE = "BACKWARD_TRANSITIVE"
    FORWARD_TRANSITIVE = "FORWARD_TRANSITIVE"
    FULL_TRANSITIVE = "FULL_TRANSITIVE"


class SchemaType(str, Enum):
    JSON = "JSON"
    AVRO = "AVRO"


class CompatibilityError(BaseModel):
    type: str
    path: str
    message: str
    breaking: bool = True


class CompatibilityResult(BaseModel):
    compatible: bool
    mode: CompatibilityMode
    errors: list[CompatibilityError] = Field(default_factory=list)
    warnings: list[str] = Field(default_factory=list)


class SchemaVersion(BaseModel):
    id: int | None = None
    subject: str
    version: int
    schema_type: SchemaType = SchemaType.JSON
    schema_definition: dict[str, Any]
    schema_hash: str = ""
    created_at: datetime = Field(default_factory=datetime.utcnow)
    metadata: dict[str, Any] = Field(default_factory=dict)

    @model_validator(mode="after")
    def compute_hash(self) -> "SchemaVersion":
        if not self.schema_hash:
            canonical = json.dumps(self.schema_definition, sort_keys=True)
            self.schema_hash = hashlib.sha256(canonical.encode()).hexdigest()[:16]
        return self


class SubjectConfig(BaseModel):
    subject: str
    compatibility: CompatibilityMode = CompatibilityMode.BACKWARD
    normalize: bool = False


class MigrationStep(BaseModel):
    op: str
    path: str
    params: dict[str, Any] = Field(default_factory=dict)


class MigrationScript(BaseModel):
    id: int | None = None
    subject: str
    from_version: int
    to_version: int
    steps: list[MigrationStep]
    dsl_source: str = ""
    auto_generated: bool = True
    created_at: datetime = Field(default_factory=datetime.utcnow)
    breaking_changes: list[str] = Field(default_factory=list)


class TransformEvent(BaseModel):
    event_id: str
    subject: str
    schema_version: int
    payload: dict[str, Any]
    timestamp: datetime = Field(default_factory=datetime.utcnow)


class ReplayResult(BaseModel):
    total: int
    succeeded: int
    failed: int
    events: list[dict[str, Any]] = Field(default_factory=list)
    errors: list[dict[str, Any]] = Field(default_factory=list)
