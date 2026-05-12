from __future__ import annotations
from typing import Any, Literal
from pydantic import BaseModel, Field


class SourceConfig(BaseModel):
    type: str
    query: str
    params: dict[str, Any] = Field(default_factory=dict)


class FieldMapping(BaseModel):
    source: str
    destination: str
    transform: str | None = None  # e.g. "upper", "lower", "str", "int"


class DestinationConfig(BaseModel):
    type: str
    params: dict[str, Any] = Field(default_factory=dict)
    field_mappings: list[FieldMapping] = Field(default_factory=list)


class ScheduleConfig(BaseModel):
    type: Literal["cron", "interval"] = "cron"
    cron: str | None = None          # e.g. "0 9 * * 1-5"
    seconds: int | None = None       # for interval type


class TriggerConfig(BaseModel):
    type: Literal["webhook"] = "webhook"
    path: str = "/trigger"


class PipelineConfig(BaseModel):
    name: str
    description: str = ""
    enabled: bool = True
    source: SourceConfig
    destination: DestinationConfig
    schedule: ScheduleConfig | None = None
    trigger: TriggerConfig | None = None


class RunResult(BaseModel):
    pipeline: str
    success: bool
    rows_extracted: int = 0
    rows_synced: int = 0
    error: str | None = None
