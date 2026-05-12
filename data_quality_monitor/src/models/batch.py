from __future__ import annotations
import uuid
from datetime import datetime
from typing import Any
import pandas as pd
from pydantic import BaseModel, Field


class BatchMetadata(BaseModel):
    source: str
    table_name: str
    partition_key: str | None = None
    schema_version: str = "1.0"
    tags: dict[str, str] = Field(default_factory=dict)


class MicroBatch(BaseModel):
    model_config = {"arbitrary_types_allowed": True}

    batch_id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    received_at: datetime = Field(default_factory=datetime.utcnow)
    metadata: BatchMetadata
    # Serialised as list[dict] over the wire; reconstructed to DataFrame in-process
    records: list[dict[str, Any]] = Field(default_factory=list)

    @classmethod
    def from_dataframe(cls, df: pd.DataFrame, metadata: BatchMetadata) -> "MicroBatch":
        return cls(metadata=metadata, records=df.to_dict(orient="records"))

    def to_dataframe(self) -> pd.DataFrame:
        return pd.DataFrame(self.records)

    @property
    def row_count(self) -> int:
        return len(self.records)
