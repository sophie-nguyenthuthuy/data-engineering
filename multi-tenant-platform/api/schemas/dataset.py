from uuid import UUID
from typing import Any
from pydantic import BaseModel


class DatasetCreate(BaseModel):
    name: str
    description: str | None = None
    schema_definition: dict[str, Any] = {}
    is_public: bool = False


class DatasetUpdate(BaseModel):
    name: str | None = None
    description: str | None = None
    is_public: bool | None = None


class DatasetResponse(BaseModel):
    id: UUID
    tenant_id: UUID
    name: str
    description: str | None
    schema_definition: dict[str, Any]
    row_count: int
    size_bytes: int
    is_public: bool

    model_config = {"from_attributes": True}


class RecordCreate(BaseModel):
    data: dict[str, Any]


class RecordResponse(BaseModel):
    id: UUID
    tenant_id: UUID
    dataset_id: UUID
    data: dict[str, Any]

    model_config = {"from_attributes": True}


class BulkRecordCreate(BaseModel):
    records: list[dict[str, Any]]

    def __len__(self) -> int:
        return len(self.records)
