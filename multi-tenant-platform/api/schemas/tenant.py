from uuid import UUID
from pydantic import BaseModel, field_validator
import re


class TenantCreate(BaseModel):
    name: str
    slug: str
    tier: str = "free"

    @field_validator("slug")
    @classmethod
    def slug_format(cls, v: str) -> str:
        if not re.match(r"^[a-z0-9-]{3,63}$", v):
            raise ValueError("Slug must be 3-63 lowercase alphanumeric chars or hyphens")
        return v

    @field_validator("tier")
    @classmethod
    def valid_tier(cls, v: str) -> str:
        if v not in {"free", "starter", "pro", "enterprise"}:
            raise ValueError(f"Invalid tier: {v}")
        return v


class TenantResponse(BaseModel):
    id: UUID
    name: str
    slug: str
    tier: str
    is_active: bool
    storage_bytes_used: int

    model_config = {"from_attributes": True}


class ApiKeyCreate(BaseModel):
    name: str


class ApiKeyResponse(BaseModel):
    id: UUID
    name: str
    raw_key: str | None = None  # only returned on creation
    is_active: bool

    model_config = {"from_attributes": True}
