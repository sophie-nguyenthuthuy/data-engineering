import uuid
from sqlalchemy import String, Enum, Boolean, BigInteger
from sqlalchemy.orm import Mapped, mapped_column
from sqlalchemy.dialects.postgresql import UUID

from db.base import Base, TimestampMixin


class Tenant(Base, TimestampMixin):
    __tablename__ = "tenants"
    __table_args__ = {"schema": "platform"}

    id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), primary_key=True, default=uuid.uuid4
    )
    name: Mapped[str] = mapped_column(String(63), unique=True, nullable=False)
    slug: Mapped[str] = mapped_column(String(63), unique=True, nullable=False)
    tier: Mapped[str] = mapped_column(
        Enum("free", "starter", "pro", "enterprise", name="tenant_tier"),
        nullable=False,
        default="free",
    )
    is_active: Mapped[bool] = mapped_column(Boolean, default=True, nullable=False)
    storage_bytes_used: Mapped[int] = mapped_column(BigInteger, default=0, nullable=False)


class TenantUser(Base, TimestampMixin):
    __tablename__ = "tenant_users"
    __table_args__ = {"schema": "platform"}

    id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), primary_key=True, default=uuid.uuid4
    )
    tenant_id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), nullable=False, index=True)
    user_id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), nullable=False, index=True)
    role: Mapped[str] = mapped_column(
        Enum("owner", "admin", "member", "viewer", name="user_role"),
        nullable=False,
        default="member",
    )


class ApiKey(Base, TimestampMixin):
    __tablename__ = "api_keys"
    __table_args__ = {"schema": "platform"}

    id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), primary_key=True, default=uuid.uuid4
    )
    tenant_id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), nullable=False, index=True)
    name: Mapped[str] = mapped_column(String(128), nullable=False)
    key_hash: Mapped[str] = mapped_column(String(64), unique=True, nullable=False)
    is_active: Mapped[bool] = mapped_column(Boolean, default=True, nullable=False)
    last_used_at: Mapped[str | None] = mapped_column(nullable=True)
