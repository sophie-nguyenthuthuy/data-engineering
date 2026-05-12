"""
Tenant data models — stored in per-tenant PostgreSQL schemas.
RLS policies on these tables enforce that rows are only visible when
current_setting('app.tenant_id') matches the row's tenant_id column.
"""
import uuid
from sqlalchemy import String, Text, BigInteger, JSON, ForeignKey
from sqlalchemy.orm import Mapped, mapped_column
from sqlalchemy.dialects.postgresql import UUID, JSONB

from db.base import Base, TimestampMixin


class Dataset(Base, TimestampMixin):
    """A logical dataset owned by a tenant."""
    __tablename__ = "datasets"
    __table_args__ = {"schema": "platform"}

    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    tenant_id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), nullable=False, index=True)
    name: Mapped[str] = mapped_column(String(255), nullable=False)
    description: Mapped[str | None] = mapped_column(Text, nullable=True)
    schema_definition: Mapped[dict] = mapped_column(JSONB, nullable=False, default=dict)
    row_count: Mapped[int] = mapped_column(BigInteger, default=0, nullable=False)
    size_bytes: Mapped[int] = mapped_column(BigInteger, default=0, nullable=False)
    is_public: Mapped[bool] = mapped_column(default=False, nullable=False)


class DataRecord(Base, TimestampMixin):
    """A single row within a dataset — RLS-protected."""
    __tablename__ = "data_records"
    __table_args__ = {"schema": "platform"}

    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    tenant_id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), nullable=False, index=True)
    dataset_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True),
        ForeignKey("platform.datasets.id", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )
    data: Mapped[dict] = mapped_column(JSONB, nullable=False)


class AuditLog(Base):
    """Immutable audit log — INSERT only, never UPDATE or DELETE."""
    __tablename__ = "audit_logs"
    __table_args__ = {"schema": "platform"}

    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    tenant_id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), nullable=False, index=True)
    actor_id: Mapped[str] = mapped_column(String(255), nullable=False)
    action: Mapped[str] = mapped_column(String(100), nullable=False)
    resource_type: Mapped[str] = mapped_column(String(100), nullable=False)
    resource_id: Mapped[str | None] = mapped_column(String(255), nullable=True)
    metadata: Mapped[dict] = mapped_column(JSONB, nullable=False, default=dict)
    occurred_at: Mapped[str] = mapped_column(nullable=False, server_default="now()")
