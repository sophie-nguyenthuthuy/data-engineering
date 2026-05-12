from datetime import datetime, timezone
from sqlalchemy import (
    Column, Integer, String, Text, Boolean, DateTime,
    ForeignKey, JSON, UniqueConstraint
)
from sqlalchemy.orm import relationship
from catalog.database import Base


def now_utc():
    return datetime.now(timezone.utc)


class DataSource(Base):
    __tablename__ = "data_sources"

    id = Column(Integer, primary_key=True, index=True)
    name = Column(String(255), unique=True, nullable=False)
    engine_type = Column(String(50), nullable=False)  # sqlite, postgresql, mysql, csv
    connection_string = Column(Text, nullable=False)
    description = Column(Text)
    created_at = Column(DateTime(timezone=True), default=now_utc)
    last_scanned_at = Column(DateTime(timezone=True))

    schemas = relationship("SchemaNode", back_populates="source", cascade="all, delete-orphan")


class SchemaNode(Base):
    __tablename__ = "schemas"

    id = Column(Integer, primary_key=True, index=True)
    source_id = Column(Integer, ForeignKey("data_sources.id"), nullable=False)
    name = Column(String(255), nullable=False)
    description = Column(Text)
    created_at = Column(DateTime(timezone=True), default=now_utc)

    __table_args__ = (UniqueConstraint("source_id", "name"),)

    source = relationship("DataSource", back_populates="schemas")
    tables = relationship("TableNode", back_populates="schema", cascade="all, delete-orphan")


class TableNode(Base):
    __tablename__ = "tables"

    id = Column(Integer, primary_key=True, index=True)
    schema_id = Column(Integer, ForeignKey("schemas.id"), nullable=False)
    name = Column(String(255), nullable=False)
    description = Column(Text)
    row_count = Column(Integer)
    tags = Column(JSON, default=list)
    created_at = Column(DateTime(timezone=True), default=now_utc)

    __table_args__ = (UniqueConstraint("schema_id", "name"),)

    schema = relationship("SchemaNode", back_populates="tables")
    columns = relationship("ColumnNode", back_populates="table", cascade="all, delete-orphan")


class ColumnNode(Base):
    __tablename__ = "columns"

    id = Column(Integer, primary_key=True, index=True)
    table_id = Column(Integer, ForeignKey("tables.id"), nullable=False)
    name = Column(String(255), nullable=False)
    data_type = Column(String(100))
    is_nullable = Column(Boolean, default=True)
    is_primary_key = Column(Boolean, default=False)
    description = Column(Text)
    pii_tags = Column(JSON, default=list)   # ["EMAIL", "PII"]
    sample_values = Column(JSON, default=list)
    created_at = Column(DateTime(timezone=True), default=now_utc)

    __table_args__ = (UniqueConstraint("table_id", "name"),)

    table = relationship("TableNode", back_populates="columns")

    # lineage edges where this column is the source
    outgoing_lineage = relationship(
        "ColumnLineage", foreign_keys="ColumnLineage.source_column_id",
        back_populates="source_column", cascade="all, delete-orphan"
    )
    # lineage edges where this column is the target
    incoming_lineage = relationship(
        "ColumnLineage", foreign_keys="ColumnLineage.target_column_id",
        back_populates="target_column", cascade="all, delete-orphan"
    )


class LineageJob(Base):
    __tablename__ = "lineage_jobs"

    id = Column(Integer, primary_key=True, index=True)
    name = Column(String(255), unique=True, nullable=False)
    description = Column(Text)
    sql_query = Column(Text)
    job_type = Column(String(50), default="sql")  # sql, dbt, spark, manual
    tags = Column(JSON, default=list)
    created_at = Column(DateTime(timezone=True), default=now_utc)
    updated_at = Column(DateTime(timezone=True), default=now_utc, onupdate=now_utc)

    edges = relationship("ColumnLineage", back_populates="job", cascade="all, delete-orphan")


class ColumnLineage(Base):
    __tablename__ = "column_lineage"

    id = Column(Integer, primary_key=True, index=True)
    job_id = Column(Integer, ForeignKey("lineage_jobs.id"), nullable=False)
    source_column_id = Column(Integer, ForeignKey("columns.id"), nullable=False)
    target_column_id = Column(Integer, ForeignKey("columns.id"), nullable=False)
    transformation_logic = Column(Text)
    created_at = Column(DateTime(timezone=True), default=now_utc)

    __table_args__ = (UniqueConstraint("job_id", "source_column_id", "target_column_id"),)

    job = relationship("LineageJob", back_populates="edges")
    source_column = relationship("ColumnNode", foreign_keys=[source_column_id], back_populates="outgoing_lineage")
    target_column = relationship("ColumnNode", foreign_keys=[target_column_id], back_populates="incoming_lineage")
