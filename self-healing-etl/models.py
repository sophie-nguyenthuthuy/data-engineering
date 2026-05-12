from __future__ import annotations

import json
from datetime import datetime
from sqlalchemy import (
    Column, Integer, String, Text, DateTime, Float, Boolean, JSON, create_engine
)
from sqlalchemy.orm import DeclarativeBase, Session


class Base(DeclarativeBase):
    pass


class SchemaVersion(Base):
    __tablename__ = "schema_versions"

    id = Column(Integer, primary_key=True, autoincrement=True)
    source_name = Column(String(256), nullable=False, index=True)
    version = Column(Integer, nullable=False)
    schema_json = Column(Text, nullable=False)   # JSON: {col: dtype_str}
    registered_at = Column(DateTime, default=datetime.utcnow)
    is_active = Column(Boolean, default=True)

    def get_schema(self) -> dict[str, str]:
        return json.loads(self.schema_json)


class QuarantineRecord(Base):
    __tablename__ = "quarantine_records"

    id = Column(Integer, primary_key=True, autoincrement=True)
    pipeline_name = Column(String(256), nullable=False, index=True)
    source_name = Column(String(256), nullable=False, index=True)
    run_id = Column(String(128), nullable=False, index=True)
    record_json = Column(Text, nullable=False)   # raw record
    error_type = Column(String(128), nullable=False, index=True)
    error_detail = Column(Text, nullable=False)
    root_cause_hint = Column(Text, nullable=True)
    quarantined_at = Column(DateTime, default=datetime.utcnow, index=True)
    schema_version = Column(Integer, nullable=True)
    resolved = Column(Boolean, default=False)


class DriftEvent(Base):
    __tablename__ = "drift_events"

    id = Column(Integer, primary_key=True, autoincrement=True)
    pipeline_name = Column(String(256), nullable=False, index=True)
    source_name = Column(String(256), nullable=False, index=True)
    run_id = Column(String(128), nullable=False)
    drift_type = Column(String(64), nullable=False)  # added_columns | removed_columns | type_changed
    details_json = Column(Text, nullable=False)
    detected_at = Column(DateTime, default=datetime.utcnow, index=True)
    healed = Column(Boolean, default=False)
    healing_action = Column(Text, nullable=True)


class PipelineRun(Base):
    __tablename__ = "pipeline_runs"

    id = Column(Integer, primary_key=True, autoincrement=True)
    run_id = Column(String(128), nullable=False, unique=True, index=True)
    pipeline_name = Column(String(256), nullable=False, index=True)
    source_name = Column(String(256), nullable=False)
    status = Column(String(32), nullable=False, default="RUNNING")
    rows_extracted = Column(Integer, default=0)
    rows_loaded = Column(Integer, default=0)
    rows_quarantined = Column(Integer, default=0)
    drift_detected = Column(Boolean, default=False)
    started_at = Column(DateTime, default=datetime.utcnow)
    finished_at = Column(DateTime, nullable=True)
    error_message = Column(Text, nullable=True)


def init_db(db_url: str) -> "Engine":
    from sqlalchemy import create_engine
    engine = create_engine(db_url, echo=False)
    Base.metadata.create_all(engine)
    return engine
