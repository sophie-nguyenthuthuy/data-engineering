from __future__ import annotations
from datetime import datetime, timedelta
from typing import Any

import structlog
from sqlalchemy import Column, DateTime, Float, Integer, String, Text, Boolean, select, func
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine, async_sessionmaker
from sqlalchemy.orm import DeclarativeBase

from ..config import settings
from ..models import DriftReport, SkewReport, RetrainingJob, PipelineRun, TrainingSnapshot

log = structlog.get_logger(__name__)


class Base(DeclarativeBase):
    pass


class DriftReportORM(Base):
    __tablename__ = "drift_reports"
    id = Column(String, primary_key=True)
    model_name = Column(String, nullable=False, index=True)
    model_version = Column(String, nullable=False)
    reference_snapshot_id = Column(String, nullable=False)
    evaluated_at = Column(DateTime, nullable=False, index=True)
    window_size = Column(Integer, nullable=False)
    overall_status = Column(String, nullable=False, index=True)
    drifted_feature_count = Column(Integer, nullable=False)
    total_feature_count = Column(Integer, nullable=False)
    drift_score = Column(Float, nullable=False)
    triggers_retraining = Column(Boolean, nullable=False, default=False)
    feature_results = Column(JSONB, nullable=False, default=list)


class SkewReportORM(Base):
    __tablename__ = "skew_reports"
    id = Column(String, primary_key=True)
    model_name = Column(String, nullable=False, index=True)
    model_version = Column(String, nullable=False)
    snapshot_id = Column(String, nullable=False)
    evaluated_at = Column(DateTime, nullable=False, index=True)
    serving_window_size = Column(Integer, nullable=False)
    overall_status = Column(String, nullable=False, index=True)
    skewed_feature_count = Column(Integer, nullable=False)
    total_feature_count = Column(Integer, nullable=False)
    feature_results = Column(JSONB, nullable=False, default=list)


class RetrainingJobORM(Base):
    __tablename__ = "retraining_jobs"
    id = Column(String, primary_key=True)
    model_name = Column(String, nullable=False, index=True)
    model_version = Column(String, nullable=False)
    trigger_reason = Column(String, nullable=False)
    status = Column(String, nullable=False, index=True)
    created_at = Column(DateTime, nullable=False, index=True)
    dispatched_at = Column(DateTime, nullable=True)
    completed_at = Column(DateTime, nullable=True)
    drift_report_id = Column(String, nullable=True)
    drifted_features = Column(JSONB, nullable=False, default=list)
    drift_score = Column(Float, nullable=False, default=0.0)
    error_message = Column(Text, nullable=True)
    new_model_version = Column(String, nullable=True)


class PipelineRunORM(Base):
    __tablename__ = "pipeline_runs"
    id = Column(String, primary_key=True)
    pipeline_name = Column(String, nullable=False, index=True)
    model_name = Column(String, nullable=False, index=True)
    triggered_by = Column(String, nullable=False)
    status = Column(String, nullable=False, index=True)
    started_at = Column(DateTime, nullable=False)
    finished_at = Column(DateTime, nullable=True)
    duration_ms = Column(Float, nullable=False, default=0.0)
    input_rows = Column(Integer, nullable=False, default=0)
    output_rows = Column(Integer, nullable=False, default=0)
    step_results = Column(JSONB, nullable=False, default=list)
    artifacts = Column(JSONB, nullable=False, default=dict)
    error_message = Column(Text, nullable=True)


class TrainingSnapshotORM(Base):
    __tablename__ = "training_snapshots"
    id = Column(String, primary_key=True)
    model_name = Column(String, nullable=False, index=True)
    model_version = Column(String, nullable=False)
    captured_at = Column(DateTime, nullable=False, index=True)
    row_count = Column(Integer, nullable=False)
    feature_stats = Column(JSONB, nullable=False, default=list)


class MLOpsRepository:
    def __init__(self) -> None:
        self._engine = create_async_engine(
            settings.database_url,
            pool_size=settings.database_pool_size,
            max_overflow=settings.database_max_overflow,
        )
        self._session_factory = async_sessionmaker(self._engine, expire_on_commit=False)

    async def init_db(self) -> None:
        async with self._engine.begin() as conn:
            await conn.run_sync(Base.metadata.create_all)
        log.info("mlops_database_ready")

    # ------------------------------------------------------------------
    # Drift reports
    # ------------------------------------------------------------------

    async def save_drift_report(self, report: DriftReport) -> None:
        async with self._session_factory() as s:
            s.add(DriftReportORM(
                id=report.report_id,
                model_name=report.model_name,
                model_version=report.model_version,
                reference_snapshot_id=report.reference_snapshot_id,
                evaluated_at=report.evaluated_at,
                window_size=report.window_size,
                overall_status=report.overall_status.value,
                drifted_feature_count=report.drifted_feature_count,
                total_feature_count=report.total_feature_count,
                drift_score=report.drift_score,
                triggers_retraining=report.triggers_retraining,
                feature_results=[r.model_dump() for r in report.feature_results],
            ))
            await s.commit()

    async def get_drift_history(
        self, model_name: str, hours: int = 24, limit: int = 100
    ) -> list[dict]:
        since = datetime.utcnow() - timedelta(hours=hours)
        async with self._session_factory() as s:
            q = (select(DriftReportORM)
                 .where(DriftReportORM.model_name == model_name,
                        DriftReportORM.evaluated_at >= since)
                 .order_by(DriftReportORM.evaluated_at.desc())
                 .limit(limit))
            rows = (await s.execute(q)).scalars().all()
            return [_orm_dict(r) for r in rows]

    # ------------------------------------------------------------------
    # Skew reports
    # ------------------------------------------------------------------

    async def save_skew_report(self, report: SkewReport) -> None:
        async with self._session_factory() as s:
            s.add(SkewReportORM(
                id=report.report_id,
                model_name=report.model_name,
                model_version=report.model_version,
                snapshot_id=report.snapshot_id,
                evaluated_at=report.evaluated_at,
                serving_window_size=report.serving_window_size,
                overall_status=report.overall_status.value,
                skewed_feature_count=report.skewed_feature_count,
                total_feature_count=report.total_feature_count,
                feature_results=[r.model_dump() for r in report.feature_results],
            ))
            await s.commit()

    # ------------------------------------------------------------------
    # Retraining jobs
    # ------------------------------------------------------------------

    async def save_retraining_job(self, job: RetrainingJob) -> None:
        async with self._session_factory() as s:
            s.add(RetrainingJobORM(
                id=job.job_id,
                model_name=job.trigger.model_name,
                model_version=job.trigger.model_version,
                trigger_reason=job.trigger.reason.value,
                status=job.status.value,
                created_at=job.created_at,
                dispatched_at=job.dispatched_at,
                completed_at=job.completed_at,
                drift_report_id=job.trigger.drift_report_id,
                drifted_features=job.trigger.drifted_features,
                drift_score=job.trigger.drift_score,
                error_message=job.error_message,
                new_model_version=job.new_model_version,
            ))
            await s.commit()

    async def get_retraining_history(
        self, model_name: str, limit: int = 50
    ) -> list[dict]:
        async with self._session_factory() as s:
            q = (select(RetrainingJobORM)
                 .where(RetrainingJobORM.model_name == model_name)
                 .order_by(RetrainingJobORM.created_at.desc())
                 .limit(limit))
            rows = (await s.execute(q)).scalars().all()
            return [_orm_dict(r) for r in rows]

    # ------------------------------------------------------------------
    # Pipeline runs
    # ------------------------------------------------------------------

    async def save_pipeline_run(self, run: PipelineRun) -> None:
        async with self._session_factory() as s:
            s.add(PipelineRunORM(
                id=run.run_id,
                pipeline_name=run.pipeline_name,
                model_name=run.model_name,
                triggered_by=run.triggered_by,
                status=run.status.value,
                started_at=run.started_at,
                finished_at=run.finished_at,
                duration_ms=run.duration_ms,
                input_rows=run.input_rows,
                output_rows=run.output_rows,
                step_results=[r.model_dump() for r in run.step_results],
                artifacts=run.artifacts,
                error_message=run.error_message,
            ))
            await s.commit()

    # ------------------------------------------------------------------
    # Training snapshots
    # ------------------------------------------------------------------

    async def save_snapshot(self, snapshot: TrainingSnapshot) -> None:
        async with self._session_factory() as s:
            s.add(TrainingSnapshotORM(
                id=snapshot.snapshot_id,
                model_name=snapshot.model_name,
                model_version=snapshot.model_version,
                captured_at=snapshot.captured_at,
                row_count=snapshot.row_count,
                feature_stats=[f.model_dump() for f in snapshot.feature_stats],
            ))
            await s.commit()

    async def get_latest_snapshot(self, model_name: str) -> dict | None:
        async with self._session_factory() as s:
            q = (select(TrainingSnapshotORM)
                 .where(TrainingSnapshotORM.model_name == model_name)
                 .order_by(TrainingSnapshotORM.captured_at.desc())
                 .limit(1))
            row = (await s.execute(q)).scalars().first()
            return _orm_dict(row) if row else None

    async def close(self) -> None:
        await self._engine.dispose()


def _orm_dict(obj) -> dict:
    return {c.key: getattr(obj, c.key) for c in obj.__table__.columns}
