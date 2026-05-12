from __future__ import annotations

import logging
from datetime import datetime, timedelta
from typing import Optional

from sqlalchemy import (
    Boolean,
    Column,
    DateTime,
    Float,
    Integer,
    String,
    create_engine,
    text,
)
from sqlalchemy.orm import DeclarativeBase, Session, sessionmaker

from .config import MetricsStoreConfig
from .models import ColdStartSavingRecord, JobRun, JobStatus, ScalingAction

logger = logging.getLogger(__name__)


class Base(DeclarativeBase):
    pass


class JobRunRow(Base):
    __tablename__ = "job_runs"

    run_id = Column(String, primary_key=True)
    job_id = Column(String, nullable=False, index=True)
    scheduled_at = Column(DateTime, nullable=False)
    started_at = Column(DateTime)
    finished_at = Column(DateTime)
    status = Column(String, nullable=False)
    peak_cpu_millicores = Column(Float)
    peak_memory_mib = Column(Float)
    avg_workers = Column(Float)
    peak_workers = Column(Integer)
    duration_seconds = Column(Float)
    cold_start_avoided = Column(Boolean, default=False)


class ScalingActionRow(Base):
    __tablename__ = "scaling_actions"

    id = Column(Integer, primary_key=True, autoincrement=True)
    job_id = Column(String, nullable=False, index=True)
    hpa_target = Column(String, nullable=False)
    namespace = Column(String, nullable=False)
    action_at = Column(DateTime, nullable=False)
    min_replicas_before = Column(Integer)
    min_replicas_after = Column(Integer)
    max_replicas_before = Column(Integer)
    max_replicas_after = Column(Integer)
    reason = Column(String)


class ColdStartSavingRow(Base):
    __tablename__ = "cold_start_savings"

    id = Column(Integer, primary_key=True, autoincrement=True)
    job_id = Column(String, nullable=False, index=True)
    run_id = Column(String, nullable=False)
    recorded_at = Column(DateTime, nullable=False)
    workers_prewarmed = Column(Integer)
    cold_start_seconds_saved = Column(Float)
    prewarm_idle_cost_usd = Column(Float)
    cold_start_avoided_cost_usd = Column(Float)


class MetricsStore:
    def __init__(self, cfg: MetricsStoreConfig) -> None:
        self._engine = create_engine(cfg.db_url, echo=False)
        self._session_factory = sessionmaker(bind=self._engine)
        self._retention_days = cfg.retention_days
        Base.metadata.create_all(self._engine)
        logger.info("MetricsStore initialised at %s", cfg.db_url)

    # ------------------------------------------------------------------ #
    #  Job runs                                                            #
    # ------------------------------------------------------------------ #

    def upsert_run(self, run: JobRun) -> None:
        with Session(self._engine) as s:
            row = s.get(JobRunRow, run.run_id)
            if row is None:
                row = JobRunRow(run_id=run.run_id, job_id=run.job_id)
                s.add(row)
            row.scheduled_at = run.scheduled_at
            row.started_at = run.started_at
            row.finished_at = run.finished_at
            row.status = run.status.value
            row.peak_cpu_millicores = run.peak_cpu_millicores
            row.peak_memory_mib = run.peak_memory_mib
            row.avg_workers = run.avg_workers
            row.peak_workers = run.peak_workers
            row.duration_seconds = run.duration_seconds
            row.cold_start_avoided = run.cold_start_avoided
            s.commit()

    def get_completed_runs(self, job_id: str, limit: int = 200) -> list[JobRun]:
        with Session(self._engine) as s:
            rows = (
                s.query(JobRunRow)
                .filter_by(job_id=job_id, status=JobStatus.COMPLETED.value)
                .order_by(JobRunRow.scheduled_at.desc())
                .limit(limit)
                .all()
            )
        return [self._row_to_run(r) for r in rows]

    def get_run(self, run_id: str) -> Optional[JobRun]:
        with Session(self._engine) as s:
            row = s.get(JobRunRow, run_id)
            return self._row_to_run(row) if row else None

    # ------------------------------------------------------------------ #
    #  Scaling actions                                                     #
    # ------------------------------------------------------------------ #

    def record_scaling_action(self, action: ScalingAction) -> None:
        with Session(self._engine) as s:
            s.add(
                ScalingActionRow(
                    job_id=action.job_id,
                    hpa_target=action.hpa_target,
                    namespace=action.namespace,
                    action_at=action.action_at,
                    min_replicas_before=action.min_replicas_before,
                    min_replicas_after=action.min_replicas_after,
                    max_replicas_before=action.max_replicas_before,
                    max_replicas_after=action.max_replicas_after,
                    reason=action.reason,
                )
            )
            s.commit()

    # ------------------------------------------------------------------ #
    #  Cold-start savings                                                  #
    # ------------------------------------------------------------------ #

    def record_saving(self, record: ColdStartSavingRecord) -> None:
        with Session(self._engine) as s:
            s.add(
                ColdStartSavingRow(
                    job_id=record.job_id,
                    run_id=record.run_id,
                    recorded_at=record.recorded_at,
                    workers_prewarmed=record.workers_prewarmed,
                    cold_start_seconds_saved=record.cold_start_seconds_saved,
                    prewarm_idle_cost_usd=record.prewarm_idle_cost_usd,
                    cold_start_avoided_cost_usd=record.cold_start_avoided_cost_usd,
                )
            )
            s.commit()

    def total_net_savings_usd(self) -> float:
        with Session(self._engine) as s:
            result = s.execute(
                text(
                    "SELECT COALESCE(SUM(cold_start_avoided_cost_usd - prewarm_idle_cost_usd), 0) "
                    "FROM cold_start_savings"
                )
            ).scalar()
        return float(result)

    def savings_by_job(self) -> dict[str, float]:
        with Session(self._engine) as s:
            rows = s.execute(
                text(
                    "SELECT job_id, SUM(cold_start_avoided_cost_usd - prewarm_idle_cost_usd) "
                    "FROM cold_start_savings GROUP BY job_id"
                )
            ).fetchall()
        return {r[0]: float(r[1]) for r in rows}

    # ------------------------------------------------------------------ #
    #  Maintenance                                                         #
    # ------------------------------------------------------------------ #

    def purge_old_records(self) -> int:
        cutoff = datetime.utcnow() - timedelta(days=self._retention_days)
        with Session(self._engine) as s:
            deleted = (
                s.query(JobRunRow)
                .filter(JobRunRow.finished_at < cutoff)
                .delete()
            )
            s.commit()
        logger.info("Purged %d old job run records", deleted)
        return deleted

    # ------------------------------------------------------------------ #
    #  Helpers                                                             #
    # ------------------------------------------------------------------ #

    @staticmethod
    def _row_to_run(row: JobRunRow) -> JobRun:
        return JobRun(
            run_id=row.run_id,
            job_id=row.job_id,
            scheduled_at=row.scheduled_at,
            started_at=row.started_at,
            finished_at=row.finished_at,
            status=JobStatus(row.status),
            peak_cpu_millicores=row.peak_cpu_millicores,
            peak_memory_mib=row.peak_memory_mib,
            avg_workers=row.avg_workers,
            peak_workers=row.peak_workers,
            duration_seconds=row.duration_seconds,
            cold_start_avoided=row.cold_start_avoided or False,
        )
