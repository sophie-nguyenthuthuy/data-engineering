from __future__ import annotations
from datetime import datetime, timedelta
from typing import Any

import structlog
from sqlalchemy import (
    Column,
    DateTime,
    Float,
    Integer,
    String,
    Text,
    select,
    func,
    text,
)
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine, async_sessionmaker
from sqlalchemy.orm import DeclarativeBase

from ..config import settings
from ..models import ValidationResult, ValidationStatus

log = structlog.get_logger(__name__)


# ---------------------------------------------------------------------------
# ORM models
# ---------------------------------------------------------------------------

class Base(DeclarativeBase):
    pass


class ValidationResultORM(Base):
    __tablename__ = "validation_results"

    id = Column(String, primary_key=True)
    batch_id = Column(String, nullable=False, index=True)
    table_name = Column(String, nullable=False, index=True)
    backend = Column(String, nullable=False)
    suite_name = Column(String, nullable=False)
    validated_at = Column(DateTime, nullable=False, index=True)
    status = Column(String, nullable=False, index=True)
    pass_rate = Column(Float, nullable=False)
    total_checks = Column(Integer, nullable=False)
    passed_checks = Column(Integer, nullable=False)
    failed_checks = Column(Integer, nullable=False)
    warning_checks = Column(Integer, nullable=False)
    row_count = Column(Integer, nullable=False)
    duration_ms = Column(Float, nullable=False)
    error_message = Column(Text, nullable=True)
    check_results = Column(JSONB, nullable=False, default=list)


# ---------------------------------------------------------------------------
# Repository
# ---------------------------------------------------------------------------

class ValidationRepository:
    def __init__(self) -> None:
        self._engine = create_async_engine(
            settings.database_url,
            pool_size=settings.database_pool_size,
            max_overflow=settings.database_max_overflow,
            echo=False,
        )
        self._session_factory = async_sessionmaker(
            self._engine, expire_on_commit=False
        )

    async def init_db(self) -> None:
        async with self._engine.begin() as conn:
            await conn.run_sync(Base.metadata.create_all)
        log.info("database_schema_ready")

    # ------------------------------------------------------------------
    # Write
    # ------------------------------------------------------------------

    async def save_result(self, result: ValidationResult) -> None:
        async with self._session_factory() as session:
            orm = ValidationResultORM(
                id=result.result_id,
                batch_id=result.batch_id,
                table_name=result.table_name,
                backend=result.backend.value,
                suite_name=result.suite_name,
                validated_at=result.validated_at,
                status=result.status.value,
                pass_rate=result.pass_rate,
                total_checks=result.total_checks,
                passed_checks=result.passed_checks,
                failed_checks=result.failed_checks,
                warning_checks=result.warning_checks,
                row_count=result.row_count,
                duration_ms=result.duration_ms,
                error_message=result.error_message,
                check_results=[c.model_dump() for c in result.check_results],
            )
            session.add(orm)
            await session.commit()

    # ------------------------------------------------------------------
    # Read
    # ------------------------------------------------------------------

    async def get_recent_results(
        self, limit: int = 100, table_name: str | None = None
    ) -> list[dict[str, Any]]:
        async with self._session_factory() as session:
            q = select(ValidationResultORM).order_by(
                ValidationResultORM.validated_at.desc()
            )
            if table_name:
                q = q.where(ValidationResultORM.table_name == table_name)
            q = q.limit(limit)
            rows = (await session.execute(q)).scalars().all()
            return [_orm_to_dict(r) for r in rows]

    async def get_pass_rate_last_hour(self, table_name: str) -> float:
        since = datetime.utcnow() - timedelta(hours=1)
        async with self._session_factory() as session:
            q = select(func.avg(ValidationResultORM.pass_rate)).where(
                ValidationResultORM.table_name == table_name,
                ValidationResultORM.validated_at >= since,
            )
            result = (await session.execute(q)).scalar()
            return float(result) if result is not None else 1.0

    async def get_failure_summary(self, hours: int = 1) -> list[dict[str, Any]]:
        since = datetime.utcnow() - timedelta(hours=hours)
        async with self._session_factory() as session:
            q = (
                select(
                    ValidationResultORM.table_name,
                    func.count().label("total"),
                    func.sum(
                        (ValidationResultORM.status == ValidationStatus.FAILED.value).cast(Integer)
                    ).label("failed"),
                    func.avg(ValidationResultORM.pass_rate).label("avg_pass_rate"),
                )
                .where(ValidationResultORM.validated_at >= since)
                .group_by(ValidationResultORM.table_name)
            )
            rows = (await session.execute(q)).all()
            return [
                {
                    "table_name": r.table_name,
                    "total": r.total,
                    "failed": r.failed or 0,
                    "avg_pass_rate": float(r.avg_pass_rate or 1.0),
                }
                for r in rows
            ]

    async def close(self) -> None:
        await self._engine.dispose()


def _orm_to_dict(r: ValidationResultORM) -> dict[str, Any]:
    return {
        "result_id": r.id,
        "batch_id": r.batch_id,
        "table_name": r.table_name,
        "backend": r.backend,
        "status": r.status,
        "pass_rate": r.pass_rate,
        "total_checks": r.total_checks,
        "passed_checks": r.passed_checks,
        "failed_checks": r.failed_checks,
        "row_count": r.row_count,
        "duration_ms": r.duration_ms,
        "validated_at": r.validated_at.isoformat(),
        "error_message": r.error_message,
    }
