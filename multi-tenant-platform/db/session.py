"""
Async SQLAlchemy session factory.

get_tenant_session() is the primary entry point — it yields a session
with the RLS context already set, so callers cannot forget to scope queries.
"""
from contextlib import asynccontextmanager
from typing import AsyncGenerator
from uuid import UUID

from sqlalchemy.ext.asyncio import (
    AsyncEngine,
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)

from core.config import settings
from core.security.rls import set_tenant_context


def build_engine() -> AsyncEngine:
    return create_async_engine(
        settings.database_url,
        pool_size=settings.database_pool_size,
        max_overflow=settings.database_max_overflow,
        pool_pre_ping=True,
        echo=settings.environment == "development",
    )


_engine: AsyncEngine | None = None
_factory: async_sessionmaker[AsyncSession] | None = None


def get_engine() -> AsyncEngine:
    global _engine
    if _engine is None:
        _engine = build_engine()
    return _engine


def get_session_factory() -> async_sessionmaker[AsyncSession]:
    global _factory
    if _factory is None:
        _factory = async_sessionmaker(get_engine(), expire_on_commit=False)
    return _factory


@asynccontextmanager
async def get_tenant_session(tenant_id: UUID) -> AsyncGenerator[AsyncSession, None]:
    """
    Yields an AsyncSession with SET LOCAL app.tenant_id already executed.
    All queries run inside this context are automatically scoped by RLS.
    """
    factory = get_session_factory()
    async with factory() as session:
        async with session.begin():
            async with set_tenant_context(session, tenant_id) as scoped:
                yield scoped


@asynccontextmanager
async def get_admin_session() -> AsyncGenerator[AsyncSession, None]:
    """Session without RLS — for platform admin operations only."""
    factory = get_session_factory()
    async with factory() as session:
        async with session.begin():
            yield session
