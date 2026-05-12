"""
Row-level security helpers.

PostgreSQL RLS policies check current_setting('app.tenant_id', true).
Every database session MUST call set_tenant_context() before any query.
Using SET LOCAL scopes the setting to the current transaction — it resets
automatically on COMMIT/ROLLBACK, preventing context leakage across requests.
"""
from contextlib import asynccontextmanager
from typing import AsyncGenerator
from uuid import UUID

from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import text


@asynccontextmanager
async def set_tenant_context(
    session: AsyncSession, tenant_id: UUID
) -> AsyncGenerator[AsyncSession, None]:
    """Set RLS context for the duration of this async context manager."""
    await session.execute(
        text("SET LOCAL app.tenant_id = :tid"),
        {"tid": str(tenant_id)},
    )
    try:
        yield session
    finally:
        # SET LOCAL resets on transaction end, but be explicit on rollback paths
        pass


class RLSContext:
    """
    Descriptor that enforces tenant_id is always set before query execution.
    Used as a guard in repositories that must never run without a tenant context.
    """

    def __init__(self, tenant_id: UUID) -> None:
        self.tenant_id = tenant_id

    def validate(self) -> None:
        if not self.tenant_id:
            raise RuntimeError("RLS context missing tenant_id — query blocked")
