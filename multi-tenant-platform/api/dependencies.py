"""
FastAPI dependency injection — shared across all routers.
"""
from typing import Annotated
from uuid import UUID

import redis.asyncio as aioredis
from fastapi import Depends, HTTPException, Request, status
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select, text

from core.auth.api_keys import verify_api_key
from core.config import settings
from core.quotas.engine import QuotaEngine
from core.storage.object_store import ObjectStore
from db.session import get_session_factory
from db.models.tenant import Tenant, ApiKey
from api.middleware.tenant import ResolvedTenant


# --- Redis / Quota ---

def _get_redis() -> aioredis.Redis:
    return aioredis.from_url(settings.redis_url, decode_responses=False)


def get_quota_engine(redis: Annotated[aioredis.Redis, Depends(_get_redis)]) -> QuotaEngine:
    return QuotaEngine(redis)


# --- Storage ---

def get_object_store() -> ObjectStore:
    return ObjectStore()


# --- Database session (no RLS — raw) ---

async def get_db() -> AsyncSession:  # type: ignore[return]
    factory = get_session_factory()
    async with factory() as session:
        async with session.begin():
            yield session


# --- Tenant resolution via API key (fallback from middleware) ---

async def resolve_api_key_tenant(request: Request, db: Annotated[AsyncSession, Depends(get_db)]) -> ResolvedTenant | None:
    raw_key = getattr(request.state, "raw_api_key", None)
    if not raw_key:
        return None

    result = await db.execute(
        select(ApiKey, Tenant)
        .join(Tenant, ApiKey.tenant_id == Tenant.id)
        .where(ApiKey.is_active.is_(True), Tenant.is_active.is_(True))
    )
    for api_key, tenant in result:
        if verify_api_key(raw_key, api_key.key_hash):
            await db.execute(
                text("UPDATE platform.api_keys SET last_used_at = NOW() WHERE id = :id"),
                {"id": api_key.id},
            )
            return ResolvedTenant(
                tenant_id=tenant.id,
                user_id=f"apikey:{api_key.id}",
                role="member",
                tier=tenant.tier,
            )
    return None


# --- Current tenant (raises 401 if unauthenticated) ---

async def require_tenant(
    request: Request,
    api_key_tenant: Annotated[ResolvedTenant | None, Depends(resolve_api_key_tenant)],
) -> ResolvedTenant:
    tenant = getattr(request.state, "tenant", None) or api_key_tenant
    if tenant is None:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Authentication required")
    return tenant


CurrentTenant = Annotated[ResolvedTenant, Depends(require_tenant)]


def require_role(*roles: str):
    async def _check(tenant: CurrentTenant) -> ResolvedTenant:
        if tenant.role not in roles:
            raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="Insufficient permissions")
        return tenant
    return _check
