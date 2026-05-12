from typing import Annotated
from uuid import uuid4

from fastapi import APIRouter, Depends, HTTPException, status
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select

from api.dependencies import CurrentTenant, get_db, require_role, get_quota_engine
from api.schemas.tenant import ApiKeyCreate, ApiKeyResponse, TenantResponse
from core.auth.api_keys import generate_api_key
from core.quotas.engine import QuotaEngine
from db.models.tenant import ApiKey, Tenant


router = APIRouter()


@router.get("/me", response_model=TenantResponse)
async def get_my_tenant(
    tenant: CurrentTenant,
    db: Annotated[AsyncSession, Depends(get_db)],
) -> TenantResponse:
    result = await db.execute(select(Tenant).where(Tenant.id == tenant.tenant_id))
    t = result.scalar_one_or_none()
    if t is None:
        raise HTTPException(status_code=404, detail="Tenant not found")
    return TenantResponse.model_validate(t)


@router.get("/me/quota")
async def get_quota_usage(
    tenant: CurrentTenant,
    quota: Annotated[QuotaEngine, Depends(get_quota_engine)],
    db: Annotated[AsyncSession, Depends(get_db)],
) -> dict:
    result = await db.execute(select(Tenant).where(Tenant.id == tenant.tenant_id))
    t = result.scalar_one_or_none()

    storage_bytes = await quota.get_storage_used(tenant.tenant_id)
    return {
        "tenant_id": str(tenant.tenant_id),
        "tier": t.tier if t else "free",
        "storage_bytes_used": storage_bytes,
    }


@router.post("/me/api-keys", response_model=ApiKeyResponse, status_code=status.HTTP_201_CREATED)
async def create_api_key(
    body: ApiKeyCreate,
    tenant: Annotated[CurrentTenant, Depends(require_role("owner", "admin"))],
    db: Annotated[AsyncSession, Depends(get_db)],
) -> ApiKeyResponse:
    raw_key, key_hash = generate_api_key()
    api_key = ApiKey(
        id=uuid4(),
        tenant_id=tenant.tenant_id,
        name=body.name,
        key_hash=key_hash,
    )
    db.add(api_key)
    await db.flush()
    return ApiKeyResponse(id=api_key.id, name=api_key.name, raw_key=raw_key, is_active=True)


@router.delete("/me/api-keys/{key_id}", status_code=status.HTTP_204_NO_CONTENT)
async def revoke_api_key(
    key_id: str,
    tenant: Annotated[CurrentTenant, Depends(require_role("owner", "admin"))],
    db: Annotated[AsyncSession, Depends(get_db)],
) -> None:
    from uuid import UUID
    result = await db.execute(
        select(ApiKey).where(ApiKey.id == UUID(key_id), ApiKey.tenant_id == tenant.tenant_id)
    )
    key = result.scalar_one_or_none()
    if key is None:
        raise HTTPException(status_code=404, detail="API key not found")
    key.is_active = False
