"""
Admin-only routes for tenant provisioning.
Protected by a static admin token (swap for RBAC/IAM in production).
"""
from typing import Annotated
from uuid import uuid4

from fastapi import APIRouter, Depends, Header, HTTPException, status
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select

from api.dependencies import get_db
from api.schemas.tenant import TenantCreate, TenantResponse
from core.config import settings
from core.storage.object_store import ObjectStore
from db.models.tenant import Tenant


router = APIRouter()


async def require_admin(x_admin_token: Annotated[str, Header()]) -> None:
    if x_admin_token != settings.admin_token.get_secret_value():
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="Invalid admin token")


AdminGuard = Depends(require_admin)


@router.post("/tenants", response_model=TenantResponse, status_code=status.HTTP_201_CREATED)
async def provision_tenant(
    body: TenantCreate,
    _: Annotated[None, AdminGuard],
    db: Annotated[AsyncSession, Depends(get_db)],
) -> TenantResponse:
    tenant = Tenant(
        id=uuid4(),
        name=body.name,
        slug=body.slug,
        tier=body.tier,
    )
    db.add(tenant)
    await db.flush()

    store = ObjectStore()
    await store.ensure_bucket(tenant.id)

    return TenantResponse.model_validate(tenant)


@router.get("/tenants", response_model=list[TenantResponse])
async def list_tenants(
    _: Annotated[None, AdminGuard],
    db: Annotated[AsyncSession, Depends(get_db)],
) -> list[TenantResponse]:
    result = await db.execute(select(Tenant).order_by(Tenant.created_at.desc()))
    return [TenantResponse.model_validate(t) for t in result.scalars()]


@router.patch("/tenants/{tenant_id}/tier")
async def change_tier(
    tenant_id: str,
    tier: str,
    _: Annotated[None, AdminGuard],
    db: Annotated[AsyncSession, Depends(get_db)],
) -> TenantResponse:
    from uuid import UUID
    if tier not in {"free", "starter", "pro", "enterprise"}:
        raise HTTPException(status_code=400, detail="Invalid tier")
    result = await db.execute(select(Tenant).where(Tenant.id == UUID(tenant_id)))
    tenant = result.scalar_one_or_none()
    if tenant is None:
        raise HTTPException(status_code=404, detail="Tenant not found")
    tenant.tier = tier
    return TenantResponse.model_validate(tenant)


@router.delete("/tenants/{tenant_id}", status_code=status.HTTP_204_NO_CONTENT)
async def deactivate_tenant(
    tenant_id: str,
    _: Annotated[None, AdminGuard],
    db: Annotated[AsyncSession, Depends(get_db)],
) -> None:
    from uuid import UUID
    result = await db.execute(select(Tenant).where(Tenant.id == UUID(tenant_id)))
    tenant = result.scalar_one_or_none()
    if tenant is None:
        raise HTTPException(status_code=404, detail="Tenant not found")
    tenant.is_active = False
