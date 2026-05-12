from typing import Annotated
from uuid import UUID

from fastapi import APIRouter, Depends, HTTPException, status
from fastapi.security import OAuth2PasswordRequestForm
from pydantic import BaseModel
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select

from core.auth.jwt import create_access_token
from db.models.tenant import TenantUser, Tenant
from api.dependencies import get_db


router = APIRouter()


class TokenResponse(BaseModel):
    access_token: str
    token_type: str = "bearer"


class LoginRequest(BaseModel):
    user_id: str
    tenant_slug: str


@router.post("/token", response_model=TokenResponse)
async def get_token(
    body: LoginRequest,
    db: Annotated[AsyncSession, Depends(get_db)],
) -> TokenResponse:
    """
    Exchange user credentials for a JWT scoped to a specific tenant.
    In production, integrate with your IdP (Auth0, Cognito, etc.) here.
    This endpoint is a simplified stand-in.
    """
    result = await db.execute(
        select(TenantUser, Tenant)
        .join(Tenant, TenantUser.tenant_id == Tenant.id)
        .where(
            Tenant.slug == body.tenant_slug,
            TenantUser.user_id == UUID(body.user_id),
            Tenant.is_active.is_(True),
        )
    )
    row = result.first()
    if row is None:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="User not found in tenant",
        )
    tenant_user, tenant = row

    token = create_access_token(
        user_id=body.user_id,
        tenant_id=tenant.id,
        role=tenant_user.role,
    )
    return TokenResponse(access_token=token)
