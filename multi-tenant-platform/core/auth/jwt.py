from datetime import datetime, timedelta, timezone
from typing import Any
from uuid import UUID

from jose import JWTError, jwt
from pydantic import BaseModel

from core.config import settings


class TokenPayload(BaseModel):
    sub: str          # user_id
    tenant_id: UUID
    role: str         # owner | admin | member | viewer
    exp: datetime


def create_access_token(
    user_id: str,
    tenant_id: UUID,
    role: str,
    expires_delta: timedelta | None = None,
) -> str:
    expire = datetime.now(timezone.utc) + (
        expires_delta or timedelta(minutes=settings.jwt_expire_minutes)
    )
    payload: dict[str, Any] = {
        "sub": user_id,
        "tenant_id": str(tenant_id),
        "role": role,
        "exp": expire,
    }
    return jwt.encode(payload, settings.jwt_secret.get_secret_value(), algorithm=settings.jwt_algorithm)


def decode_token(token: str) -> TokenPayload:
    try:
        raw = jwt.decode(
            token,
            settings.jwt_secret.get_secret_value(),
            algorithms=[settings.jwt_algorithm],
        )
        return TokenPayload(**raw)
    except JWTError as exc:
        raise ValueError(f"Invalid token: {exc}") from exc
