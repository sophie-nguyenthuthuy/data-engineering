"""
Tenant resolution middleware.

Resolves the tenant from the JWT token or API key on every request and
stores it in request.state so downstream handlers and the quota/audit
middleware can access it without re-parsing.

Resolution order:
  1. Bearer JWT  → decode → tenant_id + role
  2. X-API-Key   → DB lookup → tenant_id + role
  3. No auth     → request.state.tenant = None (public endpoints only)
"""
from uuid import UUID

from fastapi import Request, Response
from starlette.middleware.base import BaseHTTPMiddleware, RequestResponseEndpoint

from core.auth.jwt import decode_token


class ResolvedTenant:
    def __init__(self, tenant_id: UUID, user_id: str, role: str, tier: str = "free") -> None:
        self.tenant_id = tenant_id
        self.user_id = user_id
        self.role = role
        self.tier = tier


_PUBLIC_PATHS = {"/health", "/metrics", "/docs", "/openapi.json", "/auth/token"}


class TenantMiddleware(BaseHTTPMiddleware):
    async def dispatch(self, request: Request, call_next: RequestResponseEndpoint) -> Response:
        request.state.tenant = None

        if request.url.path in _PUBLIC_PATHS:
            return await call_next(request)

        auth_header = request.headers.get("Authorization", "")
        api_key = request.headers.get("X-API-Key", "")

        if auth_header.startswith("Bearer "):
            token = auth_header.removeprefix("Bearer ")
            try:
                payload = decode_token(token)
                request.state.tenant = ResolvedTenant(
                    tenant_id=payload.tenant_id,
                    user_id=payload.sub,
                    role=payload.role,
                )
            except ValueError:
                from fastapi.responses import JSONResponse
                return JSONResponse({"detail": "Invalid or expired token"}, status_code=401)

        elif api_key:
            # API key resolution deferred to the route handler via dependency
            # so we can use the DB session from DI rather than opening one here.
            request.state.raw_api_key = api_key

        return await call_next(request)
