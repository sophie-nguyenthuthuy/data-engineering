"""
Quota enforcement middleware.

Runs after TenantMiddleware. Checks the request token bucket for the
resolved tenant and returns 429 if the quota is exhausted.
"""
import structlog
from fastapi import Request, Response
from fastapi.responses import JSONResponse
from starlette.middleware.base import BaseHTTPMiddleware, RequestResponseEndpoint

import redis.asyncio as aioredis

from core.config import settings
from core.quotas.engine import QuotaEngine


log = structlog.get_logger()

_SKIP_PATHS = {"/health", "/metrics", "/docs", "/openapi.json", "/auth/token", "/admin"}


class QuotaMiddleware(BaseHTTPMiddleware):
    def __init__(self, app) -> None:  # type: ignore[override]
        super().__init__(app)
        redis_client = aioredis.from_url(settings.redis_url, decode_responses=False)
        self._quota = QuotaEngine(redis_client)

    async def dispatch(self, request: Request, call_next: RequestResponseEndpoint) -> Response:
        tenant = getattr(request.state, "tenant", None)

        if tenant is None or any(request.url.path.startswith(p) for p in _SKIP_PATHS):
            return await call_next(request)

        result = await self._quota.check_request(tenant.tenant_id, tenant.tier)
        if not result.allowed:
            log.warning(
                "quota.exceeded",
                tenant_id=str(tenant.tenant_id),
                dimension="requests",
                path=request.url.path,
            )
            return JSONResponse(
                {"detail": "Rate limit exceeded. Upgrade your plan for higher limits."},
                status_code=429,
                headers={"Retry-After": "60"},
            )

        return await call_next(request)
