"""
Audit logging middleware — records every mutating API call.
Reads are not logged here; sensitive data queries are logged by the repo layer.
"""
import time
import uuid

import structlog
from fastapi import Request, Response
from starlette.middleware.base import BaseHTTPMiddleware, RequestResponseEndpoint

log = structlog.get_logger()

_MUTATING_METHODS = {"POST", "PUT", "PATCH", "DELETE"}


class AuditMiddleware(BaseHTTPMiddleware):
    async def dispatch(self, request: Request, call_next: RequestResponseEndpoint) -> Response:
        start = time.perf_counter()
        request_id = str(uuid.uuid4())
        request.state.request_id = request_id

        response = await call_next(request)

        if request.method in _MUTATING_METHODS:
            tenant = getattr(request.state, "tenant", None)
            elapsed_ms = (time.perf_counter() - start) * 1000
            log.info(
                "api.request",
                request_id=request_id,
                method=request.method,
                path=request.url.path,
                status=response.status_code,
                tenant_id=str(tenant.tenant_id) if tenant else None,
                actor=tenant.user_id if tenant else "anonymous",
                elapsed_ms=round(elapsed_ms, 2),
            )

        return response
