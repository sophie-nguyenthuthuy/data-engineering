from __future__ import annotations
from typing import Annotated

from fastapi import APIRouter, HTTPException, Query

router = APIRouter(prefix="/api/v1/drift", tags=["drift"])


@router.get("/{model_name}/latest")
async def get_latest_drift_report(model_name: str, request):
    """Return the most recent drift report from the Redis cache."""
    redis = request.app.state.redis
    model_version = request.app.state.model_version
    raw = await redis.get(f"mlops:drift:latest:{model_name}:{model_version}")
    if not raw:
        raise HTTPException(status_code=404, detail="No drift report available yet")
    import json
    return json.loads(raw)


@router.get("/{model_name}/history")
async def drift_history(
    model_name: str,
    request,
    hours: Annotated[int, Query(ge=1, le=168)] = 24,
    limit: Annotated[int, Query(ge=1, le=500)] = 100,
):
    repo = request.app.state.repository
    return await repo.get_drift_history(model_name, hours=hours, limit=limit)


@router.post("/{model_name}/check")
async def trigger_drift_check(model_name: str, request):
    """Manually kick off a drift check right now."""
    monitor = request.app.state.drift_monitor
    report = await monitor.check_now()
    if report is None:
        raise HTTPException(status_code=422, detail="Insufficient data or missing snapshot")
    repo = request.app.state.repository
    await repo.save_drift_report(report)
    return report


@router.get("/{model_name}/skew/latest")
async def get_latest_skew_report(model_name: str, request):
    redis = request.app.state.redis
    model_version = request.app.state.model_version
    raw = await redis.get(f"mlops:skew:latest:{model_name}:{model_version}")
    if not raw:
        raise HTTPException(status_code=404, detail="No skew report available yet")
    import json
    return json.loads(raw)
