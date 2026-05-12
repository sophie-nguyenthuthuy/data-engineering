from __future__ import annotations
from typing import Annotated

from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel

router = APIRouter(prefix="/api/v1/retraining", tags=["retraining"])


class ManualTriggerRequest(BaseModel):
    model_name: str
    model_version: str
    reason: str = "manual trigger via API"


class ScheduleRequest(BaseModel):
    model_name: str
    model_version: str
    schedule_type: str = "cron"   # cron | interval
    cron_expression: str = "0 2 * * 0"
    interval_hours: int = 24


@router.post("/trigger")
async def manual_trigger(body: ManualTriggerRequest, request):
    """Manually trigger a retraining job (bypasses cooldown)."""
    engine = request.app.state.trigger_engine
    job = await engine.trigger_manual(body.model_name, body.model_version, body.reason)
    repo = request.app.state.repository
    await repo.save_retraining_job(job)
    return job


@router.get("/{model_name}/history")
async def retraining_history(
    model_name: str,
    request,
    limit: Annotated[int, Query(ge=1, le=200)] = 50,
):
    repo = request.app.state.repository
    return await repo.get_retraining_history(model_name, limit=limit)


@router.get("/job/{job_id}")
async def get_job(job_id: str, request):
    engine = request.app.state.trigger_engine
    job = await engine.get_job(job_id)
    if not job:
        raise HTTPException(status_code=404, detail="Job not found")
    return job


@router.post("/schedule")
async def add_schedule(body: ScheduleRequest, request):
    """Register a recurring retraining schedule."""
    scheduler = request.app.state.retraining_scheduler
    if body.schedule_type == "cron":
        job_id = scheduler.add_cron_schedule(
            body.model_name, body.model_version, body.cron_expression
        )
    else:
        job_id = scheduler.add_interval_schedule(
            body.model_name, body.model_version, body.interval_hours
        )
    return {"schedule_id": job_id, "type": body.schedule_type}


@router.get("/schedules")
async def list_schedules(request):
    scheduler = request.app.state.retraining_scheduler
    return scheduler.list_schedules()


@router.delete("/schedule/{schedule_id}")
async def remove_schedule(schedule_id: str, request):
    scheduler = request.app.state.retraining_scheduler
    scheduler.remove_schedule(schedule_id)
    return {"removed": schedule_id}
