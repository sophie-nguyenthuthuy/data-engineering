from __future__ import annotations
import asyncio
from datetime import datetime

import redis.asyncio as aioredis
import structlog
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger
from apscheduler.triggers.interval import IntervalTrigger

from ..config import settings
from ..models import TriggerReason, RetrainingTrigger
from .trigger import RetrainingTriggerEngine

log = structlog.get_logger(__name__)


class RetrainingScheduler:
    """
    Wraps APScheduler to fire scheduled retraining triggers.

    Supports:
    - Cron-based schedules (e.g. "retrain every Sunday at 02:00")
    - Interval-based schedules (e.g. "retrain every 24 h")
    - Redis pub/sub listener (react to drift/skew events in real time)
    """

    def __init__(
        self,
        trigger_engine: RetrainingTriggerEngine,
        redis_client: aioredis.Redis,
    ) -> None:
        self._engine = trigger_engine
        self._redis = redis_client
        self._scheduler = AsyncIOScheduler(timezone="UTC")
        self._listener_task: asyncio.Task | None = None

    # ------------------------------------------------------------------
    # Schedule management
    # ------------------------------------------------------------------

    def add_cron_schedule(
        self,
        model_name: str,
        model_version: str,
        cron_expression: str = "0 2 * * 0",  # Sunday 02:00 UTC
    ) -> str:
        job_id = f"cron_{model_name}_{model_version}"
        self._scheduler.add_job(
            self._fire_scheduled_trigger,
            CronTrigger.from_crontab(cron_expression),
            id=job_id,
            args=[model_name, model_version],
            replace_existing=True,
        )
        log.info("cron_schedule_added", model=model_name, cron=cron_expression)
        return job_id

    def add_interval_schedule(
        self,
        model_name: str,
        model_version: str,
        hours: int = 24,
    ) -> str:
        job_id = f"interval_{model_name}_{model_version}"
        self._scheduler.add_job(
            self._fire_scheduled_trigger,
            IntervalTrigger(hours=hours),
            id=job_id,
            args=[model_name, model_version],
            replace_existing=True,
        )
        log.info("interval_schedule_added", model=model_name, hours=hours)
        return job_id

    def remove_schedule(self, job_id: str) -> None:
        try:
            self._scheduler.remove_job(job_id)
            log.info("schedule_removed", job_id=job_id)
        except Exception:
            pass

    def list_schedules(self) -> list[dict]:
        return [
            {
                "id": job.id,
                "next_run": job.next_run_time.isoformat() if job.next_run_time else None,
                "trigger": str(job.trigger),
            }
            for job in self._scheduler.get_jobs()
        ]

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    async def start(self) -> None:
        self._scheduler.start()
        self._listener_task = asyncio.create_task(
            self._redis_listener(), name="retrain_redis_listener"
        )
        log.info("retraining_scheduler_started")

    async def stop(self) -> None:
        self._scheduler.shutdown(wait=False)
        if self._listener_task:
            self._listener_task.cancel()
            try:
                await self._listener_task
            except asyncio.CancelledError:
                pass
        log.info("retraining_scheduler_stopped")

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    async def _fire_scheduled_trigger(self, model_name: str, model_version: str) -> None:
        log.info("scheduled_retrain_triggered", model=model_name)
        trigger = RetrainingTrigger(
            model_name=model_name,
            model_version=model_version,
            reason=TriggerReason.SCHEDULED,
        )
        await self._engine._dispatch(trigger)

    async def _redis_listener(self) -> None:
        """
        Subscribe to the retrain channel and log / acknowledge
        every inbound retrain event (already dispatched by the trigger engine).
        """
        pubsub = self._redis.pubsub()
        await pubsub.subscribe(settings.retrain_channel)
        log.info("retrain_redis_listener_subscribed")
        async for message in pubsub.listen():
            if message["type"] != "message":
                continue
            data = message["data"]
            if isinstance(data, bytes):
                data = data.decode()
            log.info("retrain_event_received", payload_bytes=len(data))
