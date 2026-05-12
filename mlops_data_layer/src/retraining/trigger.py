from __future__ import annotations
import asyncio
from datetime import datetime, timedelta

import aiohttp
import redis.asyncio as aioredis
import structlog

from ..config import settings
from ..models import (
    DriftReport,
    SkewReport,
    DriftStatus,
    RetrainingTrigger,
    RetrainingJob,
    RetrainingJobStatus,
    TriggerReason,
)

log = structlog.get_logger(__name__)

_LAST_TRIGGER_KEY = "mlops:retrain:last_trigger:{model_name}"
_JOB_KEY = "mlops:retrain:job:{job_id}"


class RetrainingTriggerEngine:
    """
    Decides whether to fire a retraining job and dispatches it.

    Rules:
    - Drift-triggered: DriftReport with status DRIFT_DETECTED
    - Skew-triggered:  SkewReport  with status DRIFT_DETECTED
    - Cooldown:        at most one trigger per ``retrain_cooldown_seconds``
    - Dispatch:        writes job record to Redis + publishes to Kafka topic
                       + optionally POSTs to a webhook URL
    """

    def __init__(self, redis_client: aioredis.Redis) -> None:
        self._redis = redis_client

    # ------------------------------------------------------------------
    # Evaluate drift report
    # ------------------------------------------------------------------

    async def evaluate_drift(self, report: DriftReport) -> RetrainingJob | None:
        if report.overall_status != DriftStatus.DRIFT_DETECTED:
            return None

        trigger = RetrainingTrigger(
            model_name=report.model_name,
            model_version=report.model_version,
            reason=TriggerReason.DATA_DRIFT,
            drift_report_id=report.report_id,
            drifted_features=[r.feature_name for r in report.drifted_features()],
            drift_score=report.drift_score,
        )
        return await self._maybe_dispatch(trigger)

    # ------------------------------------------------------------------
    # Evaluate skew report
    # ------------------------------------------------------------------

    async def evaluate_skew(self, report: SkewReport) -> RetrainingJob | None:
        if report.overall_status != DriftStatus.DRIFT_DETECTED:
            return None

        trigger = RetrainingTrigger(
            model_name=report.model_name,
            model_version=report.model_version,
            reason=TriggerReason.TRAINING_SERVING_SKEW,
            skew_report_id=report.report_id,
            drifted_features=[r.feature_name for r in report.feature_results
                               if r.status == DriftStatus.DRIFT_DETECTED],
        )
        return await self._maybe_dispatch(trigger)

    # ------------------------------------------------------------------
    # Manual trigger
    # ------------------------------------------------------------------

    async def trigger_manual(
        self, model_name: str, model_version: str, reason: str = ""
    ) -> RetrainingJob:
        trigger = RetrainingTrigger(
            model_name=model_name,
            model_version=model_version,
            reason=TriggerReason.MANUAL,
            metadata={"reason": reason},
        )
        return await self._dispatch(trigger)

    # ------------------------------------------------------------------
    # Core dispatch
    # ------------------------------------------------------------------

    async def _maybe_dispatch(self, trigger: RetrainingTrigger) -> RetrainingJob | None:
        """Return None (skip) if within cooldown window."""
        key = _LAST_TRIGGER_KEY.format(model_name=trigger.model_name)
        last_raw = await self._redis.get(key)
        if last_raw:
            last_ts = datetime.fromisoformat(last_raw.decode())
            elapsed = (datetime.utcnow() - last_ts).total_seconds()
            if elapsed < settings.retrain_cooldown_seconds:
                remaining = int(settings.retrain_cooldown_seconds - elapsed)
                log.info(
                    "retrain_cooldown_active",
                    model=trigger.model_name,
                    remaining_s=remaining,
                )
                job = RetrainingJob(trigger=trigger, status=RetrainingJobStatus.SKIPPED)
                return job

        return await self._dispatch(trigger)

    async def _dispatch(self, trigger: RetrainingTrigger) -> RetrainingJob:
        job = RetrainingJob(trigger=trigger, status=RetrainingJobStatus.DISPATCHED)
        job.dispatched_at = datetime.utcnow()

        # Persist job record
        await self._redis.setex(
            _JOB_KEY.format(job_id=job.job_id),
            86400,
            job.model_dump_json(),
        )

        # Update cooldown timestamp
        key = _LAST_TRIGGER_KEY.format(model_name=trigger.model_name)
        await self._redis.setex(
            key,
            settings.retrain_cooldown_seconds,
            datetime.utcnow().isoformat(),
        )

        # Publish to retrain channel
        await self._redis.publish(settings.retrain_channel, job.model_dump_json())

        log.warning(
            "retraining_dispatched",
            job_id=job.job_id,
            model=trigger.model_name,
            reason=trigger.reason,
            features=trigger.drifted_features,
        )

        # Optional webhook notification
        if settings.retrain_webhook_url:
            asyncio.create_task(self._call_webhook(job))

        return job

    async def _call_webhook(self, job: RetrainingJob) -> None:
        try:
            async with aiohttp.ClientSession() as session:
                async with session.post(
                    settings.retrain_webhook_url,
                    json=job.model_dump(),
                    timeout=aiohttp.ClientTimeout(total=10),
                ) as resp:
                    log.info("webhook_called", status=resp.status, job_id=job.job_id)
        except Exception as exc:
            log.error("webhook_failed", error=str(exc), job_id=job.job_id)

    # ------------------------------------------------------------------
    # Job queries
    # ------------------------------------------------------------------

    async def get_job(self, job_id: str) -> RetrainingJob | None:
        raw = await self._redis.get(_JOB_KEY.format(job_id=job_id))
        if not raw:
            return None
        return RetrainingJob.model_validate_json(raw)
