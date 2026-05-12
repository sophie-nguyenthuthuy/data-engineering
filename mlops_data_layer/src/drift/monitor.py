from __future__ import annotations
import asyncio
from datetime import datetime

import redis.asyncio as aioredis
import structlog

from ..config import settings
from ..models import DriftReport, DriftStatus
from ..features.store import FeatureStore
from .detector import DriftDetector

log = structlog.get_logger(__name__)


class DriftMonitor:
    """
    Background service that periodically:
    1. Pulls the latest serving window from the FeatureStore
    2. Runs the DriftDetector against the latest training snapshot
    3. Publishes a DriftReport to Redis pub/sub
    4. If drift triggers retraining → pushes a retrain event

    Designed to run as a long-lived asyncio task alongside the API server.
    """

    def __init__(
        self,
        model_name: str,
        model_version: str,
        store: FeatureStore,
        detector: DriftDetector,
        redis_client: aioredis.Redis,
        interval_seconds: float | None = None,
    ) -> None:
        self._model_name = model_name
        self._model_version = model_version
        self._store = store
        self._detector = detector
        self._redis = redis_client
        self._interval = interval_seconds or settings.drift_eval_interval_seconds
        self._task: asyncio.Task | None = None
        self._last_report: DriftReport | None = None

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    async def start(self) -> None:
        self._task = asyncio.create_task(self._run_loop(), name=f"drift_monitor_{self._model_name}")
        log.info("drift_monitor_started", model=self._model_name, interval=self._interval)

    async def stop(self) -> None:
        if self._task:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
        log.info("drift_monitor_stopped", model=self._model_name)

    # ------------------------------------------------------------------
    # On-demand check
    # ------------------------------------------------------------------

    async def check_now(self) -> DriftReport | None:
        snapshot = await self._store.get_training_snapshot(
            self._model_name, self._model_version
        )
        if snapshot is None:
            log.warning("no_training_snapshot", model=self._model_name)
            return None

        serving_df = await self._store.get_serving_window(
            self._model_name, limit=settings.drift_eval_window_size
        )
        if serving_df.empty or len(serving_df) < 10:
            log.info("insufficient_serving_data", model=self._model_name, rows=len(serving_df))
            return None

        report = self._detector.detect(snapshot, serving_df)
        self._last_report = report
        await self._publish(report)
        return report

    @property
    def last_report(self) -> DriftReport | None:
        return self._last_report

    # ------------------------------------------------------------------
    # Internal loop
    # ------------------------------------------------------------------

    async def _run_loop(self) -> None:
        while True:
            try:
                await self.check_now()
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                log.error("drift_monitor_error", model=self._model_name, error=str(exc))
            await asyncio.sleep(self._interval)

    async def _publish(self, report: DriftReport) -> None:
        payload = report.model_dump_json()

        # Cache the latest report
        await self._redis.set(
            f"mlops:drift:latest:{self._model_name}:{self._model_version}",
            payload,
            ex=self._interval * 3,
        )

        # Broadcast to drift channel
        await self._redis.publish(settings.drift_channel, payload)

        if report.triggers_retraining:
            log.warning(
                "drift_triggers_retraining",
                model=self._model_name,
                drifted_features=[r.feature_name for r in report.drifted_features()],
                drift_score=report.drift_score,
            )
            await self._redis.publish(settings.retrain_channel, payload)
