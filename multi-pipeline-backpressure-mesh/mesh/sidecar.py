"""Job sidecar agent.

Runs alongside (external to) a streaming job. Its responsibilities:
  1. Poll the job's metrics endpoint at a configurable interval.
  2. Detect backpressure conditions from those metrics.
  3. Publish BackpressureSignal events to the bus when pressure is detected.
  4. Subscribe to ThrottleCommand events and apply them to the job's
     TokenBucketThrottle, which is wired into the job's source reader.

The sidecar requires *zero* changes to job internals — it only needs:
  - A callable that returns current JobMetrics (metrics_provider).
  - A reference to the job's TokenBucketThrottle (throttle).
"""
from __future__ import annotations

import asyncio
import logging
import time
from typing import Awaitable, Callable, Optional

from .bus import BackpressureBus
from .metrics import BackpressureLevel, BackpressureSignal, JobMetrics, ThrottleCommand
from .throttle import TokenBucketThrottle

logger = logging.getLogger(__name__)

MetricsProvider = Callable[[], JobMetrics | Awaitable[JobMetrics]]

_SIGNAL_COOLDOWN_SECS = 1.0  # minimum interval between emitted signals


def _score_to_level(score: float) -> BackpressureLevel:
    if score < 0.15:
        return BackpressureLevel.NONE
    if score < 0.40:
        return BackpressureLevel.LOW
    if score < 0.65:
        return BackpressureLevel.MEDIUM
    if score < 0.85:
        return BackpressureLevel.HIGH
    return BackpressureLevel.CRITICAL


class JobSidecar:
    def __init__(
        self,
        job_id: str,
        bus: BackpressureBus,
        metrics_provider: MetricsProvider,
        throttle: TokenBucketThrottle,
        poll_interval: float = 1.0,
        pressure_threshold: float = 0.15,
    ) -> None:
        self.job_id = job_id
        self._bus = bus
        self._metrics_provider = metrics_provider
        self._throttle = throttle
        self._poll_interval = poll_interval
        self._pressure_threshold = pressure_threshold
        self._task: Optional[asyncio.Task] = None
        self._last_signal_at: float = 0.0
        self._last_metrics: Optional[JobMetrics] = None

    async def start(self) -> None:
        await self._bus.subscribe_throttle(self.job_id, self._on_throttle_command)
        self._task = asyncio.create_task(self._poll_loop(), name=f"sidecar-{self.job_id}")
        logger.info("Sidecar started for job %s", self.job_id)

    async def stop(self) -> None:
        if self._task:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
        logger.info("Sidecar stopped for job %s", self.job_id)

    async def _poll_loop(self) -> None:
        while True:
            await asyncio.sleep(self._poll_interval)
            try:
                await self._tick()
            except Exception:
                logger.exception("Sidecar poll error for %s", self.job_id)

    async def _tick(self) -> None:
        metrics = await self._get_metrics()
        self._last_metrics = metrics
        score = metrics.backpressure_score()
        level = _score_to_level(score)

        if score >= self._pressure_threshold:
            now = time.monotonic()
            if now - self._last_signal_at >= _SIGNAL_COOLDOWN_SECS:
                signal = BackpressureSignal(
                    source_job_id=self.job_id,
                    level=level,
                    score=round(score, 3),
                    message=(
                        f"queue={metrics.input_utilization:.0%} "
                        f"lag={metrics.processing_lag_ms:.0f}ms "
                        f"throughput_ratio={metrics.throughput_ratio:.2f}"
                    ),
                )
                await self._bus.publish_signal(signal)
                self._last_signal_at = now

    async def _get_metrics(self) -> JobMetrics:
        result = self._metrics_provider()
        if asyncio.iscoroutine(result):
            return await result
        return result

    async def _on_throttle_command(self, cmd: ThrottleCommand) -> None:
        logger.info(
            "Sidecar %s received throttle factor=%.2f reason=%s",
            self.job_id,
            cmd.throttle_factor,
            cmd.reason,
        )
        self._throttle.set_throttle_factor(cmd.throttle_factor)

    @property
    def current_metrics(self) -> Optional[JobMetrics]:
        return self._last_metrics

    @property
    def current_throttle_factor(self) -> float:
        return self._throttle._factor
