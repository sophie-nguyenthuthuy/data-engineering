"""Slow sink job — the typical source of backpressure in production pipelines."""
from __future__ import annotations

import asyncio
import logging
import time
from typing import Any, Optional

from .base_job import BaseStreamingJob

logger = logging.getLogger(__name__)


class SinkJob(BaseStreamingJob):
    """
    Simulates a Flink/Spark sink writing to a slow external system
    (e.g. overloaded database, throttled API).

    The sink_rate controls how many records/sec the destination accepts.
    When it degrades (simulate_degradation), throughput drops and the
    sidecar will detect the backpressure condition.
    """

    def __init__(
        self,
        job_id: str,
        upstream_queue: asyncio.Queue,
        sink_rate: float = 200.0,
        source_rate: float = 2000.0,
        queue_capacity: int = 500,
    ) -> None:
        super().__init__(job_id, source_rate=source_rate, queue_capacity=queue_capacity)
        self._upstream = upstream_queue
        self._sink_rate = sink_rate
        self._degraded = False
        self._degraded_rate = sink_rate * 0.1  # 10% of normal when degraded

    def degrade(self, degraded_rate_multiplier: float = 0.05) -> None:
        """Simulate the downstream system slowing down (e.g. DB overload)."""
        self._degraded = True
        self._degraded_rate = self._sink_rate * degraded_rate_multiplier
        logger.warning(
            "SinkJob %s: sink degraded → %.0f rec/s",
            self.job_id,
            self._degraded_rate,
        )

    def recover(self) -> None:
        """Simulate the downstream system recovering."""
        self._degraded = False
        logger.info("SinkJob %s: sink recovered → %.0f rec/s", self.job_id, self._sink_rate)

    async def _read_record(self) -> Optional[Any]:
        try:
            return self._upstream.get_nowait()
        except asyncio.QueueEmpty:
            await asyncio.sleep(0.001)
            return None

    async def _process(self, record: Any) -> Optional[Any]:
        # Simulate write latency to external system
        effective_rate = self._degraded_rate if self._degraded else self._sink_rate
        if effective_rate > 0:
            await asyncio.sleep(1.0 / effective_rate)
        return None  # sink doesn't emit downstream

    async def _write_record(self, record: Any) -> None:
        pass  # terminal job, output absorbed

    def get_metrics(self):
        m = super().get_metrics()
        # Sink reports its output queue as "full" to signal the pressure back upstream
        if self._degraded:
            m.output_queue_depth = int(self._queue_capacity * 0.95)
            m.processing_lag_ms = max(m.processing_lag_ms, 3000.0)
        return m
