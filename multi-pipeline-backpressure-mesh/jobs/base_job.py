"""Abstract base class for a simulated streaming job.

Simulates the Flink/Spark model:
  - source reader → processing loop → sink writer
  - exposes a metrics snapshot callable (simulating Flink REST metrics API)

The job itself has no knowledge of the backpressure mesh.  All coordination
happens externally via the sidecar that wraps the job's throttle handle.
"""
from __future__ import annotations

import asyncio
import logging
import time
from abc import ABC, abstractmethod
from collections import deque
from typing import Any, Optional

from mesh.metrics import JobMetrics
from mesh.throttle import TokenBucketThrottle

logger = logging.getLogger(__name__)


class BaseStreamingJob(ABC):
    def __init__(
        self,
        job_id: str,
        source_rate: float = 500.0,
        queue_capacity: int = 1000,
    ) -> None:
        self.job_id = job_id
        self._input_queue: asyncio.Queue = asyncio.Queue(maxsize=queue_capacity)
        self._output_queue: asyncio.Queue = asyncio.Queue(maxsize=queue_capacity)
        self._queue_capacity = queue_capacity

        # The throttle lives here but is driven exclusively by the external sidecar
        self.throttle = TokenBucketThrottle(rate=source_rate)

        self._records_in = 0
        self._records_out = 0
        self._window_start = time.monotonic()
        self._window_in = 0
        self._window_out = 0
        self._lag_samples: deque[float] = deque(maxlen=100)

        self._task: Optional[asyncio.Task] = None

    async def start(self) -> None:
        self._task = asyncio.create_task(self._run(), name=f"job-{self.job_id}")
        logger.info("Job %s started", self.job_id)

    async def stop(self) -> None:
        if self._task:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
        logger.info("Job %s stopped", self.job_id)

    async def _run(self) -> None:
        while True:
            try:
                # Honour external throttle before reading next record
                await self.throttle.acquire()
                record = await self._read_record()
                if record is None:
                    await asyncio.sleep(0.001)
                    continue

                t0 = time.monotonic()
                processed = await self._process(record)
                latency_ms = (time.monotonic() - t0) * 1000

                self._lag_samples.append(latency_ms)
                self._records_in += 1
                self._window_in += 1

                if processed is not None:
                    await self._write_record(processed)
                    self._records_out += 1
                    self._window_out += 1

            except asyncio.CancelledError:
                raise
            except Exception:
                logger.exception("Job %s processing error", self.job_id)

    @abstractmethod
    async def _read_record(self) -> Optional[Any]: ...

    @abstractmethod
    async def _process(self, record: Any) -> Optional[Any]: ...

    @abstractmethod
    async def _write_record(self, record: Any) -> None: ...

    def get_metrics(self) -> JobMetrics:
        now = time.monotonic()
        window = max(now - self._window_start, 0.001)

        in_rate = self._window_in / window
        out_rate = self._window_out / window

        # Reset window
        self._window_start = now
        self._window_in = 0
        self._window_out = 0

        avg_lat = sum(self._lag_samples) / len(self._lag_samples) if self._lag_samples else 0.0

        return JobMetrics(
            job_id=self.job_id,
            records_in_per_sec=round(in_rate, 1),
            records_out_per_sec=round(out_rate, 1),
            input_queue_depth=self._input_queue.qsize(),
            input_queue_capacity=self._queue_capacity,
            output_queue_depth=self._output_queue.qsize(),
            output_queue_capacity=self._queue_capacity,
            processing_lag_ms=round(avg_lat, 2),
            avg_record_latency_ms=round(avg_lat, 2),
        )
