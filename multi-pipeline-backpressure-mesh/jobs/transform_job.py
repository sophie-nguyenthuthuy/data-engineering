"""Mid-pipeline transform job — reads from upstream queue, applies transform, writes downstream."""
from __future__ import annotations

import asyncio
import logging
from typing import Any, Optional

from .base_job import BaseStreamingJob

logger = logging.getLogger(__name__)


class TransformJob(BaseStreamingJob):
    """
    Simulates a stateless Flink map/filter operator running as a separate job.
    Connected by shared async queues that represent inter-job Kafka topics.
    """

    def __init__(
        self,
        job_id: str,
        upstream_queue: asyncio.Queue,
        downstream_queue: Optional[asyncio.Queue] = None,
        processing_delay_ms: float = 0.5,
        source_rate: float = 2000.0,
        queue_capacity: int = 1000,
    ) -> None:
        super().__init__(job_id, source_rate=source_rate, queue_capacity=queue_capacity)
        self._upstream = upstream_queue
        self._downstream = downstream_queue or self._output_queue
        self._processing_delay_ms = processing_delay_ms

    async def _read_record(self) -> Optional[Any]:
        try:
            return self._upstream.get_nowait()
        except asyncio.QueueEmpty:
            await asyncio.sleep(0.001)
            return None

    async def _process(self, record: Any) -> Optional[Any]:
        if self._processing_delay_ms > 0:
            await asyncio.sleep(self._processing_delay_ms / 1000.0)
        record["transformed_by"] = self.job_id
        record["value"] = record.get("value", 0) * 2
        return record

    async def _write_record(self, record: Any) -> None:
        try:
            self._downstream.put_nowait(record)
        except asyncio.QueueFull:
            await asyncio.sleep(0.005)
