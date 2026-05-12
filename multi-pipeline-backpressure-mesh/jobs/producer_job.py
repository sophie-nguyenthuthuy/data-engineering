"""High-throughput source job — generates synthetic records at a configurable rate."""
from __future__ import annotations

import asyncio
import itertools
import logging
from typing import Any, Optional

from .base_job import BaseStreamingJob

logger = logging.getLogger(__name__)

_counter = itertools.count(1)


class ProducerJob(BaseStreamingJob):
    """
    Simulates a Flink/Spark source that reads from an infinite stream (e.g. Kafka).
    The TokenBucketThrottle is the only point of contact with the backpressure mesh.
    """

    def __init__(
        self,
        job_id: str,
        source_rate: float = 1000.0,
        downstream_queue: Optional[asyncio.Queue] = None,
        queue_capacity: int = 2000,
    ) -> None:
        super().__init__(job_id, source_rate=source_rate, queue_capacity=queue_capacity)
        self._downstream = downstream_queue or self._output_queue

    async def _read_record(self) -> Optional[Any]:
        # Simulate reading from an external source (Kafka, Kinesis, etc.)
        seq = next(_counter)
        return {"seq": seq, "value": seq * 3.14, "source": self.job_id}

    async def _process(self, record: Any) -> Optional[Any]:
        # Minimal enrichment — source jobs are lightweight
        record["producer"] = self.job_id
        return record

    async def _write_record(self, record: Any) -> None:
        try:
            self._downstream.put_nowait(record)
        except asyncio.QueueFull:
            # Back-off when downstream buffer is full — surface via metrics
            await asyncio.sleep(0.005)
            logger.debug("ProducerJob %s: downstream full, dropping", self.job_id)
