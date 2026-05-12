from __future__ import annotations
import asyncio
import json

import redis.asyncio as aioredis
import structlog

from ..config import settings
from ..models import ValidationResult, MetricSnapshot, QualityMetric
from .collector import MetricsCollector
from ..blocking.job_controller import JobController

log = structlog.get_logger(__name__)


class MetricsPublisher:
    """
    Periodically computes a MetricSnapshot from MetricsCollector data,
    caches it in Redis, and broadcasts it on the Redis pub/sub channel
    so the FastAPI dashboard WebSocket handler can fan it out to browsers.
    """

    def __init__(
        self,
        redis_client: aioredis.Redis,
        collector: MetricsCollector,
        job_controller: JobController,
        interval_seconds: float = 5.0,
    ) -> None:
        self._redis = redis_client
        self._collector = collector
        self._job_controller = job_controller
        self._interval = interval_seconds
        self._task: asyncio.Task | None = None

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    async def start(self) -> None:
        self._task = asyncio.create_task(self._publish_loop(), name="metrics_publisher")
        log.info("metrics_publisher_started", interval_s=self._interval)

    async def stop(self) -> None:
        if self._task:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
        log.info("metrics_publisher_stopped")

    # ------------------------------------------------------------------
    # Publish one result immediately (event-driven path)
    # ------------------------------------------------------------------

    async def publish_result(self, result: ValidationResult) -> None:
        """Cache an individual result in Redis with TTL."""
        key = f"dq:result:{result.result_id}"
        await self._redis.setex(
            key,
            settings.redis_results_ttl_seconds,
            result.model_dump_json(),
        )
        # Also push to a sorted set for recency queries
        score = result.validated_at.timestamp()
        await self._redis.zadd(
            f"dq:results:{result.table_name}",
            {result.result_id: score},
        )
        await self._redis.expire(
            f"dq:results:{result.table_name}", settings.redis_results_ttl_seconds
        )

    # ------------------------------------------------------------------
    # Background loop
    # ------------------------------------------------------------------

    async def _publish_loop(self) -> None:
        while True:
            try:
                snapshot = await self._build_snapshot()
                payload = snapshot.model_dump_json()

                # Write latest snapshot under a well-known key
                await self._redis.set("dq:snapshot:latest", payload, ex=60)

                # Broadcast to all WebSocket subscribers
                await self._redis.publish(settings.redis_metrics_channel, payload)

                log.debug(
                    "snapshot_published",
                    overall_pass_rate=snapshot.overall_pass_rate,
                    active_blocks=len(snapshot.active_blocks),
                )
            except Exception as exc:
                log.error("publisher_error", error=str(exc))

            await asyncio.sleep(self._interval)

    async def _build_snapshot(self) -> MetricSnapshot:
        summary = self._collector.summary()
        active_blocks_raw = await self._job_controller.list_active_blocks()
        active_block_names = [b["job_name"] for b in active_blocks_raw]

        per_table_metrics = [
            QualityMetric(
                table_name=table,
                pass_rate=stats["avg_pass_rate"],
                total_batches=stats["total"],
                failed_batches=stats["failed"],
                avg_row_count=stats["avg_row_count"],
                avg_duration_ms=stats["avg_duration_ms"],
                active_blocks=sum(
                    1 for b in active_blocks_raw if b.get("table_name") == table
                ),
                checks_passed=0,   # enriched from DB if needed
                checks_failed=0,
            )
            for table, stats in summary.get("per_table", {}).items()
        ]

        self._collector.update_active_blocks(len(active_block_names))

        return MetricSnapshot(
            overall_pass_rate=summary.get("overall_pass_rate", 1.0),
            total_batches_last_hour=summary.get("total", 0),
            failed_batches_last_hour=summary.get("failed", 0),
            active_blocks=active_block_names,
            per_table=per_table_metrics,
        )
