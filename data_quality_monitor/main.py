"""
Data Quality Monitor — main entry point.

Usage:
    # Run the stream validator + dashboard together
    python main.py

    # Run only the dashboard (useful in multi-process deployments)
    python main.py --mode dashboard

    # Run only the stream consumer / validator
    python main.py --mode consumer
"""
from __future__ import annotations
import argparse
import asyncio
import signal
import sys

import redis.asyncio as aioredis
import structlog
import uvicorn

from src.config import settings
from src.storage.repository import ValidationRepository
from src.stream.consumer import KafkaBatchConsumer
from src.stream.producer import KafkaResultProducer
from src.metrics.collector import MetricsCollector
from src.metrics.publisher import MetricsPublisher
from src.blocking.job_controller import JobController
from src.pipeline.micro_batch_processor import MicroBatchProcessor
from src.dashboard.api import create_app

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------

structlog.configure(
    processors=[
        structlog.contextvars.merge_contextvars,
        structlog.processors.add_log_level,
        structlog.processors.TimeStamper(fmt="iso"),
        structlog.dev.ConsoleRenderer()
        if settings.log_format == "console"
        else structlog.processors.JSONRenderer(),
    ],
    wrapper_class=structlog.make_filtering_bound_logger(
        getattr(__import__("logging"), settings.log_level)
    ),
    logger_factory=structlog.PrintLoggerFactory(),
)

log = structlog.get_logger(__name__)


# ---------------------------------------------------------------------------
# Consumer loop
# ---------------------------------------------------------------------------

async def run_consumer(
    processor: MicroBatchProcessor,
    consumer: KafkaBatchConsumer,
) -> None:
    log.info("consumer_loop_started", topic=settings.kafka_input_topic)
    async for batch in consumer.stream_batches():
        try:
            result = await processor.process(batch)
            log.info(
                "batch_processed",
                batch_id=batch.batch_id,
                status=result.status,
                pass_rate=f"{result.pass_rate:.2%}",
                duration_ms=f"{result.duration_ms:.1f}",
            )
        except Exception as exc:
            log.error("batch_processing_failed", batch_id=batch.batch_id, error=str(exc))


# ---------------------------------------------------------------------------
# Bootstrap
# ---------------------------------------------------------------------------

async def main(mode: str) -> None:
    # Shared infrastructure
    redis_client = aioredis.from_url(settings.redis_url, decode_responses=False)
    repo = ValidationRepository()
    await repo.init_db()

    job_controller = JobController(redis_client)
    collector = MetricsCollector()
    publisher = MetricsPublisher(redis_client, collector, job_controller)

    if mode in ("consumer", "both"):
        producer = KafkaResultProducer()
        processor = MicroBatchProcessor(
            repository=repo,
            producer=producer,
            collector=collector,
            publisher=publisher,
            job_controller=job_controller,
        )
        consumer = KafkaBatchConsumer()

    # Graceful shutdown
    loop = asyncio.get_running_loop()
    stop_event = asyncio.Event()

    def _signal_handler():
        log.info("shutdown_signal_received")
        stop_event.set()

    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, _signal_handler)

    tasks: list[asyncio.Task] = []

    if mode in ("consumer", "both"):
        await publisher.start()
        tasks.append(asyncio.create_task(run_consumer(processor, consumer)))

    if mode in ("dashboard", "both"):
        app = create_app(repo, redis_client, job_controller, collector)
        config = uvicorn.Config(
            app,
            host=settings.dashboard_host,
            port=settings.dashboard_port,
            log_level=settings.log_level.lower(),
        )
        server = uvicorn.Server(config)
        tasks.append(asyncio.create_task(server.serve()))

    log.info("data_quality_monitor_running", mode=mode)

    # Wait until a stop signal or a task raises
    done, pending = await asyncio.wait(
        [asyncio.create_task(stop_event.wait()), *tasks],
        return_when=asyncio.FIRST_COMPLETED,
    )

    log.info("shutdown_initiated")
    for task in pending:
        task.cancel()
    await asyncio.gather(*pending, return_exceptions=True)

    # Cleanup
    if mode in ("consumer", "both"):
        await consumer.close()
        producer.close()
        await publisher.stop()

    await repo.close()
    await redis_client.aclose()
    log.info("shutdown_complete")


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Data Quality Monitor")
    parser.add_argument(
        "--mode",
        choices=["consumer", "dashboard", "both"],
        default="both",
        help="Which components to run (default: both)",
    )
    args = parser.parse_args()
    asyncio.run(main(args.mode))
