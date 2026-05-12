"""
Three-stage pipeline demo
─────────────────────────
ProducerJob → TransformJob → SinkJob
   (fast)        (medium)      (slow, degrades at t=5s, recovers at t=15s)

Watch the coordinator throttle the Producer and Transform jobs when the Sink
degrades, then release the throttle after recovery — all without touching job code.

Run:
    python -m examples.three_stage_pipeline
"""
from __future__ import annotations

import asyncio
import logging
import sys
import time

sys.path.insert(0, ".")

from jobs import ProducerJob, SinkJob, TransformJob
from mesh import (
    BackpressureCoordinator,
    InMemoryBus,
    JobSidecar,
    PipelineTopology,
    TokenBucketThrottle,
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)-8s %(name)s — %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger("demo")


async def print_status(jobs, coordinator, interval=2.0):
    while True:
        await asyncio.sleep(interval)
        print("\n┌─ Mesh Status ──────────────────────────────────────────────┐")
        for job in jobs:
            m = job.get_metrics()
            factor = job.throttle._factor
            print(
                f"│ {job.job_id:<20} in={m.records_in_per_sec:>6.0f}/s "
                f"out={m.records_out_per_sec:>6.0f}/s "
                f"q={m.input_utilization:>4.0%} "
                f"throttle={factor:.2f} │"
            )
        pressures = coordinator.active_pressure
        throttles = coordinator.active_throttles
        print(f"│ Pressure: {pressures}")
        print(f"│ Throttles: {throttles}")
        print("└────────────────────────────────────────────────────────────┘")


async def main():
    # ── Shared queues (simulate Kafka topics) ────────────────────────────────
    q_prod_to_xform: asyncio.Queue = asyncio.Queue(maxsize=1000)
    q_xform_to_sink: asyncio.Queue = asyncio.Queue(maxsize=500)

    # ── Jobs (no backpressure awareness inside) ───────────────────────────────
    producer = ProducerJob("producer", source_rate=800.0, downstream_queue=q_prod_to_xform)
    transform = TransformJob("transform", upstream_queue=q_prod_to_xform,
                             downstream_queue=q_xform_to_sink, processing_delay_ms=1.0)
    sink = SinkJob("sink", upstream_queue=q_xform_to_sink, sink_rate=600.0)

    # ── Topology ──────────────────────────────────────────────────────────────
    topo = PipelineTopology.linear("producer", "transform", "sink")

    # ── Mesh ──────────────────────────────────────────────────────────────────
    bus = InMemoryBus()
    coordinator = BackpressureCoordinator(bus, topo, reconcile_interval=2.0)

    sidecars = [
        JobSidecar("producer", bus, producer.get_metrics, producer.throttle, poll_interval=1.0),
        JobSidecar("transform", bus, transform.get_metrics, transform.throttle, poll_interval=1.0),
        JobSidecar("sink", bus, sink.get_metrics, sink.throttle, poll_interval=0.5,
                   pressure_threshold=0.10),
    ]

    # ── Start everything ──────────────────────────────────────────────────────
    await coordinator.start()
    for s in sidecars:
        await s.start()
    for j in [producer, transform, sink]:
        await j.start()

    status_task = asyncio.create_task(print_status([producer, transform, sink], coordinator))

    # ── Scenario ──────────────────────────────────────────────────────────────
    logger.info("t=0  Pipeline running normally")
    await asyncio.sleep(5)

    logger.info("t=5  >>> Sink degrading (simulating DB overload) <<<")
    sink.degrade(degraded_rate_multiplier=0.05)
    await asyncio.sleep(12)

    logger.info("t=17 >>> Sink recovering <<<")
    sink.recover()
    await asyncio.sleep(8)

    logger.info("t=25 Demo complete — shutting down")
    status_task.cancel()
    for j in [producer, transform, sink]:
        await j.stop()
    for s in sidecars:
        await s.stop()
    await coordinator.stop()
    await bus.close()


if __name__ == "__main__":
    asyncio.run(main())
