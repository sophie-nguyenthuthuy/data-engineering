"""
Fan-out topology demo
─────────────────────
                         ┌→ SinkA (fast)
ProducerA ─→ FanOutHub ─┤
ProducerB ─/             └→ SinkB (degrades at t=5s)

When SinkB degrades, only FanOutHub and ProducerB are throttled.
ProducerA (feeding a separate path that only reaches SinkA) is unaffected.

Run:
    python -m examples.fan_out_topology
"""
from __future__ import annotations

import asyncio
import logging
import sys

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
logger = logging.getLogger("fan-out-demo")


async def print_status(jobs, coordinator, interval=2.0):
    while True:
        await asyncio.sleep(interval)
        print("\n┌─ Fan-Out Mesh Status ───────────────────────────────────────┐")
        for job in jobs:
            m = job.get_metrics()
            factor = job.throttle._factor
            print(
                f"│ {job.job_id:<20} in={m.records_in_per_sec:>6.0f}/s "
                f"throttle={factor:.2f} │"
            )
        print(f"│ Active throttles: {coordinator.active_throttles}")
        print("└─────────────────────────────────────────────────────────────┘")


async def main():
    q_a = asyncio.Queue(maxsize=1000)
    q_b = asyncio.Queue(maxsize=1000)
    q_hub_to_sink_a = asyncio.Queue(maxsize=500)
    q_hub_to_sink_b = asyncio.Queue(maxsize=500)

    producer_a = ProducerJob("producer_a", source_rate=400.0, downstream_queue=q_a)
    producer_b = ProducerJob("producer_b", source_rate=400.0, downstream_queue=q_b)

    # FanOutHub reads from both producers and routes to two sinks
    # Simulated here as two separate transform jobs sharing the same hub queues
    hub_a = TransformJob("hub_to_sink_a", upstream_queue=q_a,
                         downstream_queue=q_hub_to_sink_a, processing_delay_ms=0.5)
    hub_b = TransformJob("hub_to_sink_b", upstream_queue=q_b,
                         downstream_queue=q_hub_to_sink_b, processing_delay_ms=0.5)

    sink_a = SinkJob("sink_a", upstream_queue=q_hub_to_sink_a, sink_rate=500.0)
    sink_b = SinkJob("sink_b", upstream_queue=q_hub_to_sink_b, sink_rate=500.0)

    # ── Topology ──────────────────────────────────────────────────────────────
    topo = PipelineTopology()
    for jid in ["producer_a", "producer_b", "hub_to_sink_a", "hub_to_sink_b", "sink_a", "sink_b"]:
        topo.add_job(jid)
    topo.add_edge("producer_a", "hub_to_sink_a")
    topo.add_edge("producer_b", "hub_to_sink_b")
    topo.add_edge("hub_to_sink_a", "sink_a")
    topo.add_edge("hub_to_sink_b", "sink_b")

    # ── Mesh ──────────────────────────────────────────────────────────────────
    bus = InMemoryBus()
    coordinator = BackpressureCoordinator(bus, topo, reconcile_interval=2.0)

    all_jobs = [producer_a, producer_b, hub_a, hub_b, sink_a, sink_b]
    job_map = {j.job_id: j for j in all_jobs}

    sidecars = []
    for job in all_jobs:
        s = JobSidecar(
            job.job_id, bus, job.get_metrics, job.throttle,
            poll_interval=0.5, pressure_threshold=0.10,
        )
        sidecars.append(s)

    await coordinator.start()
    for s in sidecars:
        await s.start()
    for j in all_jobs:
        await j.start()

    status_task = asyncio.create_task(print_status(all_jobs, coordinator))

    logger.info("t=0  Both pipelines running at full rate")
    await asyncio.sleep(5)

    logger.info("t=5  >>> sink_b degrades — only producer_b path should throttle <<<")
    sink_b.degrade(degraded_rate_multiplier=0.05)
    await asyncio.sleep(12)

    logger.info("t=17 >>> sink_b recovers <<<")
    sink_b.recover()
    await asyncio.sleep(6)

    logger.info("t=23 Demo complete")
    status_task.cancel()
    for j in all_jobs:
        await j.stop()
    for s in sidecars:
        await s.stop()
    await coordinator.stop()
    await bus.close()


if __name__ == "__main__":
    asyncio.run(main())
