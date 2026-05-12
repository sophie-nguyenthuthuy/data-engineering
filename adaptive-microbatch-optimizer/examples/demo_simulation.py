"""
Extended simulation with three phases:

  Phase 1 — normal load, PID converges to a stable window
  Phase 2 — latency spike (downstream slows down), window shrinks
  Phase 3 — heavy backpressure, window floor kicks in
  Phase 4 — recovery

Prints a rolling ASCII dashboard every second.
"""

import asyncio
import logging
import math
import random
import sys
import time

sys.path.insert(0, "..")

from adaptive_microbatch import MicroBatchProcessor
from adaptive_microbatch.window_manager import SLAConfig

logging.basicConfig(level=logging.WARNING)

PHASES = [
    {"name": "Normal load",         "duration": 6,  "cost_mean": 0.05, "bp": 0.0},
    {"name": "Latency spike",       "duration": 6,  "cost_mean": 0.25, "bp": 0.1},
    {"name": "Heavy backpressure",  "duration": 6,  "cost_mean": 0.10, "bp": 0.9},
    {"name": "Recovery",            "duration": 6,  "cost_mean": 0.04, "bp": 0.0},
]

_phase_cost = 0.05
_phase_bp   = 0.0


async def handler(batch: list[int]) -> None:
    jitter = random.gauss(0, _phase_cost * 0.2)
    await asyncio.sleep(max(0.001, _phase_cost + jitter))


async def dashboard_loop(proc: MicroBatchProcessor, stop_event: asyncio.Event) -> None:
    bar_width = 40
    while not stop_event.is_set():
        await asyncio.sleep(1.0)
        stats = proc.stats()
        snap = proc.metrics.latency_snapshot()
        tput = proc.metrics.throughput_snapshot()
        bp   = proc.backpressure.current_level()

        w = stats.current_window_s
        bar_fill = int((w - 0.05) / (5.0 - 0.05) * bar_width)
        bar = "█" * bar_fill + "░" * (bar_width - bar_fill)

        p95  = f"{snap.p95*1000:6.1f}ms" if snap else "   n/a  "
        eps  = f"{tput.events_per_second:7.1f}" if tput else "    n/a"

        print(
            f"\r  window [{bar}] {w*1000:6.0f}ms  "
            f"p95={p95}  tput={eps} eps  bp={bp:.2f}  "
            f"batches={stats.total_batches:4d}  dropped={stats.dropped_events}",
            end="",
            flush=True,
        )


async def main() -> None:
    global _phase_cost, _phase_bp

    sla = SLAConfig(target_latency_s=0.08, min_throughput_eps=50.0, backpressure_weight=0.6)
    proc: MicroBatchProcessor[int] = MicroBatchProcessor(
        handler=handler,
        sla=sla,
        initial_window=0.5,
    )
    await proc.start()

    stop_dash = asyncio.Event()
    dash_task = asyncio.create_task(dashboard_loop(proc, stop_dash))

    event_id = 0
    for phase in PHASES:
        _phase_cost = phase["cost_mean"]
        _phase_bp   = phase["bp"]
        print(f"\n\n=== Phase: {phase['name']} (cost~{_phase_cost*1000:.0f}ms, bp={_phase_bp}) ===")

        end = time.monotonic() + phase["duration"]
        while time.monotonic() < end:
            await proc.ingest(event_id)
            event_id += 1
            if _phase_bp > 0:
                proc.report_backpressure("downstream-worker", _phase_bp + random.uniform(-0.05, 0.05))
            await asyncio.sleep(random.expovariate(300))

    stop_dash.set()
    await dash_task
    await proc.stop(drain=True)

    stats = proc.stats()
    print(f"\n\nSimulation complete.")
    print(f"  Total events : {stats.total_events}")
    print(f"  Total batches: {stats.total_batches}")
    print(f"  Dropped      : {stats.dropped_events}")
    print(f"  Uptime       : {stats.uptime_s:.1f}s")
    print(f"  Final window : {stats.current_window_s*1000:.0f}ms")


if __name__ == "__main__":
    asyncio.run(main())
