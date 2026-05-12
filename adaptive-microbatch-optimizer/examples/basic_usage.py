"""
Basic usage example — process simulated events through the adaptive processor.
"""

import asyncio
import logging
import random

from adaptive_microbatch import MicroBatchProcessor
from adaptive_microbatch.window_manager import SLAConfig

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")


async def my_handler(batch: list[dict]) -> None:
    """Simulate variable-cost downstream work."""
    cost = random.uniform(0.01, 0.15)
    await asyncio.sleep(cost)
    print(f"  Processed {len(batch):4d} events in {cost*1000:.0f}ms")


async def main() -> None:
    sla = SLAConfig(target_latency_s=0.08, min_throughput_eps=200.0)
    proc: MicroBatchProcessor[dict] = MicroBatchProcessor(
        handler=my_handler,
        sla=sla,
        initial_window=0.5,
    )

    await proc.start()

    for i in range(2000):
        await proc.ingest({"id": i, "value": random.random()})
        # Simulate bursty arrival
        await asyncio.sleep(random.expovariate(500))

        # Occasionally signal downstream backpressure
        if i % 200 == 0 and i > 0:
            pressure = random.uniform(0.6, 0.95)
            proc.report_backpressure("db-sink", pressure)
            print(f"\n[backpressure signal] db-sink={pressure:.2f}\n")

    await proc.stop(drain=True)
    stats = proc.stats()
    print(f"\nFinal stats: {stats}")


if __name__ == "__main__":
    asyncio.run(main())
