"""
Throughput and latency benchmark for the stream processor.

Run:
    python benchmarks/latency_benchmark.py
"""
import sys, os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

import time
import random
import statistics

from src import Event, StreamProcessor, FixedLagWatermark, DynamicPerKeyWatermark
from src import TumblingWindow, SlidingWindow
from src.policies import DropPolicy

random.seed(0)

SIZES = [1_000, 10_000, 100_000]
LAG = 5.0
WINDOW = 60.0
BASE = 1_700_000_000.0


def make_events(n: int) -> list[Event]:
    evts = []
    for i in range(n):
        et = BASE + random.uniform(0, n)
        pt = et + random.uniform(1, LAG * 2)
        evts.append(Event(event_time=et, key=f"k{i%10}", value=i,
                           processing_time=pt, sequence_id=i))
    evts.sort(key=lambda e: e.processing_time)
    return evts


def benchmark(name: str, n: int, processor_factory):
    events = make_events(n)
    times = []
    RUNS = 3
    for _ in range(RUNS):
        p = processor_factory()
        t0 = time.perf_counter()
        for e in events:
            p.process(e)
        p.flush()
        times.append(time.perf_counter() - t0)
    mean_ms = statistics.mean(times) * 1000
    throughput = n / statistics.mean(times)
    print(f"  {name:<35} n={n:>7}  mean={mean_ms:>8.1f}ms  "
          f"throughput={throughput:>10,.0f} events/s")


print("Stream Processor Throughput Benchmark")
print("=" * 80)

for n in SIZES:
    print(f"\nn={n:,}")
    benchmark(
        "FixedLag + Tumbling + Drop",
        n,
        lambda: StreamProcessor(
            FixedLagWatermark(LAG),
            TumblingWindow(WINDOW),
            DropPolicy(),
        ),
    )
    benchmark(
        "Dynamic(p95) + Tumbling + Drop",
        n,
        lambda: StreamProcessor(
            DynamicPerKeyWatermark(95, window_size=50),
            TumblingWindow(WINDOW),
            DropPolicy(),
        ),
    )
    benchmark(
        "FixedLag + Sliding(120s/60s) + Drop",
        n,
        lambda: StreamProcessor(
            FixedLagWatermark(LAG),
            SlidingWindow(120, 60),
            DropPolicy(),
        ),
    )

print()
print("Done.")
