"""
What-if simulator: compare multiple watermark strategies on the same
historical stream and analyse the completeness vs. latency trade-off.

Run:
    python examples/whatif_simulation.py
"""
import sys, os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

import random
from src import Event
from src import (
    FixedLagWatermark, DynamicPerKeyWatermark, PercentileWatermark,
    TumblingWindow, SlidingWindow,
    DropPolicy, RestatePolicy, SideOutputPolicy,
)
from src.simulator import WhatIfComparator, ReplayConfig

random.seed(99)
BASE = 0.0
N = 500


def build_historical_stream() -> list[Event]:
    """
    Synthetic stream with three latency tiers:
      - 60 % of events: low latency (1–5 s)
      - 30 % of events: medium latency (10–60 s)
      - 10 % of events: high latency / stragglers (1–6 hours)
    """
    events = []
    for i in range(N):
        et = BASE + i * 1.0
        r = random.random()
        if r < 0.60:
            lat = random.uniform(1, 5)
        elif r < 0.90:
            lat = random.uniform(10, 60)
        else:
            lat = random.uniform(3600, 21600)  # 1–6 hours late

        key = f"sensor_{i % 5}"
        events.append(Event(
            event_time=et,
            key=key,
            value=round(random.gauss(20, 3), 2),
            processing_time=et + lat,
            sequence_id=i,
        ))
    return events


historical = build_historical_stream()
print(f"Historical stream: {len(historical)} events")
print()

# ── Define strategies to compare ──
comparator = WhatIfComparator(historical)

comparator.add(ReplayConfig(
    name="fixed_5s (drop)",
    watermark=FixedLagWatermark(lag_seconds=5),
    window=TumblingWindow(size_seconds=60),
    late_policy=DropPolicy(),
    description="Tight watermark, drop all late events",
))

comparator.add(ReplayConfig(
    name="fixed_60s (drop)",
    watermark=FixedLagWatermark(lag_seconds=60),
    window=TumblingWindow(size_seconds=60),
    late_policy=DropPolicy(),
    description="Medium watermark, drop all late events",
))

comparator.add(ReplayConfig(
    name="fixed_3600s (drop)",
    watermark=FixedLagWatermark(lag_seconds=3600),
    window=TumblingWindow(size_seconds=60),
    late_policy=DropPolicy(),
    description="1-hour watermark — catches most events but very high latency",
))

comparator.add(ReplayConfig(
    name="dynamic_p90 (drop)",
    watermark=DynamicPerKeyWatermark(percentile=90, window_size=50),
    window=TumblingWindow(size_seconds=60),
    late_policy=DropPolicy(),
    description="Per-key adaptive at p90",
))

comparator.add(ReplayConfig(
    name="dynamic_p95 (drop)",
    watermark=DynamicPerKeyWatermark(percentile=95, window_size=50),
    window=TumblingWindow(size_seconds=60),
    late_policy=DropPolicy(),
    description="Per-key adaptive at p95",
))

comparator.add(ReplayConfig(
    name="fixed_60s (restate)",
    watermark=FixedLagWatermark(lag_seconds=60),
    window=TumblingWindow(size_seconds=60),
    late_policy=RestatePolicy(max_lateness=7200),
    description="Medium watermark with restatement for late events (up to 2h late)",
))

comparator.add(ReplayConfig(
    name="fixed_60s (side-out)",
    watermark=FixedLagWatermark(lag_seconds=60),
    window=TumblingWindow(size_seconds=60),
    late_policy=SideOutputPolicy(),
    description="Medium watermark, route late events to side output",
))

# ── Run all strategies ──
print("Running what-if comparison...")
report = comparator.run()
print()

# ── Summary table ──
print("=" * 85)
print("COMPARATIVE RESULTS")
print("=" * 85)
print(report.summary_table())
print()

# ── Pareto analysis ──
print("=" * 85)
print("PARETO FRONTIER (completeness vs. avg output latency)")
print("=" * 85)
frontier = report.pareto_frontier()
for sr in frontier:
    print(
        f"  {sr.config.name:<25}  "
        f"completeness={sr.metrics.completeness:.2%}  "
        f"avg_latency={sr.metrics.avg_output_latency:.3f}s"
    )
print()

# ── Best by metric ──
best_complete = report.best_by("completeness")
best_latency  = report.best_by("avg_output_latency")
print(f"Best completeness : {best_complete.config.name}  "
      f"({best_complete.metrics.completeness:.2%})")
print(f"Best latency      : {best_latency.config.name}  "
      f"({best_latency.metrics.avg_output_latency:.3f}s)")
print()

# ── Per-strategy detail ──
print("=" * 85)
print("PER-STRATEGY DETAIL")
print("=" * 85)
print(report.per_strategy_detail())
