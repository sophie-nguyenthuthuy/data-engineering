"""
Basic usage: tumbling windows with a fixed-lag watermark.

Run:
    python examples/basic_usage.py
"""
import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

import time
from src import (
    Event, StreamProcessor,
    FixedLagWatermark, TumblingWindow,
    SideOutputPolicy,
)

# ── Build events simulating a 5-minute sensor stream with 10s ingestion lag ──
BASE_TIME = 1_700_000_000.0
WINDOW_SIZE = 60   # 1-minute windows
LAG = 10           # 10-second fixed lag

events = []
for i in range(300):
    event_time = BASE_TIME + i
    processing_time = event_time + LAG + (30 if i % 50 == 0 else 0)  # occasional stragglers
    events.append(Event(
        event_time=event_time,
        key=f"sensor_{i % 3}",
        value=round(20 + (i % 10) * 0.5, 2),
        processing_time=processing_time,
        sequence_id=i,
    ))

# Shuffle to simulate out-of-order delivery
import random; random.seed(42)
random.shuffle(events)
# Re-sort by processing_time (order in which events arrive at the processor)
events.sort(key=lambda e: e.processing_time)

# ── Set up the processor ──
policy = SideOutputPolicy()
processor = StreamProcessor(
    watermark=FixedLagWatermark(lag_seconds=LAG),
    window=TumblingWindow(size_seconds=WINDOW_SIZE),
    late_policy=policy,
)

print("Processing stream...")
print("─" * 60)

for event in events:
    results, lates = processor.process(event)
    for r in results:
        avg_val = sum(r.values) / r.count if r.count else 0
        print(
            f"  Window [{r.window_start - BASE_TIME:.0f}s, "
            f"{r.window_end - BASE_TIME:.0f}s) key={r.key}  "
            f"count={r.count}  avg={avg_val:.2f}"
            + (" [RESTATEMENT]" if r.is_restatement else "")
        )

# Flush remaining windows
for r in processor.flush():
    avg_val = sum(r.values) / r.count if r.count else 0
    print(
        f"  Window [{r.window_start - BASE_TIME:.0f}s, "
        f"{r.window_end - BASE_TIME:.0f}s) key={r.key}  "
        f"count={r.count}  avg={avg_val:.2f}  [FLUSHED]"
    )

print("─" * 60)
s = processor.stats()
print(f"Total processed : {s['processed']}")
print(f"Windows emitted : {s['emitted_windows']}")
print(f"Late events     : {s['late_total']}")
side = policy.side_output
print(f"Side output     : {len(side)} event(s)")
