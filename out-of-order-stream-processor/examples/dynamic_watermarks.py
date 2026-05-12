"""
Dynamic per-key watermarks: each key self-tunes its lag based on observed
ingestion latency distribution.

Run:
    python examples/dynamic_watermarks.py
"""
import sys, os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

import random
from src import (
    Event, StreamProcessor,
    DynamicPerKeyWatermark, PercentileWatermark,
    TumblingWindow, SideOutputPolicy,
)

random.seed(7)
BASE = 1_700_000_000.0

def gen_key_stream(key, n, base_lag, jitter):
    """Generate events for one key with a given latency profile."""
    evts = []
    for i in range(n):
        et = BASE + i * 2
        lat = max(0.1, random.gauss(base_lag, jitter))
        pt = et + lat
        evts.append(Event(event_time=et, key=key, value=i,
                           processing_time=pt, sequence_id=i))
    return evts

# Three keys with very different latency profiles
streams = {
    "reliable":  gen_key_stream("reliable",  200, base_lag=2,    jitter=0.5),
    "variable":  gen_key_stream("variable",  200, base_lag=30,   jitter=15),
    "stragglers":gen_key_stream("stragglers",200, base_lag=120,  jitter=60),
}

all_events = []
for evts in streams.values():
    all_events.extend(evts)
all_events.sort(key=lambda e: e.processing_time)

# ── Dynamic per-key watermark ──
dynamic_wm = DynamicPerKeyWatermark(percentile=95, window_size=50, min_lag=1.0)
processor = StreamProcessor(
    watermark=dynamic_wm,
    window=TumblingWindow(size_seconds=60),
    late_policy=SideOutputPolicy(),
)

for event in all_events:
    processor.process(event)
processor.flush()

print("Dynamic Per-Key Watermark — Learned Lags")
print("─" * 50)
for key, info in dynamic_wm.stats_summary().items():
    print(
        f"  {key:<12}  samples={info['samples']:>4}  "
        f"p95_lag={info['p95_lag']:>7.2f}s  "
        f"effective_lag={info['effective_lag']:>7.2f}s"
    )

print()
s = processor.stats()
print(f"Late events : {s['late_total']}")
print(f"Side output : {len(processor.late_policy.side_output)}")  # type: ignore

# ── Comparison: global percentile watermark ──
from src import PercentileWatermark
global_wm = PercentileWatermark(percentile=95, window_size=200)
proc2 = StreamProcessor(
    watermark=global_wm,
    window=TumblingWindow(size_seconds=60),
    late_policy=SideOutputPolicy(),
)
for event in all_events:
    proc2.process(event)
proc2.flush()

print()
print("Global Percentile Watermark (p95)")
print("─" * 50)
s2 = proc2.stats()
print(f"Lag used    : {global_wm.current_lag:.2f}s")
print(f"Late events : {s2['late_total']}")
print(f"Side output : {len(proc2.late_policy.side_output)}")  # type: ignore

print()
print("Per-key dynamic strategy lets 'reliable' have a much tighter")
print("watermark than 'stragglers', independently reducing output latency.")
