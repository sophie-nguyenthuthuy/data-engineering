# Out-of-Order Event-Time Stream Processor with Dynamic Watermarks

A pure-Python stream processing engine that handles events arriving arbitrarily late (minutes, hours, days) using pluggable watermark strategies, multiple windowing modes, and a **what-if simulator** for comparing strategies on historical data.

## Features

### Watermark Strategies

| Strategy | Description |
|---|---|
| `FixedLagWatermark` | Classic `max_event_time − lag`. Simple, predictable. |
| `DynamicPerKeyWatermark` | Per-key lag derived from a sliding percentile of observed ingestion latencies. Tight for reliable producers, loose for stragglers — independently. |
| `PercentileWatermark` | Global percentile watermark; adapts to changing stream latency without per-key bookkeeping. |

### Windowing Strategies

| Window | Description |
|---|---|
| `TumblingWindow(size)` | Non-overlapping fixed-size windows. Each event falls in exactly one window. |
| `SlidingWindow(size, slide)` | Overlapping windows. An event may belong to `ceil(size/slide)` windows. |
| `SessionWindow(gap)` | Gap-based sessions. Windows grow and merge as events arrive; no fixed size. |

### Late-Data Policies

| Policy | Behaviour |
|---|---|
| `DropPolicy` | Discard the event, record a `LateEvent` for audit. |
| `RestatePolicy(max_lateness)` | Reopen the window, add the event, emit a corrected `WindowResult` marked `is_restatement=True`. |
| `SideOutputPolicy` | Route to a side-output collection for downstream handling (dead-letter queue, manual review, etc.). |

### What-If Simulator

Replay a fixed historical event stream through multiple strategy combinations and get a comparative report:

- **Completeness** — fraction of events captured in at least one emitted window
- **Avg output latency** — mean time from `window_end` to result emission
- **Pareto frontier** — strategies non-dominated in completeness × latency space
- **Per-strategy detail** — late counts, drops, restatements, wall time

---

## Installation

```bash
git clone https://github.com/<you>/out-of-order-stream-processor.git
cd out-of-order-stream-processor
pip install -r requirements.txt
```

No heavy dependencies — only `pytest` for testing.

---

## Quick Start

```python
from src import (
    Event, StreamProcessor,
    DynamicPerKeyWatermark, TumblingWindow, SideOutputPolicy,
)

processor = StreamProcessor(
    watermark=DynamicPerKeyWatermark(percentile=95, window_size=50),
    window=TumblingWindow(size_seconds=60),
    late_policy=SideOutputPolicy(),
)

# Feed events (sorted by processing_time, i.e. arrival order)
for event in my_stream:
    results, late_records = processor.process(event)
    for r in results:
        print(r)   # WindowResult emitted when window closes

# Flush remaining open windows at end of stream
for r in processor.flush():
    print(r)

print(processor.stats())
```

---

## Examples

```bash
# Basic tumbling-window processing with side-output for late events
python examples/basic_usage.py

# Per-key dynamic watermarks vs. global percentile
python examples/dynamic_watermarks.py

# What-if simulator: compare 7 strategies on one historical stream
python examples/whatif_simulation.py
```

### What-if output (sample)

```
COMPARATIVE RESULTS
=====================================================================================
Strategy                  Completeness   Avg Latency    Late   Dropped  Restatements
-------------------------------------------------------------------------------------
fixed_5s (drop)              83.20%         4.988s      84        84             0
fixed_60s (drop)             89.20%        59.984s      54        54             0
fixed_3600s (drop)           97.40%      3599.991s      13        13             0
dynamic_p90 (drop)           88.00%        14.321s      60        60             0
dynamic_p95 (drop)           89.40%        28.437s      53        53             0
fixed_60s (restate)          89.20%        59.984s      54         0            54
fixed_60s (side-out)         89.20%        59.984s      54         0             0
-------------------------------------------------------------------------------------
* Completeness = fraction of events captured in at least one emitted window result

PARETO FRONTIER (completeness vs. avg output latency)
  fixed_5s (drop)          completeness=83.20%  avg_latency=4.988s
  dynamic_p95 (drop)       completeness=89.40%  avg_latency=28.437s
  fixed_3600s (drop)       completeness=97.40%  avg_latency=3599.991s
```

---

## Running Tests

```bash
pytest                    # all 49 tests
pytest -v --tb=short      # verbose
pytest tests/test_simulator.py   # just simulator tests
```

---

## Running the Benchmark

```bash
python benchmarks/latency_benchmark.py
```

Measures throughput (events/s) for fixed-lag, dynamic, and sliding-window configurations at 1k / 10k / 100k event scales.

---

## Architecture

```
src/
├── event.py              # Event, WindowResult, LateEvent dataclasses
├── processor.py          # StreamProcessor — core processing loop
├── watermarks/
│   ├── base.py           # Watermark ABC
│   ├── fixed.py          # FixedLagWatermark
│   ├── dynamic.py        # DynamicPerKeyWatermark
│   └── percentile.py     # PercentileWatermark
├── windows/
│   ├── base.py           # Window ABC, WindowAssignment
│   ├── tumbling.py       # TumblingWindow
│   ├── sliding.py        # SlidingWindow
│   └── session.py        # SessionWindow + merge logic
├── policies/
│   ├── base.py           # LateDataPolicy ABC
│   ├── drop.py           # DropPolicy
│   ├── restate.py        # RestatePolicy
│   └── side_output.py    # SideOutputPolicy
└── simulator/
    ├── replay.py         # StreamReplay, ReplayConfig, ReplayMetrics
    └── comparator.py     # WhatIfComparator, ComparisonReport
```

### Key design decisions

**Why per-key watermarks?**  
A single global watermark is bottlenecked by the slowest key. A service that mixes high-frequency reliable sensors with occasional batch uploads should not penalise all sensors because one upload is slow. `DynamicPerKeyWatermark` gives each key an independent, data-driven lag; the global watermark is `min(per-key watermarks)` so correctness guarantees are preserved.

**Why strict `<` for lateness?**  
An event whose `event_time` exactly equals the current watermark set the watermark (or arrived at the exact boundary). Treating it as late would mean the very event that advanced the watermark is discarded — incorrect. Using `event_time < watermark` is the standard Beam/Flink semantics.

**Session window merging**  
Session boundaries are not known in advance. The processor maintains per-key lists of provisional windows `[event_time, event_time + gap)` and merges overlapping intervals on every new event using `SessionWindow.merge()`. When the watermark passes a merged session's end, it is emitted as a single `WindowResult`.

**Restatements**  
`RestatePolicy` marks corrected results with `is_restatement=True`. Downstream consumers keyed on `(key, window_start, window_end)` can upsert rather than append, achieving eventual consistency without requiring a two-phase commit protocol.

---

## License

MIT
