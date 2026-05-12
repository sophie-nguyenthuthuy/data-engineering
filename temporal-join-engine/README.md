# Temporal Join Engine

AS OF temporal joins between two out-of-order event streams, with per-key interval trees and automatic late-arrival corrections.

## What it does

```
left  stream (probe):   readings, clicks, transactions …
right stream (build):   enrichment events, price updates, firmware versions …

For each left event L at time T_l with key K, emit:
    (L, R*)  where R* = latest right event with R.key = K and T_l − W ≤ R.event_time ≤ T_l
```

When a **late right event** arrives out-of-order but still within the configured lateness window, the engine automatically detects which already-emitted results are now stale and emits corrections:

```
RETRACT  (L, R_old)   ← withdraw the previously emitted result
EMIT     (L, R_new)   ← corrected result with the better match
```

## Core concepts

| Concept | Description |
|---|---|
| **AS OF join** | For each left event find the latest right event that precedes it (within a lookback window). |
| **Watermark** | `max_event_time_seen − lateness_bound`. Events older than the watermark are dropped. |
| **Reclaimably late** | Event is out-of-order but still within the lateness budget → eligible for corrections. |
| **Irreparably late** | Event is older than the watermark → discarded. |
| **IntervalTree** | Augmented AVL BST (per key) supporting O(log n) predecessor and range queries. |

## Architecture

```
src/temporal_join/
├── interval_tree.py   Augmented AVL BST — insert/delete/predecessor/range in O(log n)
├── event.py           Event and JoinResult dataclasses
├── watermark.py       Per-stream watermark tracking
└── join_engine.py     AsOfJoinEngine — wires everything together
```

## Quick start

```python
from temporal_join import AsOfJoinEngine, Event, STREAM_LEFT, STREAM_RIGHT

engine = AsOfJoinEngine(
    lookback_window=30_000,      # look back up to 30 s on the right stream
    left_lateness_bound=5_000,   # left events may arrive up to 5 s late
    right_lateness_bound=15_000, # right events may arrive up to 15 s late
)

# Right events go into the build-side interval tree (per key)
engine.process_event(Event("user-42", event_time=1_000, stream_id=STREAM_RIGHT, payload={"tier": "gold"}))

# Left event is probed against the build side
results = engine.process_event(Event("user-42", event_time=5_000, stream_id=STREAM_LEFT, payload={"action": "buy"}))
# → [JoinResult(EMIT key='user-42' L@5000 ⋈ R@1000)]

# Advance frontier so a late right event is reclaimably late
engine.process_event(Event("user-42", event_time=20_000, stream_id=STREAM_RIGHT))

# Late right event: arrives now but has event_time=3000 (between R@1000 and L@5000)
corrections = engine.process_event(Event("user-42", event_time=3_000, stream_id=STREAM_RIGHT, payload={"tier": "platinum"}))
# → [JoinResult(RETRACT … L@5000 ⋈ R@1000),
#    JoinResult(EMIT    … L@5000 ⋈ R@3000)]
```

## Late-arrival correction algorithm

```
When right event R' arrives at T_r (reclaimably late):
  1. Candidate left events: all L where T_r ≤ T_l ≤ T_r + lookback_window
     (range query on the correction index)
  2. For each L_i with current match R_old:
       if T_r > R_old.event_time (or R_old is None):
         emit RETRACT(L_i, R_old)
         emit EMIT(L_i, R')
         update stored match to R'
```

## Running tests

```bash
pip install -e ".[dev]"
pytest
```

## Running the demo

```bash
python examples/demo.py
```
