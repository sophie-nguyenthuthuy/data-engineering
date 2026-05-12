# Deterministic Replay Engine with Causal Ordering

Replay any distributed pipeline event log deterministically — preserving causal ordering across producers — while detecting non-determinism in UDFs and exactly-once violations.

## Why this exists

Debugging exactly-once violations in distributed pipelines is hard:

- Events from multiple producers arrive out of physical order
- UDFs that call `random()` or `time.now()` produce different outputs on each replay
- Duplicate deliveries hide inside reprocessed windows or consumer-group rebalances

This engine takes a raw event log and replays it in a **total order consistent with the happens-before partial order** (via vector clocks + topological sort). It then runs each UDF multiple times per event and flags any divergence.

---

## Architecture

```
EventLog (JSONL / JSON)
       │
       ▼
 causal_order.py         ← vector-clock topological sort (Kahn's algorithm)
       │
       ▼
  replay.py              ← orchestration loop
   ├── udf_detector.py   ← runs each UDF N times, hashes outputs
   └── exactly_once.py   ← tracks duplicates, missing predecessors, out-of-order
       │
       ▼
  ReplayResult           ← ordered events + violation reports
```

### Key modules

| Module | Responsibility |
|---|---|
| `vector_clock.py` | Immutable vector clock with `happens_before`, `merge`, `compare` |
| `causal_order.py` | `causal_sort()` — Kahn topological sort with deterministic tie-breaking |
| `event.py` | `Event` dataclass + `EventLog` (JSONL / JSON I/O) |
| `udf_detector.py` | `UDFDetector` — wraps any `Event → Any` callable, runs it N times |
| `exactly_once.py` | `ExactlyOnceTracker` — flags `DUPLICATE_DELIVERY`, `MISSING_PREDECESSOR`, `OUT_OF_ORDER` |
| `replay.py` | `ReplayEngine` — ties everything together |
| `cli.py` | `replay-engine replay / validate` CLI |

---

## Quick start

```bash
pip install -e ".[dev]"

# Run the example pipelines
python examples/simple_pipeline.py
python examples/distributed_join.py
```

### Programmatic API

```python
from replay_engine import Event, EventLog, ReplayEngine, VectorClock

log = EventLog()
log.append(Event("a0", "A", 0, 1000.0, VectorClock({"A": 0}), {"val": 1}))
log.append(Event("b0", "B", 0, 1001.0, VectorClock({"A": 0, "B": 0}), {"val": 2}))

def transform(event: Event):
    return event.payload["val"] * 10

engine = ReplayEngine(udfs={"transform": transform}, udf_runs=3)
result = engine.replay(log)

print(result.summary())
# Replayed 2 events in 0.1 ms
# Exactly-once violations: 0
# UDF non-determinism violations: 0
```

### CLI

```bash
# Replay a JSONL log file
replay-engine replay events.jsonl

# Write ordered output and a JSON report
replay-engine replay events.jsonl -o ordered.jsonl -r report.json

# Validate causal structure only (no replay)
replay-engine validate events.jsonl
```

---

## Event log format

Events are JSON objects (one per line in JSONL, array in JSON):

```json
{
  "event_id":    "order-42",
  "producer_id": "orders-service",
  "sequence_num": 0,
  "timestamp":    1715000000.0,
  "vector_clock": {"orders-service": 0},
  "payload":      {"order_id": 42, "amount": 99.0}
}
```

**`vector_clock`** encodes causal dependencies: `{"A": 3, "B": 1}` means this event causally follows `A`'s event at sequence 3 and `B`'s event at sequence 1.

---

## UDF non-determinism detection

```python
from replay_engine.udf_detector import UDFDetector, NonDeterminismError
import random

def flaky(event):
    return random.random()           # reads global random state → non-deterministic

detector = UDFDetector("flaky", flaky, num_runs=2)

try:
    detector(event)
except NonDeterminismError as e:
    print(e.udf_name, e.event_id, e.run1, e.run2)

print(detector.report())
# {'udf_name': 'flaky', 'total_violations': 1, 'violation_event_ids': ['e0'], ...}
```

The detector computes a **SHA-256 content hash** of each event (excluding wall-clock timestamp) and runs the UDF `num_runs` times with that same input. Any output divergence is a violation.

---

## Exactly-once violation detection

The `ExactlyOnceTracker` processes events in causal order and flags:

| Violation | Meaning |
|---|---|
| `DUPLICATE_DELIVERY` | Same `event_id` tracked twice |
| `MISSING_PREDECESSOR` | Event depends on producer X at seq N, but X hasn't reached seq N yet |
| `OUT_OF_ORDER` | Producer's sequence numbers are not strictly consecutive |

---

## Causal ordering algorithm

1. For every pair `(A, B)` of events, compute `A.vector_clock.compare(B.vector_clock)`.
2. If `A < B` (A happened-before B), add directed edge `A → B`.
3. Run **Kahn's topological sort**; tie-break concurrent events by `(producer_id, sequence_num)` for full determinism regardless of input ordering.

This guarantees the same log always produces the same replay order.

---

## Running tests

```bash
pytest
pytest --cov=replay_engine --cov-report=term-missing
```

---

## Use cases

- **Debugging exactly-once pipelines** — feed a Kafka consumer's local log, replay it, spot the re-delivered messages
- **Regression testing UDFs** — wrap any transform with `UDFDetector` in CI to catch accidental non-determinism (e.g. reading `datetime.now()`)
- **Audit trails** — produce a canonical causally-ordered event sequence from multi-producer logs for compliance
- **Chaos / fault injection** — shuffle events, replay, verify the engine detects out-of-order delivery
