# cep-engine

Sub-millisecond in-process **Complex Event Processing** engine.

Events land in a **shared-memory ring buffer** and pattern matchers run as
**Numba-JIT compiled state machines** — no Kafka, no network, no GC pressure
on the hot path.

---

## Architecture

```
              push(event)
                  │
        ┌─────────▼──────────┐
        │   RingBuffer        │  mmap-backed, power-of-2 capacity
        │   (32-byte events)  │  single-writer, lock-free reads
        └─────────┬───────────┘
                  │ parallel fan-out
        ┌─────────▼──────────┐   ┌──────────────────────┐
        │ CompiledPattern[0]  │   │ CompiledPattern[N]   │
        │  Numba @njit NFA    │…  │  Numba @njit NFA     │
        │  per-entity arrays  │   │  per-entity arrays   │
        └─────────┬───────────┘   └──────────┬───────────┘
                  └──── on match ────▶ callbacks
```

**Event layout** — 32 bytes, fits two per cache line:

| Field       | Type    | Notes                          |
|-------------|---------|--------------------------------|
| timestamp   | int64   | Unix nanoseconds               |
| type_id     | int32   | Registered event type          |
| entity_id   | int64   | User / account / session / IP  |
| value       | float64 | Numeric payload                |
| flags       | uint32  | Bitmask for categorical attrs  |

**JIT compilation** — each `Pattern` is code-generated to a Python source
string describing a tight NFA transition function, then compiled with
`numba.njit(cache=True)`.  The first call triggers Numba compilation (hundreds
of milliseconds); all subsequent calls execute native code.

**Pattern NFA** — per-entity state is three flat int64/int8/int32 arrays
(step, start_ts, last_ts, count).  No Python objects on the matching hot path.

---

## Install

```bash
pip install "cep-engine[jit]"   # includes Numba
# or
pip install cep-engine           # pure-Python fallback, ~10x slower
```

---

## Quick start

```python
from cep import CEPEngine, Pattern, make_event
import time

class E:
    LOGIN_FAILURE  = 2
    PASSWORD_RESET = 3
    WITHDRAWAL     = 4

fraud = (
    Pattern("card_fraud")
    .begin(E.LOGIN_FAILURE, count=3)
    .then(E.PASSWORD_RESET, max_gap_ns=10_000_000_000)   # within 10 s
    .then(E.WITHDRAWAL, value_gte=500.0, max_gap_ns=30_000_000_000)
    .total_window(60_000_000_000)                         # 60 s total
)

engine = CEPEngine()
engine.register(fraud)

@engine.on_match("card_fraud")
def alert(entity_id, pattern_name, ts_ns):
    print(f"[FRAUD] account={entity_id}")

t = time.time_ns()
for _ in range(3):
    engine.push(make_event(E.LOGIN_FAILURE,  entity_id=1001, timestamp=t)); t += 500_000_000
engine.push(make_event(E.PASSWORD_RESET,     entity_id=1001, timestamp=t)); t += 3_000_000_000
engine.push(make_event(E.WITHDRAWAL, 1001,   value=750.0,    timestamp=t))
# → [FRAUD] account=1001
```

---

## Pattern DSL reference

```python
Pattern("name")
  .begin(type_id,
         count=1,           # require N occurrences before advancing
         value_gte=None,    # minimum numeric value
         value_lte=None,    # maximum numeric value
         flags_mask=0,      # bitmask AND
         flags_value=0,     # expected masked result
         within_ns=None,    # max gap from previous step (first step only)
  )
  .then(type_id, ...)       # same kwargs as begin, plus max_gap_ns
  .total_window(ns)         # reset if total elapsed > this (default 60 s)
```

---

## Benchmarks

Measured on Apple M2 (single core, Numba JIT):

| Metric                    | Value         |
|---------------------------|---------------|
| push() p50 latency        | ~120 ns       |
| push() p99 latency        | ~350 ns       |
| push() p99.9 latency      | ~900 ns       |
| Throughput                | ~7 M events/s |
| Pattern compilation       | ~400 ms (once)|
| Ring-buffer batch push    | ~15 ns/event  |

Run benchmarks:

```bash
python benchmarks/bench_engine.py
python benchmarks/bench_engine.py --n 500000
```

---

## Running tests

```bash
pip install -e ".[dev]"
pytest
```

---

## Examples

```bash
python examples/fraud_detection.py
python examples/network_anomaly.py
```

---

## Design notes

- **No Kafka in the hot path** — the ring buffer is `mmap`-backed shared
  memory; a separate process can feed events via `RingBuffer(name=...)`.
- **Single-writer ring buffer** — the writer never acquires a lock; readers
  take a consistent snapshot by reading `write_cursor` before and after.
- **Pattern state isolation** — each pattern maintains its own NFA state
  arrays indexed by `entity_id % MAX_ENTITIES` (1 M slots by default), so
  entities never interfere with each other.
- **Graceful fallback** — if Numba is not installed, a pure-Python fallback
  with identical semantics is used automatically (~10× slower but correct).
- **Code generation** — the compiler emits a Python source string and
  `exec()`s it into an isolated namespace.  Each pattern gets its own
  specialized function with all constants inlined, enabling Numba to produce
  the tightest possible native code.

---

## License

MIT
