# Adaptive Micro-batch Optimizer

A streaming processor that dynamically adjusts its micro-batch window size
(**50 ms → 5 s**) in response to real-time throughput, latency SLA targets,
and downstream backpressure — with no human intervention required.

## How it works

```
                ┌──────────────────────────────────────────────┐
  ingest()  ──► │  asyncio.Queue  (bounded, drop on overflow)  │
                └───────────────────┬──────────────────────────┘
                                    │  every window_size seconds
                                    ▼
                ┌──────────────────────────────────────────────┐
                │           MicroBatchProcessor                │
                │  • drains queue → calls handler(batch)       │
                │  • records processing_time, batch_size       │
                └───────────────────┬──────────────────────────┘
                                    │
                                    ▼
                ┌──────────────────────────────────────────────┐
                │         AdaptiveWindowManager                │
                │                                              │
                │  error = latency_error + bp_weight × bp      │
                │       ──────────────────────────────         │
                │  latency_error = (p95 − target) / target     │
                │  bp            = BackpressureMonitor level   │
                │                                              │
                │  new_window = PIDController.apply(error)     │
                │  clamped to [50 ms, 5 s]                     │
                └──────────────────────────────────────────────┘
```

### PID Controller

The [proportional-integral-derivative controller](https://en.wikipedia.org/wiki/PID_controller)
drives window-size adjustments:

| Term | Role |
|---|---|
| **P** | Reacts immediately to current latency overshoot / undershoot |
| **I** | Eliminates steady-state offset; anti-windup clamped |
| **D** | Damps oscillation when latency is changing fast |

Default gains (`kp=0.4, ki=0.05, kd=0.15`) provide stable convergence for
most workloads; tune via `PIDConfig`.

### Backpressure

Downstream workers call `processor.report_backpressure(source, level)` with a
load factor in `[0, 1]`.  The monitor keeps an exponentially-weighted rolling
average (half-life ≈ `window / 3`) and injects it into the error signal,
causing the window to shrink when consumers are saturated.

## Installation

```bash
pip install -e ".[dev]"
```

Requires Python ≥ 3.11 (uses `list[T]` generics in type hints).

## Quick start

```python
import asyncio
from adaptive_microbatch import MicroBatchProcessor
from adaptive_microbatch.window_manager import SLAConfig

async def my_handler(batch: list[dict]) -> None:
    # write to DB, publish to Kafka, etc.
    ...

async def main():
    sla = SLAConfig(
        target_latency_s=0.1,    # 100 ms p95 target
        min_throughput_eps=200,  # 200 events/sec floor
        backpressure_weight=0.5,
    )
    proc = MicroBatchProcessor(handler=my_handler, sla=sla)

    await proc.start()

    # Produce events from any coroutine / thread
    await proc.ingest({"id": 1, "payload": "..."})

    # Downstream worker signals load
    proc.report_backpressure("db-pool", level=0.8)

    await proc.stop(drain=True)
    print(proc.stats())

asyncio.run(main())
```

## Configuration

### `SLAConfig`

| Field | Default | Description |
|---|---|---|
| `target_latency_s` | `0.2` | p95 latency target (seconds) |
| `min_throughput_eps` | `100` | Minimum acceptable events/sec |
| `backpressure_weight` | `0.5` | How much backpressure inflates the error |

### `PIDConfig`

| Field | Default | Description |
|---|---|---|
| `kp` | `0.4` | Proportional gain |
| `ki` | `0.05` | Integral gain |
| `kd` | `0.15` | Derivative gain |
| `min_output` | `0.05` | Window floor (50 ms) |
| `max_output` | `5.0` | Window ceiling (5 s) |
| `integral_clamp` | `2.0` | Anti-windup clamp |

### `MicroBatchProcessor`

| Parameter | Default | Description |
|---|---|---|
| `handler` | required | `async (list[T]) -> None` batch callback |
| `sla` | `SLAConfig()` | SLA targets |
| `max_queue_size` | `10_000` | Queue capacity; overflow events are dropped |
| `initial_window` | `0.5` | Starting window size (seconds) |

## Running the demo

```bash
cd examples
python demo_simulation.py
```

Runs four phases (normal → latency spike → heavy backpressure → recovery)
with a live ASCII dashboard showing how the window tracks conditions.

## Tests

```bash
pytest
```

## Project layout

```
src/adaptive_microbatch/
  processor.py      # MicroBatchProcessor — public API
  pid_controller.py # PID controller with anti-windup
  window_manager.py # Integrates PID + metrics + backpressure
  metrics.py        # Rolling-window latency / throughput collector
  backpressure.py   # Downstream pressure aggregator
examples/
  basic_usage.py
  demo_simulation.py
tests/
  test_pid_controller.py
  test_backpressure.py
  test_window_manager.py
  test_processor.py
```
