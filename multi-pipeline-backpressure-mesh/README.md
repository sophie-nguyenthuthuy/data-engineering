# Multi-Pipeline Backpressure Mesh

External backpressure coordination layer for distributed streaming pipelines.  
When a downstream sink slows, upstream jobs **coordinately throttle together** — preventing the classic fast-producer / slow-consumer cascade — **without any changes to job internals**.

```
ProducerJob ──→ TransformJob ──→ SinkJob  (slow)
     ↑                ↑              │
     └── throttle ────┘   ←── signal ┘
              BackpressureMesh
```

## Key Design Principles

| Concern | Approach |
|---|---|
| **Non-invasive** | Jobs expose only a metrics endpoint and a throttle handle — no mesh imports inside job logic |
| **Distributed** | Signals travel over a pluggable bus (in-memory for tests, Redis pub/sub for production) |
| **Topology-aware** | Backpressure propagates only to transitive upstream ancestors, scaled by hop distance |
| **Self-healing** | Coordinator reconciles every N seconds; throttles are released automatically when pressure expires |

## Architecture

```
┌──────────────────────────────────────────────────────────────────┐
│                        Backpressure Mesh                         │
│                                                                  │
│  ┌─────────────┐   signal    ┌─────────────────┐   throttle     │
│  │  JobSidecar  │ ──────────→ │   Coordinator   │ ─────────────→ │
│  │  (per job)  │             │  (single global)│                │
│  └──────┬──────┘             └─────────────────┘                │
│         │ polls                                                  │
│         ▼                                                        │
│  ┌─────────────┐                                                 │
│  │ JobMetrics  │  (queue depth, throughput, lag)                 │
│  └─────────────┘                                                 │
│                                                                  │
│  ┌─────────────────────────────┐                                 │
│  │    BackpressureBus          │                                 │
│  │  InMemoryBus | RedisBus     │                                 │
│  └─────────────────────────────┘                                 │
└──────────────────────────────────────────────────────────────────┘
```

### Components

- **`BackpressureBus`** — pub/sub transport for signals and throttle commands.  
  `InMemoryBus` needs no infrastructure; `RedisBus` works across processes/hosts.

- **`JobSidecar`** — runs alongside each job. Polls `JobMetrics`, emits `BackpressureSignal` when pressure is detected, and applies `ThrottleCommand` to the job's `TokenBucketThrottle`.

- **`BackpressureCoordinator`** — single global service. Receives signals, walks the topology DAG upstream, and publishes proportionally attenuated throttle commands. Runs a periodic reconcile to expire stale pressure and release throttles.

- **`TokenBucketThrottle`** — rate limiter injected at the job's source reader (`await throttle.acquire()`). Adjusted in real-time by the sidecar without stopping or restarting the job.

- **`PipelineTopology`** — DAG of job nodes. Defines which jobs are upstream of which, and the propagation weights.

### Propagation Model

When job **C** emits a backpressure signal with score `s`:

```
throttle_factor(job, hop) = 1 - s × HOP_ATTENUATION^hop × node.propagation_weight
```

Default `HOP_ATTENUATION = 0.7`, so:

| Hop | Attenuation | throttle_factor (s=0.8) |
|-----|-------------|-------------------------|
| 1   | 0.70        | 0.44                    |
| 2   | 0.49        | 0.61                    |
| 3   | 0.34        | 0.73                    |

Pressure records expire after 10 seconds. The reconcile loop (default 2s) recomputes required throttle levels across all active signals and releases excess restrictions.

## Quickstart

```bash
git clone https://github.com/<you>/multi-pipeline-backpressure-mesh
cd multi-pipeline-backpressure-mesh
pip install -e ".[dev]"

# Run the three-stage linear pipeline demo
python -m examples.three_stage_pipeline

# Run the fan-out topology demo
python -m examples.fan_out_topology
```

### With Redis (distributed)

```bash
docker compose up redis -d
REDIS_URL=redis://localhost:6379 python -m examples.three_stage_pipeline
```

## Testing

```bash
pytest tests/ -v
```

## Wiring Into a Real Job

The only two integration points are:

**1. Expose a metrics callable** (e.g. read from Flink REST API):

```python
def get_flink_metrics() -> JobMetrics:
    resp = requests.get(f"http://flink-jm:8081/jobs/{job_id}/metrics")
    ...
    return JobMetrics(job_id=job_id, input_queue_depth=..., ...)
```

**2. Inject the throttle into your source reader**:

```python
throttle = TokenBucketThrottle(rate=1000.0)

async def read_from_kafka():
    await throttle.acquire()        # ← only external change
    return consumer.poll(timeout=0.1)
```

Then start a sidecar:

```python
sidecar = JobSidecar(
    job_id="my-flink-job",
    bus=bus,
    metrics_provider=get_flink_metrics,
    throttle=throttle,
)
await sidecar.start()
```

## Project Structure

```
mesh/
  bus.py          BackpressureBus (InMemoryBus + RedisBus)
  coordinator.py  Central coordinator + propagation logic
  sidecar.py      Per-job monitoring + throttle application
  throttle.py     TokenBucketThrottle
  metrics.py      JobMetrics, BackpressureSignal, ThrottleCommand
  topology.py     PipelineTopology DAG

jobs/             Simulated Flink/Spark jobs (demo / testing)
examples/         Runnable scenarios
tests/            pytest suite
```
