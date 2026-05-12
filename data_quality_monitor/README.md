# data_quality_monitor

Event-driven stream validator that runs **Great Expectations** and **Soda** checks against every Kafka micro-batch, surfaces results on a live dashboard, and uses a Redis-backed gate to block downstream jobs when quality drops below threshold.

## Architecture

```
Kafka topic ──▶ KafkaBatchConsumer ──▶ MicroBatchProcessor
                                          │
                                          ├─▶ GreatExpectations + Soda validators
                                          │      │
                                          │      ├─▶ ValidationRepository (Postgres)
                                          │      ├─▶ MetricsCollector → Prometheus
                                          │      └─▶ JobController (Redis: block/allow)
                                          │
                                          └─▶ Kafka result topic
                                                 │
                                                 └─▶ FastAPI dashboard (live results)
```

## Components

| Path | Role |
|---|---|
| `src/stream/` | Kafka consumer (batch poll) + result producer |
| `src/pipeline/micro_batch_processor.py` | Orchestrates per-batch validation |
| `src/validators/` | Great Expectations + Soda runners |
| `src/storage/` | Postgres repository for validation runs |
| `src/blocking/job_controller.py` | Redis flag — downstream jobs check before starting |
| `src/metrics/` | Prometheus collector + publisher |
| `src/dashboard/` | FastAPI app — live results, drilldowns |
| `config/expectations/` | GE suite + project config |
| `config/soda/` | Soda checks YAML |

## Quick start

```bash
docker compose up -d            # Kafka, Zookeeper, Postgres, Redis, Prometheus
pip install -r requirements.txt
python main.py                  # consumer + dashboard
# or split:
python main.py --mode consumer
python main.py --mode dashboard
```

Dashboard: http://localhost:8000 · Prometheus: http://localhost:9090

## Configuration

Driven by env vars consumed in `src/config.py` (pydantic-settings). Key knobs:

- `KAFKA_BOOTSTRAP_SERVERS`, `KAFKA_INPUT_TOPIC`, `KAFKA_RESULT_TOPIC`
- `POSTGRES_DSN`
- `REDIS_URL`
- `BATCH_SIZE`, `BATCH_TIMEOUT_MS`
- `EXPECTATIONS_DIR`, `SODA_CHECKS_FILE`
- `BLOCK_THRESHOLD` — failure ratio above which downstream jobs are gated

## Downstream gating

Each validated batch writes a status key to Redis:

```
quality:status:<dataset>  =  "allow" | "block"  (TTL = N batches)
```

Downstream jobs should `GET` the key before processing. The `JobController` exposes a Python client and the dashboard surfaces current state.

## Tests

```bash
pytest                          # all
pytest tests/test_validators.py # validator unit tests
pytest tests/test_blocking.py   # Redis gating
```

## Status

Production-shaped reference implementation. Single-region. Roadmap: dynamic expectation generation from data profiling, multi-tenant dashboards.
