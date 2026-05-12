# Streaming Feature Store with Training-Serving Skew Detection

A production-grade feature store where **the same feature computation logic runs in both batch (training) and streaming (serving)** — eliminating training-serving skew by design. A nightly statistical comparator flags distribution drift and triggers automatic retraining.

## Architecture

```
┌─────────────────────────────────────────────────────────────────────┐
│                     SHARED TRANSFORMATION LAYER                     │
│           feature_store/transformations.py  ←  same code            │
└────────────────────┬───────────────────────────────────┬────────────┘
                     │                                   │
          ┌──────────▼──────────┐             ┌──────────▼──────────┐
          │   BATCH PROCESSOR   │             │  STREAM PROCESSOR   │
          │  (training pipeline)│             │  (Kafka consumer)   │
          └──────────┬──────────┘             └──────────┬──────────┘
                     │                                   │
          ┌──────────▼──────────┐             ┌──────────▼──────────┐
          │   OFFLINE STORE     │             │   ONLINE STORE      │
          │   (Parquet files)   │             │   (Redis)           │
          └──────────┬──────────┘             └──────────┬──────────┘
                     │                                   │
                     └──────────────┬────────────────────┘
                                    │
                       ┌────────────▼────────────┐
                       │   NIGHTLY DRIFT CHECK   │
                       │  (APScheduler 02:00 UTC)│
                       │                         │
                       │  KS test (continuous)   │
                       │  PSI (continuous)       │
                       │  Chi-squared (categ.)   │
                       │  Jensen-Shannon (categ.)│
                       └────────────┬────────────┘
                                    │ drift > threshold?
                                    ▼
                       ┌────────────────────────┐
                       │   RETRAINING TRIGGER   │
                       │  (webhook / log)       │
                       └────────────────────────┘
```

## Key Design Decisions

### No Training-Serving Skew
Features are defined as **pure functions** in `feature_store/transformations.py`. The batch processor and stream processor both import and call the same functions — there is no separate SQL/Spark/Python split that would let the implementations diverge.

### Drift Detection Metrics
| Feature Type | Metric | Default Threshold |
|---|---|---|
| Continuous | Kolmogorov-Smirnov p-value | < 0.05 |
| Continuous | Population Stability Index (PSI) | > 0.20 |
| Categorical | Chi-squared p-value | < 0.05 |
| Categorical | Jensen-Shannon divergence | > 0.10 |

A feature is flagged as **drifted** when any applicable metric exceeds its threshold. Retraining fires when **≥ 25%** of features are drifted (configurable).

### Production Distribution Sampling
The stream processor pushes every computed feature value into a **Redis ring-buffer** (10,000 entries per feature). The nightly job reads this buffer as the "production distribution" and compares it to the training snapshot stored in Parquet.

## Quickstart

### Prerequisites
- Docker & Docker Compose
- Python 3.11+ (for running tests locally)

### Run everything

```bash
# Start Kafka, Redis, stream processor, API, scheduler, and simulator
make up

# Seed a training snapshot (run once after services are up)
make seed-training

# Watch logs
make logs
```

The simulator sends 50 events/second. After 500 events the distribution **intentionally drifts** (amounts shift from ~$250 to ~$1500, categories skew toward high-risk). Watch the API for drift reports.

### Trigger a drift check immediately

```bash
make drift-check
```

### Check the API

```bash
# Health
curl http://localhost:8000/health

# Feature registry
curl http://localhost:8000/registry

# On-demand feature computation
curl -X POST http://localhost:8000/features/compute \
  -H 'Content-Type: application/json' \
  -d '{"user_id": "user_0001", "amount": 750.0, "category": "gambling", "user_age": 28}'

# Latest features for a user (after streaming ingestion)
curl http://localhost:8000/features/user_0001

# Latest drift report
curl http://localhost:8000/drift/latest

# Drift history
curl http://localhost:8000/drift/history
```

### Run tests (no Docker required)

```bash
make test
```

## Project Structure

```
streaming-feature-store/
├── feature_store/
│   ├── registry.py           # FeatureDefinition & FeatureRegistry
│   ├── transformations.py    # ← shared feature computation logic
│   ├── batch_processor.py    # Training pipeline
│   ├── stream_processor.py   # Kafka consumer / serving pipeline
│   ├── online_store.py       # Redis wrapper (serving + ring-buffer)
│   ├── offline_store.py      # Parquet wrapper (training snapshots)
│   ├── drift_detector.py     # KS, PSI, Chi2, Jensen-Shannon
│   └── retraining_trigger.py # Fires retraining when drift > threshold
├── scheduler/
│   └── nightly_job.py        # APScheduler — runs drift check at 02:00 UTC
├── api/
│   └── serving.py            # FastAPI feature serving endpoints
├── simulator/
│   ├── data_generator.py     # Synthetic events with configurable drift
│   └── kafka_producer.py     # Publishes events to Kafka
├── scripts/
│   └── seed_training.py      # Generates and saves a training snapshot
├── tests/
│   ├── test_transformations.py
│   ├── test_drift_detector.py
│   └── test_batch_processor.py
├── docker-compose.yml
├── Dockerfile
└── Makefile
```

## Configuration

All thresholds are configurable via environment variables:

| Variable | Default | Description |
|---|---|---|
| `DRIFT_THRESHOLD_PSI` | `0.2` | PSI threshold for continuous features |
| `DRIFT_THRESHOLD_KS` | `0.05` | KS p-value threshold |
| `DRIFT_THRESHOLD_JS` | `0.1` | Jensen-Shannon threshold for categoricals |
| `DRIFT_FRACTION_THRESHOLD` | `0.25` | Fraction of drifted features to trigger retraining |
| `DRIFT_CRON` | `0 2 * * *` | Cron schedule for nightly check (UTC) |
| `RETRAINING_BACKEND` | `log` | `log` or `webhook` |
| `RETRAINING_WEBHOOK_URL` | `` | URL to POST retraining trigger payload |
| `DRIFT_AFTER_EVENTS` | `500` | Simulator: event count before distribution shifts |
| `KAFKA_BOOTSTRAP_SERVERS` | `localhost:9092` | Kafka bootstrap address |
| `REDIS_URL` | `redis://localhost:6379` | Redis URL |
| `OFFLINE_STORE_PATH` | `/data/offline` | Parquet storage directory |

## Extending

### Adding a new feature

1. Add a pure compute function to `feature_store/transformations.py`
2. Register it in `build_registry()` — both batch and stream paths pick it up automatically
3. Add tests to `tests/test_transformations.py`

No other changes needed.

### Connecting a real retraining pipeline

Set `RETRAINING_BACKEND=webhook` and point `RETRAINING_WEBHOOK_URL` at your MLflow, SageMaker, Vertex AI, or Airflow trigger endpoint. The payload is:

```json
{
  "triggered_at": 1718400000.0,
  "drifted_features": ["amount_log1p", "amount_zscore"],
  "overall_drift_score": 0.36,
  "feature_results": [...]
}
```
