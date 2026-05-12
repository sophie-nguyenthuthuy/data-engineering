# mlops_data_layer

End-to-end **MLOps data layer**: feature engineering pipelines, training/serving **skew** detection, statistical **drift** monitoring, and automated **retraining** triggers — all wired together over Kafka, Postgres, and Redis.

## Architecture

```
Production events ──▶ Kafka ──▶ Feature pipeline ──▶ Feature store (Postgres + Redis)
                                       │                       │
                                       │                       └─▶ Online serving (FastAPI)
                                       │
                                       ├─▶ Skew detector  (training-vs-serving distribution diff)
                                       ├─▶ Drift monitor  (KS / PSI / Chi² vs reference)
                                       └─▶ Retraining trigger ──▶ webhook / queue
```

## Components

| Path | Role |
|---|---|
| `src/features/registry.py` | Feature definitions (YAML-driven) |
| `src/features/pipeline.py` | Streaming feature engineering |
| `src/features/store.py` | Postgres + Redis dual-layer feature store |
| `src/features/transforms.py` | Pure-function transforms (reusable batch & stream) |
| `src/skew/profiler.py` | Computes training & serving distribution profiles |
| `src/skew/detector.py` | Compares profiles, emits skew alerts |
| `src/drift/` | KS / PSI / Chi² / JS drift tests against a reference window |
| `src/retraining/` | Threshold rules → trigger emitter |
| `src/api/app.py` | FastAPI: feature lookup, drift status, manual retrain |
| `src/stream/` | Kafka producer + consumer |

## Quick start

```bash
docker compose up -d            # Kafka, Zookeeper, Postgres, Redis, Prometheus
pip install -r requirements.txt

# Full stack: API + drift monitor + Kafka consumer
python main.py

# Subprocess modes
python main.py --mode api
python main.py --mode monitor

# Bind a specific model
python main.py --model fraud_classifier --version v3
```

API: http://localhost:8000 · Prometheus: http://localhost:9090

## Configuration

Env-driven via `src/config.py` (pydantic-settings):

- `KAFKA_BOOTSTRAP_SERVERS`, `KAFKA_EVENTS_TOPIC`
- `POSTGRES_DSN`, `REDIS_URL`
- `FEATURE_DEFINITIONS` (path to `config/features/feature_definitions.yml`)
- `DRIFT_THRESHOLDS` (path to `config/drift/thresholds.yml`)
- `REFERENCE_WINDOW_DAYS`, `DRIFT_CHECK_INTERVAL_S`
- `RETRAIN_WEBHOOK_URL`

## Drift thresholds (`config/drift/thresholds.yml`)

```yaml
defaults:
  ks_p_value:  0.01
  psi:         0.2
  chi2_p:      0.01
overrides:
  fraud_classifier:
    psi: 0.1                    # tighter for fraud
```

When any test crosses its threshold for a configured number of consecutive windows, the retraining trigger fires.

## Tests

```bash
pytest                            # all
pytest tests/test_features.py
pytest tests/test_skew.py
pytest tests/test_drift.py
pytest tests/test_retraining.py
```

## Status

Reference implementation. Single-region, one model per process. Roadmap: multi-model orchestration, ground-truth-delayed metrics, feedback-loop bias detection.
