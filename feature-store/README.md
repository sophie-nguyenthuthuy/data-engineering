# feature-store

Real-time **dual-layer feature store**: Kafka ingestion fans out to a Redis **online** store (low-latency serving) and a Parquet **offline** store (training datasets). Built for <10 ms p99 serving latency.

## Architecture

```
Producers ──▶ Kafka ──▶ Ingestion consumer ──┬─▶ Redis (online, hot features)
                                              │      │
                                              │      └─▶ FastAPI serving (<10ms p99)
                                              │
                                              └─▶ Parquet (offline, training)
                                                     │
                                                     └─▶ Training data API
                       Feature Registry  ◀────────── (YAML-defined feature views)
                       Consistency Sync  ◀────────── (online↔offline reconciliation)
```

## Modules

| Module | Purpose |
|---|---|
| `src/feature_store/registry/` | Feature definitions loaded from `configs/feature_store.yaml` |
| `src/feature_store/ingestion/` | Kafka producer + consumer (writes both stores) |
| `src/feature_store/online/redis_store.py` | Hash-based feature reads, pipelined |
| `src/feature_store/offline/parquet_store.py` | Partitioned Parquet writer + reader |
| `src/feature_store/consistency/sync.py` | Periodic online↔offline reconciliation |
| `src/feature_store/serving/server.py` | FastAPI batch lookup API |
| `src/feature_store/serving/client.py` | Python client SDK |

## Quick start

```bash
docker compose up -d            # Kafka, Zookeeper, Redis, kafka-ui
pip install -e ".[dev]"
./scripts/setup_topics.sh       # create feature topics

# Examples
python examples/producer_example.py        # publish feature events
python examples/serving_example.py         # online lookup
python examples/training_data_example.py   # offline batch read
```

Serving API: http://localhost:8001 · Kafka UI: http://localhost:8080

## Benchmark

```bash
python scripts/benchmark.py --concurrency 50 --requests 10000
```

Targets: <10 ms p99 online read, <100 ms p99 batch (100 features).

## Feature definition

`configs/feature_store.yaml`:

```yaml
feature_views:
  - name: user_session_features
    entities: [user_id]
    ttl: 3600
    schema:
      - name: clicks_30m
        dtype: int64
      - name: avg_dwell_ms
        dtype: float64
    online: true
    offline: true
```

## Tests

```bash
pytest                              # full suite
pytest tests/test_online_store.py
pytest tests/test_offline_store.py
pytest tests/test_serving.py
pytest tests/test_registry.py
```

## Status

MVP. Single Kafka cluster, single Redis. Roadmap: point-in-time correctness for offline reads, materialised on-demand features, multi-region replication.
