# Banking Transaction Anomaly Pipeline

Real-time fraud detection on synthetic banking transactions using Kafka and a micro-batch stream processor, with a live monitoring dashboard.

```
Producer → Kafka (transactions) → Anomaly Detector → Kafka (fraud-alerts) → Dashboard
                                        ↕
                                      Redis
                                  (velocity / geo state)
```

## Stack

| Component | Technology |
|---|---|
| Message broker | Apache Kafka 3.6 (KRaft, no ZooKeeper) |
| Stream processor | Python micro-batch (drop-in PySpark version included) |
| Fraud state store | Redis 7 |
| Dashboard backend | FastAPI + Server-Sent Events |
| Dashboard frontend | Vanilla JS + Chart.js |
| Orchestration | Docker Compose |

## Quick start

```bash
make up
# Dashboard → http://localhost:8080
```

Tear down:
```bash
make down   # keep volumes
make clean  # remove volumes too
```

## Fraud detection rules

| Rule | Trigger | Severity |
|---|---|---|
| `HIGH_AMOUNT` | Transaction ≥ $5,000 | HIGH / CRITICAL |
| `CARD_NOT_PRESENT_HIGH` | Card absent + amount ≥ $2,000 | HIGH |
| `ODD_HOURS` | Transaction at 02:00–05:00 UTC | MEDIUM |
| `ROUND_NUMBER` | Exact round amount ($500, $1 000 …) | LOW |
| `HIGH_RISK_MERCHANT` | Category in `wire_transfer`, `crypto`, `gambling`, `unknown` | MEDIUM |
| `VELOCITY` | > 6 transactions from same account in 10 min | HIGH |
| `GEO_VELOCITY` | Same account in locations ≥ 400 km apart within 30 min | CRITICAL |

Risk scores from triggered rules are summed; the highest bucket determines overall severity.

## PySpark deployment

A production-ready PySpark Structured Streaming job is included at `processor/spark_detector.py`. Run it on any Spark cluster:

```bash
spark-submit \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0 \
  processor/spark_detector.py
```

Stateful rules (velocity, geo-velocity) are handled via Redis `ForeachBatch` in `anomaly_detector.py` and can be ported to Spark's `mapGroupsWithState` for fully stateful cluster processing.

## Configuration

| Variable | Default | Description |
|---|---|---|
| `TPS` | `5` | Transactions per second from the producer |
| `KAFKA_BOOTSTRAP_SERVERS` | `kafka:9092` | Kafka bootstrap address |
| `REDIS_HOST` | `redis` | Redis hostname |

Copy `.env.example` → `.env` to override.

## Project layout

```
banking-anomaly-pipeline/
├── docker-compose.yml
├── Makefile
├── producer/
│   ├── transaction_producer.py   # synthetic data generator
│   └── Dockerfile
├── processor/
│   ├── anomaly_detector.py       # micro-batch processor (runs in Docker)
│   ├── spark_detector.py         # PySpark Structured Streaming version
│   ├── fraud_rules.py            # stateless + stateful rule engine
│   └── Dockerfile
└── dashboard/
    ├── app.py                    # FastAPI + SSE backend
    ├── static/index.html         # real-time monitoring UI
    └── Dockerfile
```
