# Finnhub Streaming Pipeline

A **single-command** real-time market-data pipeline. Python end-to-end, no JVM you'll ever have to touch, no Kubernetes. Finnhub's WebSocket → Kafka → **PyFlink** (5-second tumbling aggregates) → **TimescaleDB** + WebSocket push → **React/Recharts** dashboard.

Inspired by [RSKriegs/finnhub-streaming-data-pipeline](https://github.com/RSKriegs/finnhub-streaming-data-pipeline), rebuilt to be:

- **Faster to run** — `docker compose up` and you're streaming. No Minikube, no Helm, no Terraform.
- **Python end-to-end** — producer, stream processor (PyFlink SQL), sink, API all in Python. No Scala, no `build.sbt`.
- **Time-series native** — TimescaleDB hypertables with compression + retention policies. Full SQL.
- **Real-time UI** — WebSocket push (not Grafana's 500 ms polling). Backpressure-safe fan-out on the server, rAF-batched rendering on the client.
- **Runnable without an API key** — synthetic tick generator kicks in when `FINNHUB_API_KEY` is blank, so the whole pipeline is exercised out of the box.

---

## Architecture

```
 ┌──────────┐      ┌───────┐      ┌────────────────┐      ┌──────────────┐
 │ Finnhub  │ ws   │       │      │   PyFlink      │ jdbc │              │
 │ wss://…  ├────▶ │ Kafka ├────▶ │  (Table API,   ├────▶ │ TimescaleDB  │
 │  (or     │      │ KRaft │      │   5s tumbling  │      │  hypertables │
 │  demo)   │      │       │      │   aggregates)  │      │              │
 └──────────┘      └───┬───┘      └──────┬─────────┘      └──────┬───────┘
                       │                 │ kafka                  │ sql
                       │                 ▼                        │
                       │         trades.agg.5s topic              │
                       │                 │                        │
                       ▼                 ▼                        ▼
                   ┌──────────────────────────────────────────────────┐
                   │   FastAPI  (WebSocket /ws fan-out, REST history) │
                   └───────────────────────┬──────────────────────────┘
                                           │ ws push (no polling)
                                           ▼
                          ┌──────────────────────────────┐
                          │  React + Recharts dashboard  │
                          │  (Vite build, served by nginx)│
                          └──────────────────────────────┘
```

Six services in one Compose file: `kafka`, `timescaledb`, `producer`, `stream_processor`, `api`, `frontend`.

---

## Quick start

```bash
git clone <this-repo> finnhub-pipeline
cd finnhub-pipeline
cp .env.example .env
# (optional) edit .env and paste your FINNHUB_API_KEY — without it, demo mode kicks in
docker compose up --build
```

Then open:

- **Dashboard**: http://localhost:3000
- **API docs**:  http://localhost:8000/docs
- **Kafka (host)**: `localhost:9094`  (`EXTERNAL` listener)
- **Postgres**: `localhost:5433`  (user/pass from `.env`; internal port 5432)

Shut down:

```bash
docker compose down        # keep volumes
docker compose down -v     # nuke db volume
```

---

## What's inside

### `producer/`
Python asyncio service. Two modes:

- **Live** — connects to `wss://ws.finnhub.io?token=…`, subscribes to `SYMBOLS`, re-publishes each trade to the `trades` Kafka topic.
- **Demo** — geometric random-walk tick generator with per-symbol drift/vol profiles. Lets you exercise the full pipeline with zero setup.

### `stream_processor/`
PyFlink Table API job. One statement set, three sinks:

1. `INSERT INTO trades_jdbc` — raw trades persisted to TimescaleDB.
2. `INSERT INTO agg_jdbc`    — 5s tumbling aggregates (`count, avg, min, max, volume, VWAP`) persisted to TimescaleDB.
3. `INSERT INTO agg_kafka`   — same aggregates re-published to the `trades.agg.5s` topic so the API can push them over WebSocket without a DB round-trip.

Watermark tolerates 2 s of out-of-order arrival. Parallelism = 1 (bump in `job.py` when you scale out).

### `db/init.sql`
TimescaleDB hypertables, symbol-segmented columnstore compression after 1 day, 7-day retention. Ready for `time_bucket()`, continuous aggregates, whatever you want to tack on.

### `api/`
FastAPI + aiokafka. Single Kafka consumer, per-WebSocket bounded queue with drop-oldest on full. Optional per-client symbol filter (`{"symbols":["AAPL"]}` as the first WS message). REST endpoints: `/symbols`, `/history/{sym}?minutes=15`, `/healthz`.

### `frontend/`
Vite + React 18 + Recharts. Dark theme, per-symbol chip selector, live price line + 5s VWAP overlay, volume bars, scrolling trades table. State updates are batched via `requestAnimationFrame` so a firehose of ticks doesn't melt the render loop.

---

## Configuration

Everything lives in `.env` (see `.env.example`). Notable knobs:

| Var | Default | Notes |
|---|---|---|
| `FINNHUB_API_KEY` | *(blank)* | Blank → demo mode |
| `SYMBOLS` | `BINANCE:BTCUSDT,…,NVDA` | Comma-separated |
| `TRADES_TOPIC` | `trades` | |
| `AGGREGATES_TOPIC` | `trades.agg.5s` | |
| `API_PORT` | `8000` | |
| `WEB_PORT` | `3000` | |

---

## Dev loop

### Frontend hot-reload
```bash
cd frontend
npm install
VITE_API_URL=http://localhost:8000 npm run dev   # http://localhost:5173
```

### Inspect Kafka from the host
```bash
docker exec -it finnhub_kafka /opt/bitnami/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server kafka:9092 --topic trades --from-beginning --max-messages 5
```

### Poke TimescaleDB
```bash
docker exec -it finnhub_timescaledb psql -U pipeline -d market -c \
  "SELECT symbol, time_bucket('1 minute', window_start) AS m, AVG(vwap) \
   FROM trades_agg_5s GROUP BY 1,2 ORDER BY 2 DESC LIMIT 20;"
```

---

## Why this, vs. the original

| | Original (RSKriegs) | This |
|---|---|---|
| Orchestration | Minikube + Helm + Terraform | `docker compose up` |
| Stream engine | Spark Structured Streaming (Scala) | PyFlink (Table API, Python) |
| Storage | Cassandra | TimescaleDB (Postgres, full SQL) |
| Dashboard | Grafana, plugin, polls every 500 ms | React/Recharts, WebSocket push |
| Codebase | Scala + Python + HCL + Shell + Dockerfiles | Python + JSX + SQL |
| Time-to-first-trade | Minutes of setup + image pulls | One compose up, ~60 s cold |

**Tradeoff**: no K8s story. If you need horizontal autoscaling, rolling upgrades, and multi-tenant isolation, the original project's K8s deployment is still the model. This one is for laptops, demos, prototypes, and small deployments.

---

## Not yet

- Schema Registry + Avro (JSON is fine for now; switch to `flink-sql-avro-confluent` + Schema Registry if you need versioning).
- Continuous aggregates (`CREATE MATERIALIZED VIEW … WITH (timescaledb.continuous)`) for 1m / 1h rollups.
- Auth on the WebSocket.
- CI.
# finnhub-pipeline
