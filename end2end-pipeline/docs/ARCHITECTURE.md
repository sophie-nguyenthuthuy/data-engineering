# Architecture

## Data flow (Phase 1)

1. **producer** generates synthetic `UserInteraction` events at a configurable
   rate, serializes them with the Avro schema registered under subject
   `user-interactions-value`, and publishes them to Kafka with an **idempotent
   producer**, `acks=all`, and zstd compression.
2. **Kafka** (single-node, KRaft) accepts the events on the
   `user-interactions` topic. Auto-topic-create is disabled; a `kafka-init`
   init container creates the topic and its DLQ partner with
   `partitions=3, replication-factor=1`.
3. **Kafka Connect** runs the **official ClickHouse sink connector**
   (`clickhouse/clickhouse-kafka-connect`). It consumes
   `user-interactions`, deserializes the Avro payload against the Schema
   Registry, and writes each record to
   `events.user_interactions` in ClickHouse.
   - `errors.tolerance=all` and `errors.deadletterqueue.topic.name`
     forward poison messages to `user-interactions-dlq` with context
     headers — you never silently drop data.
4. **ClickHouse** stores rows in a `MergeTree` partitioned by day, with a
   30-day TTL. A materialized view (`user_interactions_1m_mv`) maintains
   a 1-minute `AggregatingMergeTree` roll-up with count, latency sum, and a
   t-digest p95 state — the dashboard's queries run against the roll-up, not
   the raw table.
5. **api** (FastAPI) exposes two endpoints:
   - `GET /api/v1/analytics/summary?minutes=N`
   - `GET /api/v1/analytics/minute?minutes=N&event_type=…`

   Both merge the aggregate states from the 1-minute MV at query time
   (`countMerge`, `sumMerge`, `quantileTDigestMerge`).
6. **dashboard** (static HTML + vanilla JS, served by nginx) polls the API
   every 5 seconds and renders summary cards + a per-minute table. Nginx
   proxies `/api/*` and `/healthz` to the API container so the browser uses
   same-origin URLs.

## Key design choices

### Idempotent producer + Avro-via-Schema-Registry
The producer sets `enable.idempotence=true` and `acks=all`. Events are
serialized with `AvroSerializer` bound to the Schema Registry; the writer's
schema ID is embedded in every message, so the sink deserializes against the
**exact** schema that was written. `BACKWARD` compatibility is enforced at the
registry, meaning new consumers can read old records.

### Sink connector over custom consumer
We use Kafka Connect with the upstream ClickHouse connector rather than a
bespoke Python consumer. Connect handles offsets, rebalancing, DLQs, and
schema evolution; we just provide config. Fewer lines to maintain, and the
same tooling ops teams already use.

### Aggregating MV, not raw scans
Dashboard queries only touch the 1-minute materialized view. Even with
millions of rows/day, `SELECT … FROM user_interactions_1m WHERE minute >=
now() - INTERVAL 15 MINUTE` is O(minutes·event_types·status) =
O(15·6·2) = 180 rows to merge. Raw-table queries are reserved for ad-hoc use
and the eventual Spark job.

### `LowCardinality` + column types
`event_type`, `status`, `country`, `device` are all `LowCardinality(String)`
— ClickHouse encodes them with a per-part dictionary. This roughly halves
storage and speeds up GROUP BYs for those columns. `Int32` for `latency_ms`
is intentional (not `UInt32`): t-digest's merge state can require signed
arithmetic under load.

### Explicit dependencies + healthchecks
Compose uses `depends_on` with `service_healthy` / `service_completed_successfully`
conditions so:
- `schema-registry` waits on Kafka
- `kafka-connect` waits on both + `kafka-init` (topic creation)
- `producer` waits on topic creation + schema registry
- `api` waits on ClickHouse

This means `docker compose up -d` gives you a stack that converges without
manual sleeps/retries in glue scripts.

### What's *not* in Phase 1 (and why)
- **No SASL/TLS on Kafka** — would force all six clients (producer, Connect,
  Schema Registry, admin, tests, dashboard-side admin) to carry credentials,
  which is a distraction before the data path works. Landed in Phase 2.
- **No orchestrator** — Phase 1's "dashboard" is over ClickHouse directly.
  The minutely Spark batch (original architecture) lives in Phase 4 on
  Dagster, because Dagster's typed assets + software-defined assets model
  the Parquet/MinIO/Spark dependency far more cleanly than a DAG of opaque
  Python callables.
- **No metrics/tracing stack** — observability is a cross-cutting concern
  that should be designed once across all services. Phase 5 adds OTel
  propagation end-to-end and a prebuilt Grafana dashboard.
