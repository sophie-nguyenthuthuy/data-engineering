# CDC Pipeline — Debezium → Kafka → Data Warehouse

A production-grade Change Data Capture pipeline that streams every `INSERT`, `UPDATE`, and `DELETE`
from a transactional PostgreSQL database into an analytical data warehouse — with full schema
evolution and out-of-order event handling.

```
┌─────────────────┐     WAL / logical     ┌──────────────────┐
│  Source Postgres │ ──── replication ───► │ Debezium Connect │
│  (transactional) │                       │  (Kafka Connect) │
└─────────────────┘                        └────────┬─────────┘
                                                    │ Avro + Schema Registry
                                                    ▼
                                          ┌─────────────────┐
                                          │      Kafka       │
                                          │  3 partitions    │
                                          │  per table topic │
                                          └────────┬────────┘
                                                   │
                                          ┌────────▼────────┐
                                          │  CDC Consumer    │
                                          │  ┌────────────┐ │
                                          │  │ReorderBuffer│ │  ← LSN-ordered
                                          │  │ (watermark) │ │    out-of-order
                                          │  └────────────┘ │    handling
                                          │  ┌────────────┐ │
                                          │  │Schema      │ │  ← v1→v2 field
                                          │  │Evolution   │ │    migration
                                          │  └────────────┘ │
                                          └────────┬────────┘
                                                   │ idempotent upserts
                                                   ▼
                                          ┌─────────────────┐
                                          │ Warehouse Postgres│
                                          │  dim_users        │
                                          │  fact_orders      │
                                          │  fact_order_items │
                                          │  cdc_audit_log    │
                                          │  cdc_watermarks   │
                                          └─────────────────┘
```

---

## Stack

| Component | Image / Version |
|---|---|
| Debezium Connector | `quay.io/debezium/connect:2.5` |
| Kafka | `confluentinc/cp-kafka:7.5.0` |
| Schema Registry | `confluentinc/cp-schema-registry:7.5.0` |
| Zookeeper | `confluentinc/cp-zookeeper:7.5.0` |
| Source DB | `postgres:15` (logical replication enabled) |
| Warehouse DB | `postgres:15` |
| Consumer | Python 3.11 + `confluent-kafka[avro]` |

---

## Quick Start

```bash
# 1. Start everything and register the Debezium connector
bash scripts/setup.sh

# 2. Watch the warehouse fill up
watch -n 2 'psql postgresql://dw_user:dw_secret@localhost:5433/data_warehouse \
  -c "SELECT COUNT(*) FROM dim_users; SELECT COUNT(*) FROM fact_orders;"'

# 3. Run the change simulator (inserts, updates, deletes + OOO scenarios)
python scripts/simulate_changes.py --duration 120 --rate 3

# 4. Check consumer lag and watermarks
bash scripts/check_lag.sh

# 5. Tear down (keep volumes)
bash scripts/teardown.sh

# 5b. Full wipe including volumes
bash scripts/teardown.sh --volumes
```

---

## Project Layout

```
cdc-pipeline/
├── docker-compose.yml          # Full stack definition with health checks
├── .env                        # Credentials / env vars
│
├── config/
│   └── debezium/
│       └── postgres-connector.json   # Debezium connector config + SMTs
│
├── schemas/                    # Avro schemas (Schema Registry subjects)
│   ├── users_v1.avsc           # Initial user schema
│   ├── users_v2.avsc           # Evolved: adds tier/phone, renames status
│   ├── orders_v1.avsc
│   └── order_items_v1.avsc
│
├── source-db/
│   └── init.sql                # Source tables + publication + seed data
│
├── warehouse/
│   └── init.sql                # DWH tables (dim/fact + audit + watermarks)
│
├── consumer/
│   ├── main.py                 # Kafka consumer loop; manual offset commit
│   ├── event_processor.py      # ReorderBuffer — LSN-ordered, watermarked
│   ├── schema_handler.py       # Schema evolution via Schema Registry
│   ├── warehouse_sink.py       # Idempotent upserts + audit log + DLQ
│   ├── config.py               # All config from env vars
│   ├── requirements.txt
│   └── Dockerfile
│
└── scripts/
    ├── setup.sh                # One-shot bootstrap
    ├── register_connector.sh   # POST/PUT connector to Kafka Connect REST API
    ├── register_schemas.sh     # Register Avro schemas in Schema Registry
    ├── evolve_schema.sh        # Live schema migration demo (v1 → v2)
    ├── simulate_changes.py     # Workload generator with OOO simulation
    ├── check_lag.sh            # Consumer lag + warehouse health
    └── teardown.sh             # docker compose down [--volumes]
```

---

## Out-of-Order Event Handling

### Problem
Kafka guarantees ordering only *within* a single partition. With 3 partitions per topic,
and with multi-row transactions that span multiple partitions, events can arrive with
LSN gaps — a row updated at LSN 500 may arrive after a row updated at LSN 510.

### Solution — `ReorderBuffer` (`consumer/event_processor.py`)

```
Kafka partition 0: LSN 100 → 102 → 105 → ...
Kafka partition 1: LSN 101 → 104 → ...
Kafka partition 2: LSN 103 → 106 → ...

safe_lsn = min(max_lsn_seen per partition)
         = min(105, 104, 106) = 104

→ Flush and apply LSN ≤ 104 in sorted order: 100, 101, 102, 103, 104
→ Hold: 105, 106 (not yet safe)
```

- **Watermark** = `min(highest LSN seen on each partition)` — all events below this LSN
  have been received from every partition, so ordering is safe.
- **Forced flush** — if no watermark advancement after `LAG_TOLERANCE_MS` (default 30s),
  flush unconditionally (handles stalled/idle partitions).
- **Buffer overflow** — if `MAX_BUFFER_SIZE` events accumulate, flush all in LSN order
  with back-pressure.

### Idempotency
The warehouse sink uses `INSERT ... ON CONFLICT DO UPDATE WHERE _cdc_lsn <= EXCLUDED._cdc_lsn`.
A late-arriving duplicate or out-of-order event with a lower LSN is silently skipped —
the newer state in the warehouse is preserved.

---

## Schema Evolution

### Strategy
Schema Registry is configured with `BACKWARD` compatibility:  
> New schema readers can read data written with the old schema.

This means you may:
- ✅ Add optional fields (with defaults)
- ✅ Delete fields
- ✅ Rename fields (using Avro `aliases`)
- ❌ Change field types or remove defaults from required fields

### Demo — users v1 → v2

```bash
bash scripts/evolve_schema.sh
```

What happens:
1. `users_v2.avsc` is registered as a new version under `cdc.public.users-value`
2. Source table is `ALTER`-ed: `status` renamed to `account_status`, `tier`/`phone` added
3. New events carry the v2 schema_id in their Avro wire header
4. Old events (with v1 schema_id) are migrated on read by `SchemaEvolutionHandler`:
   - `status` → `account_status` via Avro alias resolution
   - `tier` / `phone` filled with `null` (their declared defaults)
5. Warehouse rows updated transparently — no reprocessing required

### Wire Format

Confluent Avro wire encoding (5-byte magic header):

```
┌──────┬──────────────────┬────────────────────────────────┐
│ 0x00 │ schema_id (4 B)  │  Avro binary payload           │
└──────┴──────────────────┴────────────────────────────────┘
```

The consumer reads `schema_id` from bytes 1–4, fetches the schema from the registry,
and compares it with the latest registered version for that subject.

---

## Debezium Connector Config Highlights

| Setting | Value | Why |
|---|---|---|
| `plugin.name` | `pgoutput` | Native PostgreSQL plugin — no extra install |
| `snapshot.mode` | `initial` | Full snapshot on first run, then streaming |
| `tombstones.on.delete` | `true` | Null-value tombstone enables Kafka log compaction |
| `transforms.unwrap` | `ExtractNewRecordState` | Flattens Debezium envelope; adds `__op`, `__lsn`, `__ts_ms` |
| `topic.creation.default.partitions` | `3` | Multiple partitions enable parallel consumption |
| `heartbeat.interval.ms` | `30000` | Keeps replication slot alive during low-traffic periods |

---

## Warehouse Schema

### `dim_users` / `fact_orders` / `fact_order_items`
Denormalized tables receiving upserts from the consumer.
Each row carries CDC metadata columns:

| Column | Description |
|---|---|
| `_cdc_op` | `c` create, `r` read (snapshot), `u` update, `d` soft-delete |
| `_cdc_lsn` | PostgreSQL LSN — used for ordering and idempotency guard |
| `_cdc_ts` | Timestamp of the original database event |
| `_cdc_tx_id` | Source transaction ID (groups related changes) |
| `_schema_version` | Avro schema_id the event was written with |
| `_dw_loaded_at` | When this row was last written to the warehouse |

### `cdc_audit_log`
Append-only record of every CDC event ever applied — useful for debugging,
point-in-time reconstruction, and compliance.

### `cdc_watermarks`
Tracks the highest processed LSN per `(topic, partition)`. Used for consumer
restart recovery — the consumer can resume from the last safe point.

### `cdc_dead_letter`
Events that failed deserialization or warehouse writes land here for manual inspection.

---

## Environment Variables

| Variable | Default | Description |
|---|---|---|
| `KAFKA_BOOTSTRAP_SERVERS` | `localhost:9092` | Kafka broker address |
| `SCHEMA_REGISTRY_URL` | `http://localhost:8081` | Confluent Schema Registry |
| `WAREHOUSE_DSN` | `postgresql://...` | Target warehouse connection string |
| `KAFKA_GROUP_ID` | `cdc-consumer-group` | Kafka consumer group |
| `TOPICS` | `cdc.public.users,...` | Comma-separated topic list |
| `LAG_TOLERANCE_MS` | `30000` | Max ms to hold events before forced flush |
| `MAX_BUFFER_SIZE` | `10000` | Max events in reorder buffer before forced flush |
| `FLUSH_INTERVAL_MS` | `5000` | Periodic flush tick interval |
| `WAREHOUSE_BATCH_SIZE` | `500` | Events per warehouse write transaction |

---

## Ports

| Service | Port |
|---|---|
| Source PostgreSQL | `5432` |
| Warehouse PostgreSQL | `5433` |
| Kafka | `9092` |
| Confluent Schema Registry | `8081` |
| Kafka Connect (Debezium) | `8083` |
