# Orchestration (Phase 4)

Phase 4 adds the analytics half of the pipeline: hourly slices of
`events.user_interactions` are extracted to Parquet on MinIO, processed by
PySpark, and written back into a ClickHouse rollup that the API serves.

It also adds a Dagster job for **replaying the DLQ** now that Phase 3 proved
out the DLQ retry/tuning.

## Topology

```
                               (every hour at :05)
                              ┌─────────────────────┐
ClickHouse.user_interactions  │ Dagster schedule    │
       │ FINAL                └──────────┬──────────┘
       ▼                                 ▼
 ┌────────────────┐  Parquet  ┌───────────────────┐  Spark  ┌────────────────────┐
 │ raw_events     │ ────────▶ │ MinIO             │ ──────▶ │ event_analysis     │
 │ _parquet       │           │ pipeline/raw/…    │         │ (local[*])         │
 └────────────────┘           └───────────────────┘         └─────────┬──────────┘
                                                                      │
                                                                      ▼
                                                             ┌────────────────────┐
                                                             │ analysis_report    │
                                                             │ → analysis_hourly  │
                                                             │    (ReplacingMT)   │
                                                             └────────────────────┘
```

The three assets share an `HourlyPartitionsDefinition`, so each scheduled
tick (and each manual backfill) processes exactly one partition. Re-running
a partition is safe:

- `raw_events_parquet` overwrites the same S3 key (`dt=…/hour=…/data.parquet`)
- `event_analysis` is pure (reads Parquet, returns aggregates, stops Spark)
- `analysis_report` inserts into `ReplacingMergeTree(generated_at)` so the
  newer row wins on merge — no `DELETE` needed.

## Asset details

### `raw_events_parquet`

Reads `SELECT … FROM user_interactions FINAL WHERE occurred_at ∈ [H, H+1)`
via `clickhouse-connect`'s Arrow path and writes a zstd Parquet to
`s3://pipeline/raw/user_interactions/dt=YYYY-MM-DD/hour=HH/data.parquet`.
`FINAL` forces ReplacingMergeTree to collapse duplicates before extraction.

Metadata surfaced in the Dagster UI: row count, byte count, S3 URI.

### `event_analysis`

PySpark job in `local[*]` mode, configured against MinIO via the Hadoop S3A
filesystem (`hadoop-aws:3.3.4` + `aws-java-sdk-bundle:1.12.262`, loaded
through `spark.jars.packages` — Ivy cache is mounted as a named volume so
the download happens exactly once per image lifetime).

The aggregation is cohort-wise: for each `(event_type, status, country,
device)` it computes `count`, error count, average latency, and the Spark
native `percentile_approx` for p95.

Empty partitions short-circuit without starting Spark.

### `analysis_report`

Writes the cohort rows to `events.analysis_hourly`. The engine is
`ReplacingMergeTree(generated_at)` keyed on
`(window_start, event_type, status, country, device)`, so re-running the
same partition idempotently replaces the old aggregates on the next merge.

## Schedule

```python
build_schedule_from_partitioned_job(hourly_analysis_job, minute_of_hour=5)
```

The `:05` offset gives the sink a few seconds past the hour boundary to
flush late arrivals before the read kicks off.

## DLQ replay

Phase 3 documented `make dlq-peek`; Phase 4 ships `dlq_replay_job` — a
Dagster job (not asset) that reads up to `max_messages` records from the
DLQ and re-emits them to the source topic using a **dedicated Kafka
principal `User:replay`** with:

- `Read`, `Describe` on `user-interactions-dlq`
- `Write`, `Describe`, `IdempotentWrite` on `user-interactions`
- `Read`, `Describe` on consumer group `dlq-replay`

All other operations (including writing to any other topic) fail closed.

Launch from the Dagster UI with a config like:

```yaml
ops:
  replay_dlq:
    config:
      max_messages: 100
      group_id: dlq-replay
```

Replay is safe under Phase 3's guarantees *after the original failure cause
has been fixed*: `ReplacingMergeTree` dedups by `event_id`, and
`exactlyOnce=true` on the secure-stack sink closes the at-least-once window
at commit time. Don't replay if the DLQ contains genuinely poisoned Avro —
those will just bounce back to the DLQ.

## API surface

`GET /api/v1/analytics/top-errors?hours=24&limit=10` reads
`analysis_hourly FINAL`, groups by cohort, and returns the N cohorts with
the most errors. Empty until Dagster has materialized at least one
partition. See [services/api/src/api/main.py](../services/api/src/api/main.py).

## Operator targets

```bash
make up            # dev stack — Dagster is on http://localhost:3000
make up-secure     # secure stack — SASL_SSL Kafka wiring for DLQ replay

make dagster-url   # print the UI URL
make minio-url     # print the MinIO console URL + creds location

make analysis-run  # kick the hourly_analysis_job via the Dagster GraphQL API

make test          # now also runs orchestrator unit tests
```

## What's deliberately left for Phase 5

- **Emit OTel traces / metrics from Dagster runs.** Once Phase 5 stands up
  the Collector and dashboards, each op will export spans + duration
  histograms keyed by asset and partition.
- **Alert on scheduled-run failure.** Needs a metrics backend.
- **Postgres for Dagster storage.** `dagster dev` + SQLite works fine for
  a single container; a multi-container Dagster deploy (separate webserver
  / daemon / code-server) needs Postgres for concurrent writes. Swap the
  three `*SqliteStorage` blocks in `dagster.yaml` for the `dagster_postgres`
  equivalents and add a `postgres` service.
- **Scoped MinIO service accounts.** We currently reuse the root credential
  as the S3 access/secret pair. MinIO supports `mc admin user add` +
  policies; that becomes worthwhile once more than one service writes
  buckets.
