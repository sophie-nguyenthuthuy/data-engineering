# Lakehouse Migration

Migrate a legacy data warehouse to a Delta Lake / Apache Iceberg architecture with time-travel queries, ACID transactions, and incremental ingestion patterns.

## Architecture

```
Legacy DWH (Hive/Redshift/Snowflake/RDBMS)
        │
        ▼
┌───────────────────────────────────────────┐
│              Bronze Layer                 │  Raw ingestion (Append-only)
│   Delta / Iceberg tables (raw_events,    │
│   raw_transactions, raw_customers …)      │
└───────────────────┬───────────────────────┘
                    │ Incremental / CDC
                    ▼
┌───────────────────────────────────────────┐
│              Silver Layer                 │  Cleaned, deduped, typed
│   ACID MERGE (SCD Type-2 / Upsert)       │
└───────────────────┬───────────────────────┘
                    │ Aggregations
                    ▼
┌───────────────────────────────────────────┐
│               Gold Layer                  │  Business-ready aggregates
│   Materialized views, dim/fact tables     │
└───────────────────────────────────────────┘
```

## Key Capabilities

| Feature | Implementation |
|---|---|
| ACID transactions | Delta Lake `MERGE INTO` / Iceberg `MERGE` |
| Time travel | `VERSION AS OF` / `TIMESTAMP AS OF` |
| Incremental ingestion | Watermark + CDC (Debezium / Spark Structured Streaming) |
| Schema evolution | `TBLPROPERTIES('delta.columnMapping.mode'='name')` |
| Partition pruning | Z-ORDER clustering / Iceberg hidden partitioning |
| Compaction | `OPTIMIZE` + `VACUUM` / Iceberg `rewrite_data_files` |

## Stack

- **Compute**: Apache Spark 3.5 (PySpark)
- **Table format**: Delta Lake 3.x *or* Apache Iceberg 1.5
- **Orchestration**: Apache Airflow 2.x
- **Catalog**: Unity Catalog / AWS Glue / Nessie
- **Storage**: S3 / ADLS / GCS

## Quickstart

```bash
# Install dependencies
pip install -r requirements.txt

# Copy and configure environment
cp config/env.example.yaml config/env.yaml

# Run the full migration pipeline (dry-run)
python scripts/run_migration.py --env dev --dry-run

# Run incremental ingestion
python scripts/run_incremental.py --table transactions --batch-date 2024-01-15
```

## Project Layout

```
lakehouse-migration/
├── src/
│   ├── ingestion/      # Full-load and incremental readers
│   ├── transformation/ # Bronze → Silver → Gold transforms
│   ├── schema/         # Schema registry + evolution helpers
│   └── utils/          # Spark session factory, logging, config
├── sql/
│   ├── ddl/            # CREATE TABLE statements (Delta / Iceberg)
│   ├── queries/        # Time-travel & audit queries
│   └── migrations/     # Versioned schema migration scripts
├── config/             # Environment configs
├── tests/              # Unit + integration tests
├── notebooks/          # Exploratory / validation notebooks
├── scripts/            # CLI entry-points
└── docs/               # Architecture decisions, runbooks
```
