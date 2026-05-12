# GH Archive Pipeline

End-to-end data engineering pipeline over [GH Archive](https://www.gharchive.org/) —
the firehose of every public GitHub event since 2011. Covers all seven modules of the
curriculum: infra, orchestration, warehouse, analytics, platform, batch, streaming,
plus the dlt ingestion workshop.

## Architecture

```
GH Archive hourly dumps ──► dlt ──► GCS (raw)    ──┐
(batch, historical)                                │
                                                   ├──► BigQuery (raw → staging → marts)
GitHub Events API      ──► Kafka ──► GCS (stream) ─┘                  │
(streaming, live)                                                     │
                                                                      ▼
                                                                     dbt (models/tests)
                                                                      │
                                                                      ▼
                                                                   Spark (heavy rollups)

Orchestration: Kestra (schedules, retries, dependencies, backfills)
Infra:         Terraform (GCS, BQ datasets, service account) + Docker Compose (local services)
Platform:      Bruin (single-file description of the whole pipeline)
```

## Module mapping

| Module | Tool | Where |
|---|---|---|
| 1 — Containerization & IaC | Docker, Terraform, Postgres | `infra/` + `docker-compose.yml` |
| 2 — Workflow Orchestration | Kestra | `orchestration/` |
| 3 — Data Warehouse | BigQuery | provisioned by `infra/terraform` |
| 4 — Analytics Engineering | dbt | `analytics/` |
| 5 — Data Platforms | Bruin | `platform/` |
| 6 — Batch Processing | Spark | `processing/spark/` |
| 7 — Streaming | Kafka | `streaming/` |
| Workshop — Data Ingestion | dlt | `ingestion/` |

## Prerequisites

- A GCP project with billing enabled
- `gcloud` CLI authenticated (`gcloud auth application-default login`)
- Docker Desktop (or colima)
- Python 3.11+, Terraform 1.6+, dbt-bigquery 1.7+
- A GitHub personal access token (for the Events API stream — `public_repo` scope is enough)

## One-time setup

```bash
cp .env.example .env
# edit .env — set GCP_PROJECT_ID, GCP_REGION, GITHUB_TOKEN

# 1. provision cloud infra
make tf-init tf-apply

# 2. boot local services (Kestra, Kafka, Spark, Postgres)
make up

# 3. install Python deps
make deps
```

## Running the pipeline

```bash
# Batch backfill — ingest one day of GH Archive through dlt
make ingest-batch DATE=2024-01-15

# Or kick it off via Kestra (UI at http://localhost:8080)
make kestra-trigger FLOW=gh_backfill

# Stream — live GitHub Events API → Kafka → GCS
make stream-start

# Model — run dbt against the raw data
make dbt-run

# Heavy rollup — Spark job reading from GCS
make spark-rollup DATE=2024-01-15

# Or run the whole thing via Bruin
make bruin-run
```

## Layout

```
gh-pipeline/
├── README.md                    this file
├── Makefile                     common commands
├── .env.example                 required env vars
├── docker-compose.yml           Kestra, Kafka, Zookeeper, Spark, Postgres
├── infra/
│   └── terraform/               GCS bucket + BQ datasets + service account
├── ingestion/
│   ├── dlt_batch.py             GH Archive hourly dumps → BQ raw
│   └── requirements.txt
├── orchestration/
│   └── flows/
│       ├── gh_backfill.yml      historical backfill flow
│       └── gh_daily.yml         daily incremental flow
├── streaming/
│   ├── producer.py              GitHub Events API → Kafka topic
│   ├── consumer.py              Kafka → GCS (jsonl) → BQ external table
│   └── requirements.txt
├── processing/
│   └── spark/
│       └── daily_rollups.py     Spark job: raw events → repo/user rollups
├── analytics/
│   ├── dbt_project.yml
│   ├── profiles.yml
│   └── models/
│       ├── staging/stg_events.sql
│       └── marts/
│           ├── fct_daily_activity.sql
│           ├── dim_repos.sql
│           └── dim_users.sql
└── platform/
    └── pipeline.yml             Bruin pipeline description
```

## Cost note

GH Archive is ~3–5 GB/day compressed. A month of backfill into BigQuery is
roughly 100–150 GB storage and a few dollars of query cost if you're careful
with partitioning. Everything here uses `_PARTITIONTIME` on `event_hour` to
keep scans cheap.
# gh-pipeline
