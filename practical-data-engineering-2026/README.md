# Practical Data Engineering — 2026 edition

![CI](https://github.com/OWNER/REPO/actions/workflows/ci.yml/badge.svg)
![Python](https://img.shields.io/badge/python-3.12-blue)
![License](https://img.shields.io/badge/license-MIT-green)

A modern, laptop-scale lakehouse that delivers the same end-to-end story as
Simon Späti's 2021 "Building a Data Engineering Project in 20 Minutes" — but
without Spark, Druid, Superset, Helm, or Kubernetes.

> Update the `OWNER/REPO` in the CI badge above after you push to GitHub.

```
scrape ─► dlt ─► Iceberg @ MinIO ─► DuckDB transforms ─► Streamlit BI
                      ▲                       ▲
                      └─ orchestrated by Dagster ─┘
```

## What's different vs. the 2021 stack

| 2021 stack            | 2026 stack           | Why better                                 |
| --------------------- | -------------------- | ------------------------------------------ |
| BeautifulSoup + custom CDC | dlt + fingerprinting | dlt handles schema evolution + merges natively |
| Spark + Delta Lake    | DuckDB + Iceberg    | Zero JVM, instant cold start, open catalog |
| Apache Druid          | DuckDB on Iceberg   | 1 process vs. 6; fast enough for < 100M rows |
| Apache Superset       | Streamlit          | Single Python file; no metadata DB         |
| Kubernetes + Helm     | Docker Compose     | One `make up`; same story, zero YAML       |
| Airflow/Dagster solids| Dagster software-defined assets | Lineage-first, not task-first   |

## Stack

- **[dlt](https://dlthub.com)** — declarative ingestion with built-in merge/CDC
- **[Apache Iceberg](https://iceberg.apache.org)** via **pyiceberg** — open table format
- **[MinIO](https://min.io)** — S3-compatible object store
- **[DuckDB](https://duckdb.org)** — transform + query engine
- **[Dagster](https://dagster.io)** — asset orchestration with medallion lineage
- **[Streamlit](https://streamlit.io)** — dashboard

## Quickstart

```bash
cd ~/practical-data-engineering-2026
make up
# once containers are healthy:
#   Dagster    http://localhost:3100
#   Streamlit  http://localhost:8501
#   MinIO UI   http://localhost:9101  (minio / minio12345)
```

In Dagster, click **Materialize all** on the asset graph — you'll see
`bronze_listings → silver_listings → silver_price_history → gold_*` run
against Iceberg on MinIO. Then refresh Streamlit.

To reset everything:

```bash
make reset
```

## Project layout

```
src/re_pipeline/
├── definitions.py         # Dagster Definitions entrypoint
├── config.py              # env-driven config (S3, catalog)
├── iceberg_utils.py       # pyiceberg + DuckDB glue
├── sources/real_estate.py # dlt source (synthetic + HTML fixture)
└── assets/
    ├── bronze.py          # dlt → Iceberg
    ├── silver.py          # deduped snapshot + price history
    └── gold.py            # city stats + price bands
dashboard/app.py           # Streamlit
data/fixtures/             # HTML fixture for the BS4 demo path
docker-compose.yml         # MinIO + Dagster + Streamlit
```

## Medallion lineage

```
bronze_listings  (Iceberg, merge on id)
      │
      ├──► silver_listings        (current snapshot, dedup latest scrape)
      └──► silver_price_history   (append-only (id, fingerprint, price, ts))
                │
                ├──► gold_city_stats   (per-city medians, feeds leaderboard)
                └──► gold_price_bands  (histogram data for dashboard)
```

Every asset is an Iceberg table on MinIO, with a SQL-backed Iceberg catalog
(SQLite in dev; swap for REST/Nessie/Glue in prod — same code).

## Scraping ethically

The `synthetic_listings` resource generates realistic data with Faker —
sufficient to exercise CDC, schema evolution, and dashboards without touching
anyone's site. `scrape_listings_from_html` demonstrates the BeautifulSoup
pattern against a local HTML fixture. If you adapt this to a live source,
check the site's robots.txt and ToS first.

## Swapping pieces in production

- **Catalog**: SQLite → Nessie, Polaris, Glue, or Unity (all work with pyiceberg)
- **Object store**: MinIO → S3, GCS, or Azure (same code; change env vars)
- **Warehouse**: DuckDB → Trino, Snowflake, ClickHouse (all read Iceberg)
- **BI**: Streamlit → Evidence, Superset, Metabase (all query Iceberg)
- **Orchestration**: keep Dagster; add branch deployments + sensors

## What's intentionally omitted

Spark, Druid, Superset, Jupyter-in-pipeline, Helm. The original's Jupyter
integration via Papermill was clever but is mostly redundant when assets
are already versioned, typed Python — run exploration in plain notebooks
against the Iceberg catalog instead.
# practical-data-engineering-2026
