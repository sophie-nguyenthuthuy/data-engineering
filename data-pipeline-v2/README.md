# data-pipeline-v2

Mock API → Postgres → dbt → Superset, orchestrated by Airflow. Rewrite of the
original Airflow/dbt/Postgres/Power BI stack with incremental extraction, a
medallion layout, per-model Airflow tasks via Cosmos, and an open-source BI
layer that actually runs locally.

## Architecture

```
          +-------------+    extract (incremental, watermarked)
          |  mock-api   |-----------------------+
          | (FastAPI)   |                       v
          +-------------+                  +---------+
                                           | raw.*   |
                                           +----+----+
                                                |
                                      dbt build (Cosmos, per-model tasks)
                                                |
                              +-----------------+-----------------+
                              v                 v                 v
                          bronze.*          silver.*           gold.*
                            views       incremental tables   BI tables
                                                                |
                                                                v
                                                  +-------------------------+
                                                  | Superset (bi_read user) |
                                                  +-------------------------+
```

Orchestrator: Airflow 2.10 (LocalExecutor, Postgres metadata DB).
Warehouse: Postgres 16.
Transform: dbt-core 1.8, rendered into Airflow as one task per model by [astronomer-cosmos](https://astronomer.github.io/astronomer-cosmos/).
BI: Superset 4.

## What's different from v1

- **Incremental extraction** with a `raw._watermarks` table — no full refresh every run.
- **Medallion layout**: `bronze` (typed views) → `silver` (incremental + joined + deduped) → `gold` (BI tables).
- **Per-model Airflow tasks** via Cosmos so a single failing dbt model doesn't mask the others.
- **Data quality gates**: dbt `not_null`/`unique`/`relationships`/`accepted_values` tests, source freshness, and custom singular tests (see [dbt/tests](dbt/tests)).
- **Read-only BI user** (`bi_read`) with `SELECT` on `gold` + `silver` only, plus default privileges for future tables.
- **Superset** instead of Power BI — open source, boots with an admin user and the analytics DB pre-registered.
- **Makefile** for the common commands.

## Quickstart

```bash
# 1. bootstrap env file
make init

# 2. generate real secrets and paste them into .env
make keys

# 3. build and start the stack
make up

# 4. watch it come up (Airflow init runs once, then web + scheduler)
make logs
```

Once services report healthy:

- Airflow: <http://localhost:8080> (user/pass from `.env`)
- Superset: <http://localhost:8088> (user/pass from `.env`)
- Mock API: <http://localhost:8000/docs>
- Postgres: `localhost:5432` (see `.env` for credentials)

Trigger the pipeline on demand:

```bash
make trigger
```

Simulate fresh activity so the next run has incremental work:

```bash
curl -X POST "http://localhost:8000/orders/tick?n=50"
```

## Repo layout

```
airflow/     Airflow image + DAGs (orders_pipeline.py)
mock_api/    FastAPI mock with pagination and updated_since watermarks
dbt/         dbt project: models/{bronze,silver,gold}, tests/, sources
postgres/    init SQL: users, dbs, schemas, grants, default privileges
superset/    Superset image + bootstrap that registers the analytics DB
docker-compose.yaml
Makefile
```

## dbt

Run ad-hoc against a running stack:

```bash
make dbt-run    # dbt run
make dbt-test   # dbt test (includes source freshness)
make dbt-docs   # dbt docs generate + serve on :8001
```

Models:

| layer  | model                      | purpose                                                |
| ------ | -------------------------- | ------------------------------------------------------ |
| bronze | `bronze_customers`         | typed passthrough of `raw.customers`                   |
| bronze | `bronze_products`          | typed passthrough of `raw.products`                    |
| bronze | `bronze_orders`            | typed passthrough of `raw.orders`                      |
| silver | `silver_customers`         | incremental, deduped on `customer_id`                  |
| silver | `silver_products`          | incremental, deduped on `product_id`                   |
| silver | `silver_orders`            | incremental, deduped, joined to customer + product     |
| gold   | `gold_daily_revenue`       | daily revenue by category (primary Superset source)    |
| gold   | `gold_product_performance` | rolling 30-day revenue + AOV per product               |
| gold   | `gold_customer_ltv`        | lifetime spend and order count per customer            |

## Connecting Superset

The Superset bootstrap script registers the `analytics` database automatically
using the `bi_read` user. In the UI you'll find it under **Data → Databases**
as `analytics`. Create datasets from any table in the `gold` or `silver`
schemas and build dashboards from there.

## Cleanup

```bash
make down     # stop containers, keep volumes
make nuke     # stop + wipe all volumes (DESTRUCTIVE)
```

## Notes / next steps

- Secrets live in `.env`. For real deployment, move them to a secret manager and
  switch Airflow to a proper executor (Celery / Kubernetes).
- Add [elementary](https://www.elementary-data.com/) for anomaly detection on
  top of the existing dbt tests.
- Wire `dbt source freshness` into the DAG as its own task if you want freshness
  failures to page the on-call instead of showing up in `dbt test` output.
