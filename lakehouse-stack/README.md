# Lakehouse Stack — Data Engineering Bootcamp

A self-contained 17-lab Data Engineering bootcamp environment. Everything runs locally in Docker with a single `docker compose up`.

## Prerequisites

- **Docker Desktop** 4.x+ with **8 GB RAM minimum** (16 GB recommended for streaming + DataHub labs).
- **Python 3.11** (3.13 isn't supported by some pinned wheels — use 3.10/3.11).
- `git`, `curl`, `jq` in your shell.

## Quick start

```bash
git clone https://github.com/sophie-nguyenthuthuy/lakehouse-stack.git
cd lakehouse-stack
docker compose up -d
```

That's it. First boot pulls images (~5 min) and waits for healthchecks. After it returns, all 14 services are wired up:

| Service             | Port   | UI / endpoint                          | Used in lab |
|---------------------|--------|----------------------------------------|-------------|
| Postgres (source)   | 5432   | `psql -U de_user -d de_db`             | 1, 2, 3, 6, 11 |
| Hive Metastore DB   | 5435   | internal                               | 7 |
| MinIO (S3)          | 9000   | http://localhost:9001 (`minio` / `minio12345`) | 7, 8, 9, 12 |
| Hive Metastore      | 9083   | Thrift                                 | 7 |
| Trino               | 8081   | http://localhost:8081                  | 7 |
| Spark               | 4040   | `docker exec spark spark-submit …`     | 8, 9, 12, 14 |
| Kafka broker        | 9092   | —                                      | 10, 11, 12 |
| Kafka UI            | 8082   | http://localhost:8082                  | 10, 11, 12 |
| Kafka Connect       | 8083   | http://localhost:8083                  | 11 |
| Airflow webserver   | 8085   | http://localhost:8085 (`airflow` / `airflow`) | 13, 15 |
| Airflow metadata DB | 5434   | internal                               | 15 |
| Metabase            | 3000   | http://localhost:3000                  | 6 |

DataHub (Lab 15) runs as a **separate** stack — see [`lab_guides/lab15.md`](lab_guides/lab15.md). It conflicts on port 9092, so stop bootcamp Kafka before starting DataHub.

## Where do I start?

[`lab_guides/README.md`](lab_guides/README.md) — index of all 17 labs with the recommended order.

| Lab | Topic                                | Guide                                       |
|-----|--------------------------------------|---------------------------------------------|
| 01  | Big Data Engineer Overview           | [lab_guides/lab01.md](lab_guides/lab01.md)  |
| 02  | SQL Fundamentals → Advanced          | [lab_guides/lab02.md](lab_guides/lab02.md)  |
| 03  | Data Modeling for Analytics          | [lab_guides/lab03.md](lab_guides/lab03.md)  |
| 04  | Python for Data Engineering          | [lab_guides/lab04.md](lab_guides/lab04.md)  |
| 05  | Unix / Linux + Shell Scripting       | [lab_guides/lab05.md](lab_guides/lab05.md)  |
| 06  | ETL / ELT & Data Warehouse           | [lab_guides/lab06.md](lab_guides/lab06.md)  |
| 07  | Data Lakehouse Architecture          | [lab_guides/lab07.md](lab_guides/lab07.md)  |
| 08  | Spark Batch Processing               | [lab_guides/lab08.md](lab_guides/lab08.md)  |
| 09  | Table Formats & Optimization         | [lab_guides/lab09.md](lab_guides/lab09.md)  |
| 10  | Apache Kafka Fundamentals            | [lab_guides/lab10.md](lab_guides/lab10.md)  |
| 11  | Change Data Capture with Debezium    | [lab_guides/lab11.md](lab_guides/lab11.md)  |
| 12  | Streaming Processing                 | [lab_guides/lab12.md](lab_guides/lab12.md)  |
| 13  | Airflow Orchestration                | [lab_guides/lab13.md](lab_guides/lab13.md)  |
| 14  | Data Quality (GE + Deequ)            | [lab_guides/lab14.md](lab_guides/lab14.md)  |
| 15  | Data Governance (DataHub)            | [lab_guides/lab15.md](lab_guides/lab15.md)  |
| 16  | Feature Store (Feast)                | [lab_guides/lab16.md](lab_guides/lab16.md)  |
| 17  | Cloud Integration (AWS)              | [lab_guides/lab17.md](lab_guides/lab17.md)  |

## Verifying the stack is healthy

```bash
docker compose ps           # all should be Up (healthy)
curl -s -o /dev/null -w "trino=%{http_code}\n"   http://localhost:8081
curl -s -o /dev/null -w "minio=%{http_code}\n"   http://localhost:9001
curl -s -o /dev/null -w "airflow=%{http_code}\n" http://localhost:8085/health
curl -s -o /dev/null -w "connect=%{http_code}\n" http://localhost:8083/connectors
curl -s -o /dev/null -w "kafkaui=%{http_code}\n" http://localhost:8082
curl -s -o /dev/null -w "metabase=%{http_code}\n" http://localhost:3000/api/health
```

All should return `200`.

## Common operations

```bash
docker compose logs -f <service>          # tail logs of one service
docker compose restart <service>          # restart one service
docker compose down                       # stop everything (data persists)
docker compose down -v                    # nuclear — wipes volumes
docker compose up -d --build airflow-webserver airflow-scheduler  # rebuild Airflow image (after changing airflow/Dockerfile)
```

## Repo layout

```
.
├── docker-compose.yml         # all services + healthchecks + dependencies
├── airflow/
│   ├── Dockerfile             # bootcamp/airflow:2.8.0-datahub (plugin baked in)
│   └── dags/
│       └── lab13_pipeline.py  # 5-task DAG with DataHub lineage annotations
├── hive/conf/                 # core-site.xml, metastore-site.xml
├── trino/                     # catalog defs (lakehouse, postgres)
├── spark/apps/
│   ├── spark_batch_job.py     # Lab 08: raw → silver → gold
│   ├── lab09_job.py           # Lab 09: partitioning + clustering
│   ├── stream_orders.py       # Lab 12: structured streaming
│   └── dq_metrics.py          # Lab 14: Deequ-style metrics
├── recipes/                   # DataHub ingestion recipes (postgres, kafka, airflow)
├── lab_guides/                # Vietnamese student walkthroughs (1-17)
├── lab02_queries.sql .. lab07_setup.sql   # per-lab SQL scaffolds
├── lab04_python_etl/          # Lab 04 ETL + pytest
├── lab05_shell/               # Lab 05 shell scripts
├── feature_repo/              # Lab 16 Feast project
├── ge_validate.py             # Lab 14 Great Expectations script
├── orders.csv                 # shared sample dataset
└── pg-orders-connector.json   # Lab 11 Debezium connector config
```

## Troubleshooting

**Containers fail to start:**
- Free 8 GB RAM in Docker Desktop (Settings → Resources → Memory).
- Free ports 3000, 5432, 5434, 5435, 8081-8083, 8085, 9001, 9000, 9083, 9092 on the host.

**`Bind for 0.0.0.0:9092 failed: port is already allocated`:**
- Another Kafka (often DataHub) holds 9092. Run `datahub docker quickstart --stop` first, or stop the colliding container.

**Airflow webserver returns 500:**
- Wait for `airflow-init` to exit `0` first: `docker logs de_airflow_init`.
- If you edited `airflow/Dockerfile`, rebuild: `docker compose up -d --build airflow-webserver airflow-scheduler`.

**Connect crashes on boot with `DNS resolution failed for kafka`:**
- Bug fixed in current `docker-compose.yml` — Connect now waits for Kafka's healthcheck before starting.

**`gx/uncommitted/` showing up in `git status`:**
- It's in `.gitignore`. Run `git rm --cached -r gx/uncommitted/` if it was tracked before.

## License

For educational use only. See individual lab guides for credits to upstream projects (Spark, Kafka, Trino, Airflow, DataHub, Feast, Great Expectations).
