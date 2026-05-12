# Lab Guides — Data Engineering Bootcamp

Student-facing walkthroughs for all 17 labs of the bootcamp, built against the `lakehouse-stack` project. Each guide is practical: clone → start services → run commands → verify output → answer questions.

## Prerequisites (one-time)

1. Docker Desktop 4.x+ (8 GB RAM, 4 CPU minimum; 16 GB for labs 12/15).
2. Python 3.10+ and `pip` (for labs 4, 14, 16).
3. `git`, `curl`, `jq` in your shell.
4. Clone/open this repo and `cd` into the project root.

## The common stack

All labs share the same `docker-compose.yml` at the project root. You typically only need to bring up the services relevant to the lab you are doing. Port map:

| Service             | Port   | UI / endpoint                   |
|---------------------|--------|---------------------------------|
| Postgres (source)   | 5432   | `psql -h localhost -U de_user`  |
| Hive metastore DB   | 5435   | internal                        |
| MinIO (S3)          | 9000   | `http://localhost:9001` console |
| Hive Metastore      | 9083   | Thrift                          |
| Trino               | 8081   | `http://localhost:8081`         |
| Spark               | 4040   | `http://localhost:4040`         |
| Kafka broker        | 9092   | —                               |
| Kafka UI            | 8082   | `http://localhost:8082`         |
| Kafka Connect       | 8083   | `http://localhost:8083`         |
| Airflow webserver   | 8085   | `http://localhost:8085`         |
| Metabase            | 3000   | `http://localhost:3000`         |

## Lab index

| Lab | Topic                                | Guide                          |
|-----|--------------------------------------|--------------------------------|
| 01  | Big Data Engineer Overview           | [lab01.md](lab01.md)           |
| 02  | SQL Fundamentals → Advanced          | [lab02.md](lab02.md)           |
| 03  | Data Modeling for Analytics          | [lab03.md](lab03.md)           |
| 04  | Python for Data Engineering          | [lab04.md](lab04.md)           |
| 05  | Unix / Linux + Shell Scripting       | [lab05.md](lab05.md)           |
| 06  | ETL / ELT & Data Warehouse           | [lab06.md](lab06.md)           |
| 07  | Data Lakehouse Architecture          | [lab07.md](lab07.md)           |
| 08  | Spark Batch Processing               | [lab08.md](lab08.md)           |
| 09  | Table Formats & Optimization         | [lab09.md](lab09.md)           |
| 10  | Apache Kafka Fundamentals            | [lab10.md](lab10.md)           |
| 11  | Change Data Capture with Debezium    | [lab11.md](lab11.md)           |
| 12  | Streaming Processing (Spark/Flink)   | [lab12.md](lab12.md)           |
| 13  | Airflow Orchestration                | [lab13.md](lab13.md)           |
| 14  | Data Quality (GE + Deequ)            | [lab14.md](lab14.md)           |
| 15  | Data Governance & Metadata (DataHub) | [lab15.md](lab15.md)           |
| 16  | Feature Store (Feast)                | [lab16.md](lab16.md)           |
| 17  | Cloud Integration (AWS)              | [lab17.md](lab17.md)           |

## How to use a guide

Each guide follows the same shape:

1. **Objectives** — what you will walk away knowing.
2. **Services you need up** — `docker compose up -d <names>`.
3. **Steps** — numbered, copy-paste friendly commands.
4. **Expected output** — what "success" looks like so you can self-verify.
5. **Deliverables** — screenshots + a short write-up (see the matching `labNN_submission.md` for the template).
6. **Self-check** — theory questions to confirm you understood.

Tip: when a lab fails, first check `docker ps` to confirm the service is `Up (healthy)`, then read container logs with `docker logs <name>`.
