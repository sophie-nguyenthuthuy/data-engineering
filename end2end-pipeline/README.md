# end2end-pipeline

Production-grade, containerized, near-real-time user-event analytics pipeline.

This is a clean rewrite of a common reference architecture (Kafka → Connect →
ClickHouse → orchestrated Spark → dashboard), with an emphasis on the
production concerns the original typically skips: security, reliability,
observability, CI, and IaC.

> **Status:** Phase 1 (MVP) + Phase 2 (security) + Phase 3 (reliability) +
> Phase 4 (orchestration) + Phase 5 (observability) + Phase 6 (CI) +
> Phase 7 (Terraform/AWS) complete. Phase 7 adds a module tree under
> `infra/terraform/` — network, kafka (MSK Serverless), storage (S3 +
> Glue), analytics (RDS for Dagster metadata), compute (ECS Fargate +
> ALB), observability (AMP + Managed Grafana), and iam (GitHub OIDC
> roles) — with `dev` and `prod` envs and a `terraform.yml` workflow
> for PR plan + manual apply. See
> [docs/SECURITY.md](docs/SECURITY.md),
> [docs/RELIABILITY.md](docs/RELIABILITY.md),
> [docs/ORCHESTRATION.md](docs/ORCHESTRATION.md),
> [docs/OBSERVABILITY.md](docs/OBSERVABILITY.md),
> [docs/CI.md](docs/CI.md),
> [docs/TERRAFORM.md](docs/TERRAFORM.md), and
> [docs/ROADMAP.md](docs/ROADMAP.md).

## Architecture (Phase 1)

```
┌──────────┐   Avro    ┌───────┐   Avro    ┌───────────────┐    sink    ┌─────────────┐
│ producer │ ────────▶ │ Kafka │ ────────▶ │ Kafka Connect │ ─────────▶ │ ClickHouse  │
└──────────┘           └───────┘           │ (CH sink)     │            └─────────────┘
      │                    ▲               └───────────────┘                    │
      │                    │                                                    │
      │             ┌──────┴────────┐                                           │
      │             │ Schema        │                                           │
      │             │ Registry      │                                           │
      │             └───────────────┘                                           │
      │                                                                         │
      ▼                                                                         │
   (DLQ topic: user-interactions-dlq)                                           │
                                                                                │
                                            ┌──────────┐       ┌──────────┐    │
                                            │  nginx   │ ◀──── │   API    │ ◀──┘
                                            │ (static) │       │ FastAPI  │
                                            └──────────┘       └──────────┘
```

Components:

| Service           | Purpose                                                  |
| ----------------- | -------------------------------------------------------- |
| `producer`        | Synthesizes user-interaction events, Avro → Kafka        |
| `kafka`           | KRaft-mode broker, auto-topic-create disabled            |
| `schema-registry` | Confluent Schema Registry, `backward` compatibility      |
| `kafka-connect`   | Confluent Connect + official ClickHouse sink connector   |
| `clickhouse`      | Columnar store (`events.user_interactions` + 1m MV)      |
| `api`             | FastAPI, queries ClickHouse, serves JSON analytics       |
| `dashboard`       | Static HTML+JS, served by nginx, auto-refreshes every 5s |

Data contract: [schemas/user_interaction.avsc](schemas/user_interaction.avsc).
The Kafka Connect sink uses the **exact same schema** via Schema Registry —
there's no hand-written ClickHouse mapping in the config.

## Quickstart

Prereqs: Docker, Docker Compose, GNU Make, OpenSSL, Python 3.

### Dev stack (plaintext — for local tinkering only)

```bash
make env        # copy .env.example to .env
make smoke      # bring up the full stack + register the sink connector
```

### Secure stack (SASL_SCRAM-SHA-512 + TLS + Docker secrets)

```bash
make bootstrap     # generate CA, broker cert, SCRAM/basic-auth passwords
make smoke-secure  # render .env.secure, bring up, register sink
```

Either way, open **http://localhost:8501** — the dashboard fills in within
10–20s as data flows end-to-end. Schema Registry on the secure stack is
behind basic auth (`admin:<cat secrets/clients/admin_password>`).

Other handy targets:

```bash
make ps                    # show service status
make logs SERVICE=producer # tail one service's logs
make connectors            # show Kafka Connect connector status
make ch-count              # rows in user_interactions
make test                  # run all unit tests
make lint                  # ruff + mypy
make clean                 # down + remove volumes (destructive)

# Secure stack
make bootstrap             # (re-)generate secrets/
make up-secure             # start SASL_SSL stack
make register-secure       # register sink with basic-auth SR + SASL
make down-secure           # stop it, keep volumes
make clean-secure          # down + remove volumes

# Reliability ops (Phase 3)
make dlq-peek              # show DLQ messages + error headers (N=50 STACK=dev)
make acls-list             # list Kafka ACLs on the secure stack
make ch-dedup              # OPTIMIZE TABLE FINAL DEDUPLICATE on user_interactions

# Orchestration (Phase 4)
make dagster-url           # print Dagster webserver URL (http://localhost:3000)
make minio-url             # print MinIO console URL + creds location
make analysis-run          # launch the hourly_analysis_job via Dagster GraphQL

# Observability (Phase 5)
make grafana-url           # print Grafana URL + creds
make otel-health           # verify the OTel Collector is up in Prometheus
make traces SERVICE=api    # latest trace IDs emitted by a given service
```

## What's in, what's next

Phase 1 lands the end-to-end happy path. What's explicitly **deferred** to
later phases — and why:

| Phase | Adds                                                           |
| ----- | -------------------------------------------------------------- |
| ✅ 2  | SASL_SCRAM + TLS for Kafka; ClickHouse users.xml; Docker secrets |
| ✅ 3  | DLQ retry policy, Kafka ACLs, schema-evolution check, ReplacingMergeTree |
| ✅ 4  | Dagster + MinIO + PySpark hourly analysis, DLQ replay job      |
| ✅ 5  | OTel traces, Prometheus metrics, Loki logs, Grafana dashboards |
| ✅ 6  | GitHub Actions: ruff/mypy/pytest, Trivy, Hadolint, compose smoke |
| ✅ 7  | Terraform modules: MSK Serverless + S3/Glue + RDS + ECS Fargate + AMP/Grafana + GHA OIDC |

See [docs/ROADMAP.md](docs/ROADMAP.md) for the full plan.

## Development layout

```
.
├── docker-compose.yml        # the whole stack
├── Makefile                  # common commands
├── schemas/                  # Avro schemas (source of truth)
├── infra/
│   ├── clickhouse/init.sql   # DB & table bootstrap
│   └── kafka-connect/        # Dockerfile + sink connector config
├── scripts/
│   └── register-connector.sh # idempotent Connect config upserter
├── services/
│   ├── producer/             # Python, confluent-kafka
│   ├── api/                  # Python, FastAPI + clickhouse-connect
│   ├── orchestrator/         # Dagster + PySpark assets (Phase 4)
│   └── dashboard/            # static HTML/JS + nginx.conf
├── infra/
│   └── terraform/            # Phase 7: AWS IaC (MSK, S3, RDS, ECS, AMP, Grafana)
└── docs/
    ├── ARCHITECTURE.md
    ├── SECURITY.md
    ├── RELIABILITY.md
    ├── ORCHESTRATION.md
    ├── OBSERVABILITY.md
    ├── CI.md
    ├── TERRAFORM.md
    └── ROADMAP.md
```

## License

MIT.
