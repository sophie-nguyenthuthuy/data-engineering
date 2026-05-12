# Roadmap

Phased production hardening. Each phase is a self-contained commit that
leaves the stack in a runnable state.

## ✅ Phase 1 — MVP (done)
- producer → Kafka → Connect → ClickHouse → FastAPI → nginx dashboard
- Idempotent producer, Avro via Schema Registry, BACKWARD compat
- DLQ topic *provisioned* (policy tuning in Phase 3)
- AggregatingMergeTree 1-minute roll-up with t-digest p95
- Compose healthchecks + ordered startup
- Makefile + quickstart + unit tests for producer and API

## ✅ Phase 2 — Security (done)
- Kafka listeners (INTERNAL + EXTERNAL) → SASL_SCRAM-SHA-512 over TLS with
  a local CA; CONTROLLER stays PLAINTEXT on loopback for KRaft
- Schema Registry → SASL_SSL to Kafka, HTTP basic auth via Jetty
  `PropertyFileLoginModule`
- Kafka Connect → SASL_SSL for worker + producer + consumer + admin;
  sink uses SR basic auth + `producer.override.*` SASL for DLQ writes
- ClickHouse → `default` removed, two named users (`pipeline` write-only,
  `api_ro` readonly) via `users.d/pipeline.xml`, `password_sha256_hex`
- Docker secrets for every password; services read `FOO_FILE` over `FOO`
- `scripts/bootstrap-secrets.sh` generates CA, broker cert, PKCS12
  truststore, per-identity SCRAM + basic-auth passwords, JAAS configs, and
  users.xml. Idempotent.
- Standalone `docker-compose.secure.yml` (does NOT extend the dev compose
  — cleaner given how much changes)
- Full rotation flow documented in [../docs/SECURITY.md](../docs/SECURITY.md)

## ✅ Phase 3 — Reliability (done)
- `ReplacingMergeTree(ingested_at)` on `events.user_interactions`, ORDER BY
  `(event_type, occurred_at, event_id)`; `event_id` is globally unique so
  duplicates collapse on merge. `make ch-dedup` target forces the merge
  via `OPTIMIZE TABLE … FINAL DEDUPLICATE`
- Sink connector: `errors.retry.timeout=300000`, `errors.retry.delay.max.ms=30000`
  on both stacks; `exactlyOnce=true` on the secure stack so offsets commit
  in the same transaction as the data
- Producer-side schema compatibility pre-flight (`schema_check.ensure_compatible`)
  calls Schema Registry `test_compatibility` against the local `.avsc` and
  fails fast before `produce()` — no more silent incompatible registrations
- Kafka ACLs via `StandardAuthorizer` + `allow.everyone.if.no.acl.found=false`;
  `infra/kafka/acls.sh` provisions per-principal grants (producer, connect,
  schemaregistry) idempotently at init time
- DLQ inspection target `make dlq-peek` with `print.headers=true` so
  `__connect.errors.*` context is visible
- Full writeup in [../docs/RELIABILITY.md](../docs/RELIABILITY.md)

*Deferred:* DLQ replay pipeline → Phase 4 (Dagster asset); API-side
PyBreaker circuit-breaker → Phase 5 with the rest of the observability
wiring (the breaker needs something to export "open" events to).

## ✅ Phase 4 — Orchestration (done)
- MinIO + `minio-init` bucket provisioning service on both stacks
- `services/orchestrator/` Dagster package with three hourly-partitioned
  assets: `raw_events_parquet` → `event_analysis` (PySpark local[*]) →
  `analysis_report` → `events.analysis_hourly` (ReplacingMergeTree)
- Hourly schedule at `:05` via `build_schedule_from_partitioned_job`
- Spark reads MinIO through the S3A filesystem; jars pulled once into an
  `ivy-cache` named volume via `spark.jars.packages`
- `dlq_replay_job` (Dagster job, not asset) with a dedicated Kafka
  `User:replay` principal — Read on DLQ, Write on source, nothing else
- New `User:replay` SCRAM user provisioned by `bootstrap-secrets.sh` and
  authorized by `infra/kafka/acls.sh`
- New API endpoint `/api/v1/analytics/top-errors` backed by
  `analysis_hourly`
- `analysis_hourly` ClickHouse table added to `init.sql`
- `make dagster-url`, `make minio-url`, `make analysis-run`,
  `make test-orchestrator` targets
- Writeup in [../docs/ORCHESTRATION.md](../docs/ORCHESTRATION.md)

*Deferred:* telemetry on Dagster runs → Phase 5 (needs the Collector and
dashboards). Dagster-on-Postgres and scoped MinIO service accounts are
annotated in the doc as straightforward follow-ups when multi-container
Dagster or multi-writer object store becomes relevant.

## ✅ Phase 5 — Observability (done)
- OpenTelemetry SDK in `producer` + `api` + `dagster` with a shared
  `obs.py` init module per service; env-driven so tests stay zero-dep
- OTel Collector fan-out: traces → Tempo, metrics → Prometheus (via the
  Collector's prometheus exporter), logs → Loki (OTLP/HTTP)
- Structlog in all three Python services injects `trace_id` / `span_id`
  so log lines link back to the originating span
- Promtail tails Docker container stdout so non-OTel containers (Kafka,
  Connect, Schema Registry, ClickHouse, MinIO) also land in Loki
- ClickHouse native `/metrics:9363` endpoint exposed via
  `config.d/prometheus.xml`; `kafka-exporter` for topic rates +
  consumer-group lag (SASL_SSL-aware on the secure stack)
- Three pre-provisioned Grafana dashboards:
  - *Pipeline — Overview* (producer throughput, API p95, Dagster asset
    duration, recent error log stream)
  - *Pipeline — Kafka & Connect* (topic in-rate, consumer-group lag,
    DLQ size, Connect logs)
  - *Pipeline — ClickHouse* (insert/select throughput, parts, merges)
- Secure stack: Grafana behind `GF_SECURITY_ADMIN_PASSWORD__FILE`
  (Docker secret); Prometheus not exposed to the host; `kafka-exporter`
  authenticates over SASL_SSL as `admin`
- `make grafana-url`, `make otel-health`, `make traces` targets
- Writeup in [../docs/OBSERVABILITY.md](../docs/OBSERVABILITY.md)

*Deferred:* Grafana alerting rules + paging integration — needs a
target rotation; trace propagation through Kafka (would require header
injection in the producer *and* a Connect SMT that preserves
`traceparent` in the sink path); S3-backed Loki/Tempo for multi-day
retention (config-only swap, not re-architecture).

## ✅ Phase 6 — CI (GitHub Actions, done)
- Three workflows: `ci.yml` (lint/type/test matrix + compose-config +
  Grafana dashboard JSON parse), `containers.yml` (Hadolint + Trivy fs
  + Trivy image per service, SARIF → Security tab), `smoke.yml`
  (compose up → /healthz ok → ClickHouse row count > 100)
- Matrix: `{producer, api, orchestrator} × {ruff, mypy, pytest}` —
  orchestrator/mypy excluded (Dagster + PySpark stubs too unstable)
- Smoke test is label-gated on PRs (`smoke` label) to control CI spend,
  always-on for `main`
- `dependabot.yml` with per-service pip groups, OTel/Dagster grouped,
  plus Docker + GitHub Actions
- `.pre-commit-config.yaml`: whitespace, YAML/JSON syntax, ruff +
  ruff-format, shellcheck, detect-secrets (with baseline)
- Full writeup in [../docs/CI.md](../docs/CI.md)

*Deferred:* CodeQL (too noisy on Python + native deps), image publishing
to GHCR/ECR (no deploy target yet — lands with Phase 7), and marking
`smoke.yml` as a required status check (by design it doesn't run on
every PR).

## ✅ Phase 7 — Terraform (AWS, done)
- `infra/terraform/modules/` tree:
  - `network` (VPC + 2/3 AZ public/private subnets, NAT, S3 gateway endpoint)
  - `kafka` (MSK Serverless, IAM auth on :9098)
  - `storage` (S3 raw + analysis buckets with SSE/versioning/lifecycle,
    Glue database + `raw_events` table matching the Dagster Parquet layout)
  - `analytics` (RDS Postgres for Dagster metadata + Secrets Manager entry)
  - `compute` (ECR repos, ECS cluster, Fargate task defs + services for
    producer and api, ALB in front of api, scoped task IAM roles —
    producer gets `kafka-cluster:Write*`, api gets S3 R/W scoped to the
    two buckets, exec role gets the DB secret)
  - `observability` (Amazon Managed Prometheus workspace + Managed
    Grafana with PROMETHEUS + CLOUDWATCH data sources, IAM service role)
  - `iam` (GitHub OIDC provider + `*-gha-plan` ReadOnlyAccess role and
    `*-gha-apply` PowerUserAccess role, both trust-policy-scoped to
    `repo:<owner>/<repo>` sub claims)
- `envs/dev` and `envs/prod` roots — same module wiring, different
  defaults (dev: 2 AZs + `force_destroy` + `skip_final_snapshot`; prod:
  3 AZs + destroy-safety on + larger instances + `api_desired_count=3`)
- S3 + DynamoDB remote backend with per-env state keys
- `.github/workflows/terraform.yml`: `fmt -check -recursive` +
  `validate` on both envs for every PR; `plan` on PRs when
  `AWS_PLAN_ROLE_ARN` repo var is set; `apply` is `workflow_dispatch`-only
  and routes through GitHub Environments for approval gating
- Writeup in [../docs/TERRAFORM.md](../docs/TERRAFORM.md) with the
  local→AWS mapping table, bootstrap commands, cost napkin math, and the
  list of deliberately-deferred pieces

*Deferred:* ClickHouse on AWS (separate decision: Cloud vs self-host on
EKS); the ADOT Collector sidecar in the ECS task defs so metrics actually
land in AMP (endpoint is already output, just needs wiring); TLS on the
ALB (needs a domain + ACM cert); Dagster-on-Fargate (RDS is waiting);
Kafka Connect (depends on ClickHouse choice); image publishing to ECR
from CI; CloudFront + WAFv2; Route53 records.
