# Observability (Phase 5)

Phase 5 gives the pipeline three pillars — **traces**, **metrics**,
**logs** — plumbed through a single OpenTelemetry Collector and surfaced
in Grafana. It layers on top of the Phase 4 stack without changing how
the data pipeline itself runs.

## Topology

```
┌──────────┐      OTLP/gRPC     ┌──────────────────┐    ┌────────┐
│ producer │ ─────────────────▶ │                  │───▶│ Tempo  │  (traces)
│  api     │                    │  OTel Collector  │───▶│ Prom   │  (metrics)
│  dagster │                    │                  │───▶│ Loki   │  (logs/OTLP)
└──────────┘                    └──────────────────┘    └────────┘
                                          ▲                  ▲
                      ┌───────────────────┤                  │
                      │                   │                  │
              ┌───────┴────────┐   ┌──────┴────────┐  ┌──────┴────────┐
              │ ClickHouse     │   │ kafka-exporter│  │  Promtail     │
              │ /metrics :9363 │   │ (scraped)     │  │ (docker logs) │
              └────────────────┘   └───────────────┘  └───────────────┘

                                                         ┌───────────┐
                                                         │  Grafana  │  one pane
                                                         └───────────┘
```

All observability containers speak on the same Docker network as the
pipeline services, so there's no host-port coupling between, say, the
API and the Collector — services emit to `otel-collector:4317` via
container DNS.

## What each service emits

| Service      | Traces                                   | Metrics                                                | Logs                      |
|--------------|------------------------------------------|--------------------------------------------------------|---------------------------|
| `producer`   | *n/a* (hot loop — would be too noisy)    | `producer.events.sent`, `producer.delivery.failed`     | JSON via stdout (→ Loki)  |
| `api`        | per-request spans from `FastAPIInstrumentor` | `http.server.request.duration` (histogram, auto)   | JSON via stdout           |
| `dagster`    | one span per asset materialization       | `dagster.asset.duration`, `dagster.asset.runs`         | JSON via stdout           |
| `clickhouse` | –                                        | scraped natively on `/metrics:9363` (Prometheus endpoint) | stdout → Loki          |
| `kafka`      | –                                        | `kafka_topic_*`, `kafka_consumergroup_lag` via `kafka-exporter:9308` | stdout → Loki |

The Python OTel setup modules (`services/*/src/*/obs.py`) are env-driven:
if `OTEL_EXPORTER_OTLP_ENDPOINT` is unset they install no-op providers so
unit tests and bare CLI runs stay zero-dependency. Compose sets it on
every service to `http://otel-collector:4317`.

## Trace ↔ log correlation

Every structlog line gets `trace_id` / `span_id` injected when a span is
active. In Grafana, Loki's *derived fields* turn the trace_id into a
clickable link back to Tempo; from Tempo you can jump forward to Loki
using `tracesToLogsV2`. This is the usual reason for stitching the three
systems together.

## Pre-provisioned dashboards

Grafana picks up three JSON dashboards at startup (`infra/observability/grafana/dashboards/`):

| Dashboard                | Key panels                                                     |
|--------------------------|----------------------------------------------------------------|
| **Pipeline — Overview**  | Producer throughput, API req-rate, API p95, Dagster asset p95, recent error logs |
| **Pipeline — Kafka & Connect** | Topic in-rate, consumer-group lag, DLQ size, broker count, Connect logs |
| **Pipeline — ClickHouse** | Insert/SELECT rate, active parts, ongoing merges, resident memory |

They query Prometheus / Tempo / Loki via provisioned datasources — no
import step needed.

## Metrics path

All OTLP metrics land in the Collector's `prometheus` exporter on
`:8889`. Prometheus scrapes that one endpoint for everything, rather
than scraping each service separately. The advantage: adding a new
service only requires pointing it at the Collector, not editing the
scrape config.

External sources (ClickHouse, `kafka-exporter`) are scraped directly by
Prometheus, since they already speak the Prometheus format.

## Operator targets

```bash
make grafana-url        # print the Grafana URL + creds location
make otel-health        # confirm Collector is alive (via Prometheus `up`)
make traces SERVICE=api # last 10 trace IDs for a given service
```

## Secure-stack differences

- Grafana uses `GF_SECURITY_ADMIN_PASSWORD__FILE` (Docker secret) instead
  of the dev plaintext env var; anonymous access is disabled.
- `kafka-exporter` uses the `admin` SCRAM principal over SASL_SSL. The
  ACLs authorize it because `admin` is in `super.users`.
- Prometheus is not published to the host. Use Grafana or `docker exec`
  to query it.
- The Collector's OTLP endpoint is *not* authenticated — it's only
  reachable inside the Docker network. If the Collector ever needs to
  cross a trust boundary (e.g. a sidecar in a separate host), switch to
  OTLP/HTTP with a bearer token or mTLS (the Collector supports both
  out of the box).

## What's deliberately out of scope

- **Alerting.** Grafana can alert off the same queries; wire it when
  you have a paging rotation to target.
- **Tracing through Kafka.** Would need the producer to inject W3C
  trace context into Kafka message headers and the sink connector to
  read them (no built-in support). Not worth the SMT complexity at
  this stage.
- **APM-style exception capture.** `OTLP logs` gets us *error-level*
  entries with trace context; richer exception signatures (Sentry-style)
  are out of scope.
- **Cost controls.** Loki/Tempo retention is set to 24h in dev. For
  long-term keep, swap the filesystem backend for S3 + a compaction
  policy — both support it upstream; it's config, not re-architecture.
