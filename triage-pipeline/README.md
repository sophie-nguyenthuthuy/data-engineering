# Email Triage — Capable Cloud-Shaped Pipeline

![CI](https://github.com/OWNER/triage-pipeline/actions/workflows/ci.yml/badge.svg)
![License](https://img.shields.io/badge/license-MIT-blue.svg)
![Python](https://img.shields.io/badge/python-3.11%2B-blue)

A "better" version of the Gmail → dlt → BigQuery → OpenAI → Slack (on Kestra)
workflow. Shape preserved; every external dependency is a **stub** behind a narrow
swap-in interface so you can run the whole thing locally, then point it at real
GCP / Slack / Anthropic / Gmail with ~50 lines of changes per service.

## What's different vs. the diagram

| Original            | Here                                                       | Why |
|---------------------|------------------------------------------------------------|-----|
| Kestra orchestrator | Lightweight scheduler + YAML DAG (`config/pipeline.yaml`)  | Same shape, one process, no JVM |
| dlt ingestion       | `src/stubs/gmail.py` → `src/ingest.py`                     | Narrow `iter_new_messages` contract |
| (implicit queue)    | **Pub/Sub stub with ack deadlines, retry counter, DLQ**    | Explicit reliability primitive |
| BigQuery            | DuckDB with BQ-shaped DDL (`emails_raw`, `emails_processed`, `runs`, `eval_results`) | Same SQL dialect, zero cost |
| OpenAI              | **Claude** (`claude-haiku-4-5-20251001`) with a deterministic mock fallback | Better triage reasoning, cheap |
| Slack webhook       | JSONL outbox replay-able in dashboard                      | Verifiable without leaking to prod |
| —                   | **Multi-tenant JWT auth** (tenant pinned in token, never in args) | Can't accidentally cross tenants |
| —                   | **Web dashboard** — metrics, messages, DLQ, runs, eval     | Operability baked in |
| —                   | **Eval harness** with golden set → per-label P/R/F1        | LLM output quality is measurable |

## Run

```bash
cd triage_pipeline
pip install -r requirements.txt

python run.py seed       # ingest → process → eval once
python run.py serve      # dashboard + background scheduler on http://127.0.0.1:8899
```

Seeded admins (password `changeme`): `acme-admin`, `globex-admin`.

## Layout

```
triage_pipeline/
├── config/pipeline.yaml      # Kestra-shaped DAG + tenants + retry/backoff
├── run.py                    # CLI: serve | seed | ingest | process | eval
├── src/
│   ├── config.py             # yaml loader, DATA_DIR
│   ├── auth.py               # PBKDF2 users, JWT, tenant-scoped deps
│   ├── ingest.py             # Gmail → Pub/Sub publish
│   ├── worker.py             # Pub/Sub pull → Claude → BQ → Slack, retry+DLQ
│   ├── orchestrator/scheduler.py   # background ingest + worker loops
│   ├── eval/                 # golden.json + harness.py (P/R/F1 per label)
│   ├── dashboard/            # FastAPI app + templates + static
│   └── stubs/
│       ├── gmail.py          # synthetic emails, 8% poison pills for DLQ demo
│       ├── pubsub.py         # DuckDB-backed queue, ack deadline, DLQ routing
│       ├── warehouse.py      # BQ-shaped DuckDB tables
│       ├── llm.py            # Claude (real) or deterministic mock
│       └── slack.py          # JSONL outbox
└── tests/test_smoke.py       # ingest → process → eval, asserts rows land
```

## Swapping stubs for real services

- `stubs/gmail.py::iter_new_messages` → `googleapiclient.discovery.build('gmail','v1').users().messages().list(...)`
- `stubs/pubsub.py` → `google.cloud.pubsub_v1.PublisherClient / SubscriberClient` (same publish/pull/ack/nack verbs)
- `stubs/warehouse.py` → `google.cloud.bigquery.Client` (schema already BQ-compatible)
- `stubs/llm.py::_claude_classify` is already the real Anthropic SDK path — set `ANTHROPIC_API_KEY` and `TRIAGE_USE_REAL_LLM=1`
- `stubs/slack.py::post` → `httpx.post(webhook_url, json=...)`

## Reliability model

- Worker leases messages for `ack_deadline_seconds`; stale leases expire and re-enter the pending pool.
- Every delivery increments `delivery_count`. On exception the worker nacks; at `max_retries` the message is routed to the DLQ topic.
- Exponential backoff between retries: `backoff_base * 2^min(attempts-1, 4)`.
- The Gmail stub injects ~8% malformed bodies so you can see the DLQ fill up in real time.

## Eval harness

`src/eval/harness.py` runs the classifier against `golden.json`, writes per-label
precision/recall/F1 + a confusion matrix into `eval_results` and `runs`. The
dashboard's **Eval scores** tab trends these over time — so model or prompt
changes are measurable before they hit prod.
# triage-pipeline
