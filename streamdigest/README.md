# streamdigest

[![CI](https://github.com/sophie-nguyenthuthuy/streamdigest/actions/workflows/ci.yml/badge.svg)](https://github.com/sophie-nguyenthuthuy/streamdigest/actions/workflows/ci.yml)
[![License: MIT](https://img.shields.io/badge/License-MIT-blue.svg)](./LICENSE)
[![Python](https://img.shields.io/badge/python-3.11%20%7C%203.12-blue.svg)](https://www.python.org/)
[![Ruff](https://img.shields.io/endpoint?url=https://raw.githubusercontent.com/astral-sh/ruff/main/assets/badge/v2.json)](https://github.com/astral-sh/ruff)
[![dlt](https://img.shields.io/badge/built%20with-dlt-yellow)](https://dlthub.com)
[![Prefect](https://img.shields.io/badge/orchestrated%20by-Prefect%203-1E3A8A)](https://docs.prefect.io)

Local-first, orchestrated ingestion + AI enrichment of noisy developer event streams.
Inspired by the [dlt-kestra-demo](https://github.com/dlt-hub/dlt-kestra-demo),
but rebuilt around four goals:

1. **Modern stack** — [Prefect 3](https://docs.prefect.io) for orchestration, typed Python, pydantic settings, a CLI, a smoke test suite, and an eval harness.
2. **Smarter AI layer** — one structured call per event producing summary + action items + priority (1–5) + sentiment, with defensive coercion and an eval harness so prompt/model changes don't silently regress.
3. **Different domain** — primary source is **GitHub notifications** (dev-native, clean PAT auth). Gmail / Slack / Linear are stubbed with implementation plans in their module docstrings.
4. **Local-first** — **DuckDB** destination and **Ollama** for inference. Runs fully offline; no cloud, no per-row cost.

## Table of contents

- [Architecture](#architecture)
- [Quickstart](#quickstart)
- [Docker](#docker)
- [CLI](#cli)
- [Layout](#layout)
- [What's stubbed / deferred](#whats-stubbed--deferred)
- [Why these choices](#why-these-choices)
- [Contributing](#contributing)
- [License](#license)

## Architecture

```
                 ┌────────────────────┐
 GitHub API ───▶ │ dlt source         │ ──▶ DuckDB (schema: github)
                 └────────────────────┘
                           │
                           ▼
                 ┌────────────────────┐
                 │ Prefect flow       │
                 │  ingest_and_enrich │
                 └────────────────────┘
                           │
                           ▼
                 ┌────────────────────┐
 Ollama (local) ◀│ AI enrichment      │ ──▶ DuckDB (schema: enriched)
                 └────────────────────┘
                           │
                           ▼
                     enriched.digest  (view joining raw + enriched)
                           │
                           ▼
                   `streamdigest digest`
```

## Quickstart

```bash
# 1. Python env
cd ~/streamdigest
python3.11 -m venv .venv && source .venv/bin/activate
pip install -e ".[dev]"

# 2. Local LLM
brew install ollama            # or see https://ollama.com
ollama serve &                 # starts the daemon
ollama pull llama3.2:3b        # ~2 GB, fits on a laptop

# 3. Secrets
cp .env.example .env
# edit .env and set GITHUB_TOKEN (classic PAT with `notifications` + `repo`,
# or fine-grained with Notifications: read)

# 4. Sanity check
streamdigest doctor

# 5. Run the pipeline
streamdigest init
streamdigest run
streamdigest digest
```

## Docker

One-command stack (app + Ollama sidecar):

```bash
cp .env.example .env             # set GITHUB_TOKEN
docker compose up -d             # starts Ollama, pulls the model, builds app
docker compose run --rm app streamdigest run
docker compose run --rm app streamdigest digest
```

The Ollama model and DuckDB database both live in named volumes
(`ollama-models`, `streamdigest-data`), so they survive `compose down`.

## CLI

| command | purpose |
|---|---|
| `streamdigest doctor` | verify env, Ollama, and DuckDB are reachable |
| `streamdigest init` | create DuckDB enriched schema (idempotent) |
| `streamdigest run` | ingest from GitHub + enrich (runs the Prefect flow) |
| `streamdigest digest` | print top-priority enriched notifications |
| `streamdigest evals` | run the eval harness against the local model |

## Layout

```
streamdigest/
├── sources/        # github (primary); gmail/slack/linear are stubs
├── ai/             # ollama client, prompts, enrichment + coercion
├── storage/        # DuckDB schema + upsert + digest view
├── flows/          # Prefect flow
├── evals/          # fixtures + scoring
└── cli.py
```

## What's stubbed / deferred

See [TODO.md](./TODO.md). Short version: OpenTelemetry, CI, integration
tests, RAG over prior events, and the three alt sources.

## Why these choices

| Choice | Reason |
|---|---|
| **Prefect over Kestra** | Python-native (no YAML round-trip); flows and tasks are just decorated functions — testable and `python -m`-runnable. |
| **DuckDB over BigQuery** | Runs on a laptop, zero setup, same SQL surface. For the scale of one human's notifications, a cloud warehouse is overkill. |
| **Ollama over OpenAI** | No API keys, no per-row cost, fully local. `llama3.2:3b` is good enough for triage summaries and a 1–5 priority score. |
| **Single-call enrichment** | Kestra demo made 2 calls (summary + sentiment). One JSON-mode call is cheaper and lets the model reason jointly — a "review requested" on a PR with failing CI should be higher priority than either signal alone. |
| **Eval harness from day one** | Local models drift across versions. Fixtures + a pass/fail matrix make model swaps safe. |

## Contributing

Dev setup, coding standards, and PR checklist are in [CONTRIBUTING.md](./CONTRIBUTING.md).
By participating, you agree to abide by the [Code of Conduct](./CODE_OF_CONDUCT.md).
Security issues: see [SECURITY.md](./SECURITY.md) — **do not** open a public issue.

## License

[MIT](./LICENSE) © streamdigest contributors
