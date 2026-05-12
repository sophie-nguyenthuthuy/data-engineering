# Automated Data Contract Enforcement Platform

A lightweight, Git-native platform for publishing, versioning, and enforcing data contracts across your pipeline ecosystem.

**Producers** publish schema + SLA + semantic rules as YAML. The platform validates every pipeline output, scores producer reliability, detects breaking changes in CI, and notifies downstream consumers with structured reports.

---

## Features

| Capability | Detail |
|---|---|
| **Contract authoring** | YAML schema with field types, nullability, constraints |
| **SLA rules** | `row_count`, `completeness`, `freshness`, `latency` |
| **Semantic rules** | Custom pandas expressions evaluated at validation time |
| **Breaking-change detection** | Automatic diff between contract versions; fails CI on major breaks |
| **Reliability scoring** | Rolling pass-rate per producer stored in SQLite |
| **Consumer notifications** | Structured JSON payload via webhook or stdout |
| **CI enforcement** | GitHub Actions workflows included |
| **CLI** | `dce validate`, `dce score`, `dce diff`, `dce check-all`, `dce list` |

---

## Quick start

```bash
git clone https://github.com/sophie-nguyenthuthuy/data-contract-platform.git
cd data-contract-platform
pip install -e .

# Run the quickstart demo
python examples/quickstart.py

# See breaking-change detection in action
python examples/breaking_change_demo.py
```

---

## CLI reference

### `dce validate` — Validate a data file against a contract

```bash
dce validate contracts/examples/orders/v1.0.0.yaml data/orders.csv \
  --output reports/orders_run.json \
  --freshness 3600 \
  --notify https://hooks.example.com/consumer-alerts
```

| Flag | Description |
|------|-------------|
| `--output` | Write JSON validation report to file |
| `--freshness` | Data age in seconds (for freshness SLA checks) |
| `--latency` | Pipeline processing time in seconds |
| `--notify` | One or more webhook URLs to POST failure payloads |
| `--db` | SQLite database path (default: `reliability.db`) |

Exit code `0` = passed, `1` = failed.

---

### `dce score` — Show reliability scores

```bash
dce score --window 50
```

```
Producer                       Contract                            Score   Runs  Last
-----------------------------------------------------------------------------------------------
order-service                  orders                             98.0%    100  2026-05-09T06:00
event-ingestion-service        user-events                        91.0%     50  2026-05-09T05:00
```

---

### `dce diff` — Diff two contract versions

```bash
dce diff contracts/ orders 1.1.0 2.0.0
dce diff contracts/ orders 1.1.0 2.0.0 --markdown reports/breaking.md
```

---

### `dce check-all` — CI breaking-change scan

```bash
dce check-all contracts/
```

Scans all consecutive version pairs in the contracts directory and exits `1` if any breaking changes are found.

---

### `dce list` — List all contracts

```bash
dce list contracts/
```

---

## Writing a contract

```yaml
id: my-dataset
version: "1.0.0"
producer: my-service
consumers:
  - analytics-team
  - ml-platform

fields:
  - name: user_id
    type: string
    nullable: false
    constraints:
      unique: true

  - name: amount
    type: number
    nullable: false
    constraints:
      min: 0.0

sla_rules:
  - name: minimum_rows
    rule_type: row_count
    threshold: 1000

  - name: freshness
    rule_type: freshness
    threshold: 86400
    unit: s

semantic_rules:
  - name: positive_amounts
    expression: "(df['amount'] >= 0).all()"
    severity: error
```

See [`docs/contract-schema.md`](docs/contract-schema.md) for the full schema reference.

---

## Project layout

```
data-contract-platform/
├── contracts/              # Git-versioned contract definitions
│   └── examples/
│       ├── orders/         # v1.0.0, v1.1.0, v2.0.0 — illustrates breaking changes
│       └── events/
├── src/dce/                # Core library
│   ├── contract.py         # Contract loading & version diff
│   ├── validator.py        # Schema, SLA, and semantic validation engine
│   ├── scorer.py           # Reliability scoring (SQLite)
│   ├── reporter.py         # Breaking-change & summary reports
│   ├── notifier.py         # Webhook / stdout consumer notifications
│   ├── registry.py         # Contract registry with version resolution
│   └── cli.py              # CLI entry point
├── tests/                  # pytest test suite
├── examples/               # Runnable demos
├── docs/                   # Schema reference
└── .github/workflows/      # CI: test + validate + breaking-change check
```

---

## CI integration

Two GitHub Actions workflows are included:

- **`ci.yml`** — runs on every push/PR: unit tests (Python 3.10–3.12), contract YAML linting, and breaking-change detection. Breaking changes block merges to `main`.
- **`scheduled-validation.yml`** — daily cron job template for validating live pipeline outputs.

---

## Architecture

```
Producer pipeline
       │
       ▼
  dce validate ──► ContractValidator
       │                 │
       │           ┌─────▼──────┐
       │           │ Schema     │  field types, nullability, constraints
       │           │ SLA        │  row_count, completeness, freshness, latency
       │           │ Semantic   │  pandas expressions
       │           └─────┬──────┘
       │                 │
       ▼                 ▼
  ReliabilityStore   ValidationResult
  (SQLite)               │
       │           ┌─────▼──────────────────┐
       │           │  passed?               │
       │           │  yes → record & exit 0 │
       │           │  no  → notify consumers│
       │           │        write report    │
       └──────────►│        exit 1          │
                   └────────────────────────┘
```

---

## Development

```bash
pip install -e ".[dev]"
pytest
pytest --cov=dce --cov-report=term-missing
```

---

## License

MIT
