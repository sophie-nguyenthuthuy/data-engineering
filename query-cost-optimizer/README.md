# ⚡ Query Cost & Performance Optimization Engine

A CLI tool that connects to **BigQuery** or **Snowflake**, analyses your query history, and surfaces actionable recommendations with estimated monthly dollar savings.

| What it finds | What it recommends |
|---|---|
| Unpartitioned tables with date filters | Partition column + DDL |
| Tables missing clustering keys | Cluster keys + ALTER TABLE |
| SELECT * on wide tables | Explicit column lists |
| Non-sargable WHERE clauses | Rewrite patterns |
| Correlated scalar subqueries | JOIN / window rewrites |
| CROSS JOINs | Fix with explicit conditions |
| ORDER BY without LIMIT | Add LIMIT or remove ORDER BY |

---

## Installation

```bash
# Core (no warehouse connector)
pip install query-cost-optimizer

# BigQuery
pip install "query-cost-optimizer[bigquery]"

# Snowflake
pip install "query-cost-optimizer[snowflake]"

# Both
pip install "query-cost-optimizer[all]"
```

Or clone and install in editable mode:

```bash
git clone https://github.com/sophie-nguyenthuthuy/query-cost-optimizer.git
cd query-cost-optimizer
pip install -e ".[all]"
```

---

## Quick start — Demo (no credentials needed)

```bash
# Console output
qco demo

# HTML report
qco demo --output html

# BigQuery-style demo as JSON
qco demo --platform bigquery --output json --out-dir ./reports
```

---

## BigQuery

### Prerequisites

Ensure you are authenticated:

```bash
gcloud auth application-default login
```

Or set `GOOGLE_APPLICATION_CREDENTIALS=/path/to/service_account.json`.

### Run

```bash
qco bigquery --project my-gcp-project

# Last 14 days, surface recommendations worth > $25/month
qco bigquery --project my-gcp-project --days 14 --min-savings 25

# All output formats
qco bigquery --project my-gcp-project --output all --out-dir ./reports
```

---

## Snowflake

### Prerequisites

Your Snowflake user needs `IMPORTED PRIVILEGES` on the `SNOWFLAKE` database
(for `ACCOUNT_USAGE.QUERY_HISTORY`).

```sql
GRANT IMPORTED PRIVILEGES ON DATABASE SNOWFLAKE TO ROLE my_role;
```

### Run

```bash
qco snowflake \
  --account myaccount.us-east-1 \
  --user myuser \
  --password "$SNOWFLAKE_PASSWORD" \
  --warehouse COMPUTE_WH

# Or use environment variables (recommended)
export SNOWFLAKE_ACCOUNT=myaccount.us-east-1
export SNOWFLAKE_USER=myuser
export SNOWFLAKE_PASSWORD=...
export SNOWFLAKE_WAREHOUSE=COMPUTE_WH

qco snowflake --output html --out-dir ./reports
```

---

## Environment variables

Copy `.env.example` to `.env` and fill in your values:

```bash
cp .env.example .env
```

| Variable | Description |
|---|---|
| `BQ_PROJECT_ID` | GCP project ID |
| `BQ_HISTORY_DAYS` | Days of history (default 30) |
| `SNOWFLAKE_ACCOUNT` | Snowflake account identifier |
| `SNOWFLAKE_USER` | Snowflake username |
| `SNOWFLAKE_PASSWORD` | Snowflake password |
| `SNOWFLAKE_WAREHOUSE` | Warehouse to run queries |
| `MIN_SAVINGS_USD` | Min monthly savings to surface a recommendation |
| `MIN_QUERY_COUNT` | Min times a pattern must appear |

---

## Output formats

| Flag | Description |
|---|---|
| `--output console` | Rich terminal table (default) |
| `--output json` | Machine-readable JSON in `--out-dir` |
| `--output html` | Self-contained HTML report in `--out-dir` |
| `--output all` | All three simultaneously |

---

## How it works

```
Query History
  (INFORMATION_SCHEMA / ACCOUNT_USAGE)
          │
          ▼
    SQL Parser (sqlglot)
    ┌──────────────────────┐
    │ • filter columns     │
    │ • join columns       │
    │ • group-by columns   │
    │ • anti-patterns      │
    └──────────────────────┘
          │
          ▼
    Recommenders
    ┌──────────────────────────────────────────┐
    │ ClusteringRecommender                    │
    │   → score columns by filter/join freq    │
    │   → emit ALTER TABLE … CLUSTER BY        │
    │                                          │
    │ PartitioningRecommender                  │
    │   → detect date-like filter columns      │
    │   → emit CREATE TABLE … PARTITION BY     │
    │                                          │
    │ PatternDetector                          │
    │   → flag SELECT *, CROSS JOIN, etc.      │
    │   → estimate % savings per pattern       │
    └──────────────────────────────────────────┘
          │
          ▼
    Reporters (console / JSON / HTML)
```

### Cost estimation

| Platform | Model |
|---|---|
| BigQuery | `$6.25 / TiB` billed (on-demand pricing) |
| Snowflake | `$3.00 / credit` × `credits_used_cloud_services` |

Savings percentages are conservative industry benchmarks:

| Strategy | Estimated saving |
|---|---|
| Partitioning | 25–30 % of scan cost |
| Clustering | 18–20 % of scan cost |
| Remove SELECT * | 25 % |
| Fix non-sargable filter | 30 % |
| Remove correlated subquery | 40 % |
| Fix CROSS JOIN | 60 % |

---

## Development

```bash
pip install -e ".[dev]"
pytest tests/ -v --tb=short
```

---

## License

MIT
