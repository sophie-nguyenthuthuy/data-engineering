# dbt Semantic Layer + BI Gateway

A production-grade dbt project that implements a **single source of truth** for revenue, customer, and product analytics — served to multiple BI tools through the [dbt Semantic Layer (MetricFlow)](https://docs.getdbt.com/docs/use-dbt-semantic-layer/dbt-sl).

```
Raw Sources → Staging → Intermediate → Marts → Semantic Models → Metrics
                                                                     ↓
                                            Tableau · Looker · Power BI · Metabase
```

---

## Project structure

```
dbt-semantic-bi-gateway/
├── models/
│   ├── staging/          # Source-aligned views: rename, cast, light cleaning
│   ├── intermediate/     # Ephemeral joins & aggregations (no direct BI access)
│   └── marts/
│       ├── core/         # Dimensions + core facts (dim_*, fct_orders, fct_order_items)
│       ├── marketing/    # LTV, campaign performance
│       └── finance/      # Daily & monthly revenue P&L
├── semantic_models/      # MetricFlow entity/dimension/measure definitions
├── metrics/              # Simple, ratio, derived, and cumulative metrics
├── exposures/            # BI tool & data product lineage
├── macros/               # generate_schema_name, safe_divide, utils
├── tests/generic/        # Custom generic tests
├── analyses/             # Ad-hoc SQL (cohort retention)
├── seeds/                # FX rates lookup table
└── snapshots/            # SCD Type 2 for customers
```

---

## Semantic Layer architecture

### Semantic models

| Model | Grain | Key entities |
|---|---|---|
| `orders` | One row per order | `order`, `customer`, `campaign` |
| `order_items` | One row per line item | `order_item`, `order`, `product`, `customer` |
| `customers` | One row per customer | `customer` |
| `campaign_performance` | One row per campaign | `campaign` |

### Metrics catalogue

| Metric | Type | Label |
|---|---|---|
| `total_net_revenue` | simple | Net Revenue (USD) |
| `total_gross_revenue` | simple | Gross Revenue (USD) |
| `total_orders` | simple | Total Orders |
| `avg_order_value` | ratio | Average Order Value (USD) |
| `gross_margin_rate` | ratio | Gross Margin % |
| `return_rate` | ratio | Return Rate % |
| `cumulative_revenue_ytd` | cumulative | Cumulative Revenue YTD |
| `avg_customer_ltv` | simple | Avg Customer LTV (USD) |
| `customer_churn_rate` | ratio | Customer Churn Rate % |
| `blended_roas` | ratio | Blended ROAS |
| `blended_cac` | ratio | Blended CAC (USD) |
| `product_gross_margin_rate` | ratio | Product Gross Margin % |

> Full catalogue: [`metrics/`](metrics/)

### BI tool exposures

| Tool | Dashboard / Explore | Schema access |
|---|---|---|
| **Tableau** | Revenue & Orders, Product Performance | Semantic Layer (JDBC) |
| **Looker** | Ecommerce Explore, Customer 360 | Semantic Layer + direct mart |
| **Power BI** | Finance Monthly Report | Direct mart (DirectQuery) |
| **Metabase** | Operations Dashboard | Direct mart |
| **Python/Jupyter** | Cohort Retention Notebook | Direct mart |
| **Hightouch** | CRM sync → Salesforce | Reverse-ETL from mart |
| **Braze** | Marketing segmentation | Reverse-ETL from mart |

---

## Quick start

### Prerequisites

- Python ≥ 3.11
- dbt Core ≥ 1.8 with an adapter (`dbt-duckdb` for local dev, `dbt-postgres` / `dbt-snowflake` / `dbt-bigquery` for cloud)

### Local setup (DuckDB)

```bash
# 1. Clone
git clone https://github.com/sophie-nguyenthuthuy/dbt-semantic-bi-gateway.git
cd dbt-semantic-bi-gateway

# 2. Create and activate a virtual environment
python -m venv .venv && source .venv/bin/activate

# 3. Install dbt with DuckDB adapter
pip install dbt-duckdb dbt-metricflow

# 4. Copy and configure profiles
cp profiles.yml ~/.dbt/profiles.yml
# Edit ~/.dbt/profiles.yml — the 'dev' target uses DuckDB, no credentials needed

# 5. Install dbt packages
dbt deps

# 6. Load seed data
dbt seed

# 7. Build everything
dbt build

# 8. Query the semantic layer
mf query --metrics total_net_revenue --group-by metric_time__month
mf query --metrics avg_order_value,return_rate --group-by country_code

# 9. Generate and serve docs
dbt docs generate && dbt docs serve
```

### Cloud warehouse setup

Copy the relevant profile block from `profiles.yml` and set the required environment variables:

```bash
# PostgreSQL
export DBT_HOST=your-host
export DBT_USER=your-user
export DBT_PASSWORD=your-password
export DBT_DATABASE=analytics

# Snowflake
export SNOWFLAKE_ACCOUNT=your-account
export SNOWFLAKE_USER=your-user
export SNOWFLAKE_PASSWORD=your-password

# BigQuery
export GCP_PROJECT=your-project
gcloud auth application-default login
```

---

## CI/CD

The GitHub Actions workflow (`.github/workflows/dbt_ci.yml`) runs:

| Trigger | Job | What it does |
|---|---|---|
| Pull request | `lint` | SQLFluff + `dbt parse` |
| Pull request | `slim-ci` | `dbt build --select state:modified+` (changed models only) |
| Push to `main` | `production` | Full `dbt build` + snapshots |

**Slim CI** uses dbt's `--defer` and `--state` flags to only build changed models, keeping CI times short.

### Required GitHub secrets

| Secret | Description |
|---|---|
| `DBT_PROD_HOST` | Production warehouse host |
| `DBT_PROD_USER` | Production warehouse user |
| `DBT_PROD_PASSWORD` | Production warehouse password |

---

## Data model overview

### Staging layer
Source-aligned 1:1 with raw tables. Responsibilities:
- Rename columns to snake_case
- Cast types (varchar, numeric, timestamp)
- Derive simple flags (`is_cancelled`, `is_returned`)
- Normalise currency to USD

### Intermediate layer (ephemeral)
Never directly queried by BI tools. Responsibilities:
- Join orders → customers → campaign attribution
- Enrich order items with product details and financials
- Pre-aggregate customer order summaries

### Marts
BI-ready tables. Responsibilities:
- Dimensional modelling (dim_*, fct_*)
- Window functions for growth rates, cohorts, RFM
- Denormalised for query performance

### Semantic models + MetricFlow
Define **what** business logic means, not how to join tables. BI tools send natural-language-style queries; MetricFlow generates the SQL. This ensures:
- Consistent metric definitions across Tableau, Looker, Power BI, and Metabase
- Automatic time-series granularity (day → week → month → quarter → year)
- Join correctness enforced by entity relationships

---

## Development conventions

- **Staging** — prefix `stg_`, views only, one file per source table
- **Intermediate** — prefix `int_`, ephemeral, suffixed with transformation type (`_enriched`, `_summary`)
- **Dimensions** — prefix `dim_`, tables
- **Facts** — prefix `fct_`, tables
- **Tests** — every primary key gets `unique` + `not_null`; foreign keys get `relationships`
- **Descriptions** — every model and every column in the marts layer must have a description

---

## Key packages

| Package | Purpose |
|---|---|
| `dbt-labs/dbt_utils` | `date_spine`, `expression_is_true`, surrogate keys |
| `dbt-labs/metrics` | MetricFlow metric validation helpers |
| `calogica/dbt_date` | Extended date/calendar utilities |
| `dbt-labs/audit_helper` | Compare model outputs between environments |

---

## Extending the project

### Add a new metric
1. Add a measure to the relevant semantic model in `semantic_models/`
2. Define the metric in `metrics/`
3. All connected BI tools can query it immediately — no dashboard changes needed

### Add a new BI tool
1. Install the tool's dbt Semantic Layer connector
2. Add an exposure to `exposures/bi_tools.yml`
3. Point the tool at the Semantic Layer endpoint (configured in dbt Cloud or self-hosted)

### Add a new data source
1. Add the source to `models/staging/_sources.yml`
2. Create a `stg_<source>.sql` model
3. Join into intermediate/mart models as needed
