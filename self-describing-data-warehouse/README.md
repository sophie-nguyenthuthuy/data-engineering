# Self-Describing Data Warehouse

A warehouse where every table is queryable not just for its data, but for its own metadata — lineage, quality scores, freshness, who uses it, and what broke it last.

Analysts can ask **"which table should I use for monthly revenue?"** and get a ranked answer based on relevance, usage popularity, data freshness, quality history, and reliability.

## Concept

Most warehouses answer: *"what data is in this table?"*

This warehouse also answers:
- **What** is this table? (description, owner, tags, column docs)
- **Where** does the data come from? (lineage — upstream/downstream)
- **How good** is the data? (quality score: null rate, duplicates, violations)
- **How current** is it? (freshness score vs expected refresh interval)
- **Who uses it?** (query count, unique users, last accessed)
- **What broke it?** (incident history with root cause)
- **Which table should I use?** (ranked recommendations from a natural-language query)

## Architecture

```
self-describing-data-warehouse/
├── warehouse/
│   ├── core/
│   │   ├── warehouse.py      # Top-level facade — the main API
│   │   ├── registry.py       # Table & column metadata registration
│   │   ├── lineage.py        # Directed graph of table dependencies
│   │   ├── quality.py        # Quality scoring (completeness, uniqueness, validity)
│   │   ├── freshness.py      # Freshness monitoring with exponential decay scoring
│   │   ├── usage.py          # Query tracking and popularity scoring
│   │   ├── incidents.py      # Incident tracking (what broke, when, root cause)
│   │   └── recommender.py    # Weighted multi-signal table recommender
│   ├── schema/
│   │   └── metadata_schema.py  # SQL DDL for all metadata tables
│   └── cli/
│       └── main.py           # CLI (sdw command)
├── demo/
│   ├── seed.py               # Seeds a realistic demo warehouse
│   └── demo.py               # Guided tour of all features
└── tests/
    └── test_warehouse.py     # 25 tests covering all subsystems
```

The warehouse is backed by **SQLite** — zero external dependencies, portable, and inspectable with any SQL tool. Swap in Postgres/DuckDB/BigQuery by replacing the connection layer.

## Quick start

```bash
git clone https://github.com/YOUR_USERNAME/self-describing-data-warehouse
cd self-describing-data-warehouse
pip install -e ".[dev]"

# Run the guided demo (uses in-memory DB)
python -m demo.demo

# Seed a persistent demo database
python -m demo.seed --db warehouse.db

# Use the CLI
sdw --db warehouse.db catalog
sdw --db warehouse.db recommend "monthly revenue by region"
sdw --db warehouse.db describe monthly_revenue_summary
sdw --db warehouse.db health
```

## CLI reference

| Command | Description |
|---|---|
| `sdw catalog [--domain DOMAIN]` | List all registered tables |
| `sdw describe TABLE` | Full metadata profile for a table |
| `sdw recommend "QUERY" [--domain D] [--top N]` | Find the best table for a natural-language query |
| `sdw health` | Quality/freshness dashboard across all tables |
| `sdw lineage TABLE [--direction up\|down\|impact]` | Show lineage or run impact analysis |
| `sdw quality TABLE [--run]` | Quality history (optionally trigger a fresh run) |
| `sdw incidents list [--table TABLE]` | Open incidents |
| `sdw incidents history --table TABLE` | Full incident log for a table |

## Python API

```python
from warehouse.core.warehouse import SelfDescribingWarehouse
from warehouse.core.registry import TableMeta, ColumnMeta

wh = SelfDescribingWarehouse(db_path="my_warehouse.db")

# Create and register a table
wh.create_table("CREATE TABLE orders (id TEXT, revenue REAL, region TEXT)")
wh.registry.register_table(TableMeta(
    table_name="orders",
    description="All customer orders — primary revenue fact table",
    owner="data-eng@acme.com",
    domain="finance",
    source_system="Stripe",
    update_frequency="hourly",
    tags=["revenue", "orders", "fact-table"],
    columns=[
        ColumnMeta("id",      "TEXT", "Order ID",      is_nullable=False),
        ColumnMeta("revenue", "REAL", "Gross USD",     is_nullable=False),
        ColumnMeta("region",  "TEXT", "Sales region"),
    ],
))

# Record quality, freshness, lineage
wh.quality.run("orders")
wh.freshness.record("orders", last_updated_at="2025-05-09T06:00:00Z", expected_interval_hours=1)
wh.lineage.add_edge("orders", "monthly_summary", transformation="dbt aggregate model")

# Ask the warehouse about itself
profile = wh.describe("orders")
# → metadata + quality + freshness + usage + lineage + incidents

# Find the best table for a task
results = wh.recommend("monthly revenue by region", top_k=3)
for r in results:
    print(f"{r.table_name}: {r.composite_score}/100")

# Query with automatic usage tracking
rows = wh.execute("SELECT region, SUM(revenue) FROM orders GROUP BY region", user="alice")

# Health dashboard
dashboard = wh.health_dashboard()
```

## Recommender scoring

The `recommend()` method scores every registered table on five signals:

| Signal | Weight | How it's computed |
|---|---|---|
| Relevance | 35% | Keyword overlap between query and table name/description/tags/columns |
| Quality | 25% | Latest quality score (completeness × 40% + uniqueness × 40% + validity × 20%) |
| Freshness | 20% | Exponential decay from expected refresh interval |
| Usage | 10% | Query frequency + recency |
| Reliability | 10% | Penalised by open incidents and recent outages |

Deprecated tables are excluded by default.

## Quality scoring

Every `quality.run(table)` inspects the live table and records:

- **Null rate** — average % of nulls across all columns
- **Duplicate rate** — % of exact-duplicate rows
- **Constraint violations** — rows that break NOT NULL constraints
- **Quality score** — composite 0-100

Trend is computed from the last 5 runs: `improving` / `stable` / `degrading`.

## Freshness scoring

Freshness decays exponentially once the expected refresh interval passes:

```
score = 100              if hours_since ≤ expected_interval
score = 100 × e^(-0.7×(ratio-1))  otherwise
```

A daily table that hasn't updated in 3 days scores ~14/100.

## Demo tables

The seed script creates five realistic tables:

| Table | Domain | Description |
|---|---|---|
| `orders` | finance | 2,000 Stripe orders — primary revenue fact table |
| `monthly_revenue_summary` | finance | Pre-aggregated monthly revenue by region (dbt model) |
| `customers` | product | 300 CRM customers with PII columns flagged |
| `events` | product | 10,000 raw clickstream events from Segment |
| `products` | product | Product catalogue dimension |
| `revenue_by_product_old` | finance | Deprecated — replaced by monthly_revenue_summary |

With lineage edges connecting them, quality runs, freshness scores, usage history, and seeded incidents.

## Running tests

```bash
pytest                    # all 25 tests
pytest -v --tb=short      # verbose
pytest tests/ -k quality  # filter by name
```

## Extending

- **Vector search**: swap `recommender._relevance()` for an embedding-based similarity search against a vector store (e.g. pgvector, Chroma) to handle semantic queries like "churn risk" finding `customer_health_scores`.
- **Real databases**: replace `sqlite3` with `psycopg2` / DuckDB / BigQuery connector — the metadata layer is ordinary SQL.
- **Automated quality runs**: schedule `wh.quality.run(table)` on a cron after each pipeline load.
- **dbt integration**: parse `manifest.json` to auto-populate lineage and column descriptions.
- **Slack/PagerDuty alerts**: hook `incidents.open()` to your alerting system.

## License

MIT
