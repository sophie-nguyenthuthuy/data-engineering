# Query Federation Engine

A lightweight, domain-specific query federation engine that lets analysts write **a single SQL query spanning Postgres, MongoDB, S3 Parquet files, and REST APIs simultaneously** — without moving data or maintaining ETL pipelines.

Think of it as a miniaturised Trino/Presto you can embed directly in a Python application.

```sql
SELECT o.id, u.name, e.event_type, p.category
FROM   postgres.orders    o
JOIN   mongodb.users      u  ON o.user_id  = u.id
JOIN   s3_parquet.events  e  ON e.order_id = o.id
JOIN   rest_api.products  p  ON p.id       = o.id
WHERE  u.country = 'US'
  AND  e.event_type = 'purchase'
  AND  o.total > 100
LIMIT  500
```

The engine **decomposes** that query, **pushes predicates** to each source in its native language, fetches results **in parallel**, and **hash-joins** everything in-process.

---

## Architecture

```
┌─────────────────────────────────────────────────────────┐
│                    FederationEngine                      │
│                                                          │
│  SQL ──► QueryPlanner ──► CostBasedOptimizer ──► Executor│
│                                   │                      │
│            ┌──────────────────────┼──────────────────────┤
│            ▼          ▼           ▼          ▼           │
│       Postgres    MongoDB    S3 Parquet   REST API        │
│      Connector   Connector   Connector   Connector        │
└─────────────────────────────────────────────────────────┘
```

| Layer | Responsibility |
|---|---|
| **SQL Parser** (`sqlglot`) | Parses ANSI SQL into an AST |
| **QueryPlanner** | Builds a logical plan; extracts per-source pushed predicates |
| **CostBasedOptimizer** | Reorders joins by estimated cardinality; annotates cost |
| **Executor** | Runs source scans in parallel threads; hash-joins results |
| **Connectors** | Translate pushed predicates into native query languages |
| **SchemaCatalog** | YAML-driven registry of sources, tables, and column types |

### Predicate pushdown by source

| Source | Pushed as |
|---|---|
| Postgres | Parameterised SQL `WHERE` clause |
| MongoDB | MQL filter document (`$eq`, `$gt`, `$in`, `$regex`, …) |
| S3 Parquet | PyArrow `compute` expression (row-group pruning) |
| REST API | Query-string parameters via configurable `param_map` |

### Cost model

The optimizer estimates rows after filtering using per-predicate selectivity heuristics (`=` → 10 %, range → 30 %) and per-source I/O cost weights:

| Source | Relative cost/row |
|---|---|
| S3 Parquet | 0.8 (columnar, cheap) |
| Postgres | 1.0 |
| MongoDB | 1.5 |
| REST API | 10.0 (network round-trips) |

Joins are reordered greedy-smallest-first so the smallest estimated result set is always the hash-build side.

---

## Quick start

### Install

```bash
pip install -e ".[dev]"
```

### Python API

```python
from federation import FederationEngine, SchemaCatalog, SourceType

# Load from a YAML catalog (see config/catalog.example.yaml)
engine = FederationEngine.from_yaml("config/catalog.yaml")

df, stats = engine.query("""
    SELECT o.id, u.name, o.total
    FROM   postgres.orders o
    JOIN   mongodb.users   u ON o.user_id = u.id
    WHERE  u.country = 'US'
    AND    o.total   > 100
""")

print(df)
print(stats.summary())
```

#### In-memory mock (no real connections needed)

```python
import pandas as pd
from federation import FederationEngine
from federation.catalog import SourceType

engine = FederationEngine.__new__(FederationEngine)
# ... (see examples/demo.py for full bootstrap)

engine.register_mock_table("postgres", "orders", SourceType.POSTGRES, orders_df)
engine.register_mock_table("mongodb",  "users",  SourceType.MONGODB,  users_df)

df, stats = engine.query("SELECT o.id, u.name FROM postgres.orders o JOIN mongodb.users u ON o.user_id = u.id")
```

### CLI

```bash
# Run a query
qfe query --config config/catalog.yaml \
    "SELECT id, total FROM postgres.orders WHERE status = 'shipped' LIMIT 10"

# CSV / JSON output
qfe query --config config/catalog.yaml --format csv \
    "SELECT * FROM mongodb.users WHERE country = 'US'"

# Show the query plan without executing
qfe explain --config config/catalog.yaml \
    "SELECT o.id, u.name FROM postgres.orders o JOIN mongodb.users u ON o.user_id = u.id"

# List all registered tables
qfe tables --config config/catalog.yaml

# Print execution stats
qfe query --config config/catalog.yaml --stats \
    "SELECT * FROM s3_parquet.events WHERE event_type = 'purchase'"
```

### Run the demo

```bash
python examples/demo.py
```

---

## Catalog configuration

Copy `config/catalog.example.yaml` → `config/catalog.yaml` and fill in your connection details.

```yaml
sources:
  - name: postgres
    type: postgres
    connection:
      host: localhost
      port: 5432
      dbname: analytics
      user: analyst
      password: secret

  - name: mongodb
    type: mongodb
    connection:
      uri: mongodb://localhost:27017
      database: app_db

  - name: s3_parquet
    type: s3_parquet
    connection:
      path: s3://my-data-lake/warehouse/
      region: us-east-1

  - name: rest_api
    type: rest_api
    connection:
      base_url: https://api.example.com/v1
      headers:
        Authorization: "Bearer TOKEN"

tables:
  - source: postgres
    table: orders
    estimated_rows: 2000000
    columns:
      - { name: id,      type: int   }
      - { name: user_id, type: int   }
      - { name: total,   type: float }
      - { name: status,  type: string }
```

Tables are referenced in SQL as `<source_name>.<table_name>`.

---

## Project layout

```
query-federation-engine/
├── src/federation/
│   ├── engine.py            # FederationEngine — public entry point
│   ├── catalog.py           # SchemaCatalog, TableSchema, ColumnDef
│   ├── executor.py          # Parallel execution engine + stats
│   ├── cli.py               # qfe command-line tool
│   ├── planner/
│   │   ├── nodes.py         # PlanNode hierarchy
│   │   ├── builder.py       # QueryPlanner (AST → plan tree)
│   │   └── optimizer.py     # CostBasedOptimizer
│   └── connectors/
│       ├── base.py          # BaseConnector interface
│       ├── postgres.py      # PostgreSQL connector
│       ├── mongodb.py       # MongoDB connector
│       ├── s3_parquet.py    # S3 / local Parquet connector
│       └── rest_api.py      # Paginated REST API connector
├── tests/
│   ├── conftest.py          # Shared fixtures & mock DataFrames
│   ├── test_catalog.py
│   ├── test_planner.py
│   ├── test_connectors.py
│   └── test_integration.py
├── examples/
│   └── demo.py              # Runnable demo across all four sources
└── config/
    └── catalog.example.yaml
```

---

## Running tests

```bash
pytest -v
```

To see coverage:

```bash
pytest --cov=federation --cov-report=term-missing
```

---

## Extending with a new source

1. Add a new `SourceType` variant in `catalog.py`
2. Implement `BaseConnector` in `connectors/your_source.py` — translate `list[exp.Expression]` predicates into the native query format
3. Register it in `engine.py`'s `_SOURCE_CONNECTOR_MAP`
4. Add source cost weight in `optimizer.py`'s `SOURCE_SCAN_COST`

---

## Limitations & future work

- **Aggregates across sources** — `GROUP BY` is currently evaluated in-process after the join
- **Subqueries / CTEs** — not yet supported by the planner
- **Statistics collection** — real cardinality estimates require ANALYZE-style sampling from each source
- **Streaming / chunked joins** — large result sets are fully materialised in RAM; spill-to-disk not implemented
- **Write operations** — INSERT / UPDATE / DELETE are not supported; read-only by design

---

## License

MIT
