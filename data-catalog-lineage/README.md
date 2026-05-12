# Data Catalog with Lineage Tracking

A self-hosted data catalog that auto-discovers database assets, automatically tags PII columns, and visualizes end-to-end **column-level lineage** through an interactive graph UI.

![Python](https://img.shields.io/badge/python-3.11%2B-blue)
![FastAPI](https://img.shields.io/badge/FastAPI-0.115-green)
![SQLGlot](https://img.shields.io/badge/sqlglot-30%2B-orange)
![Tests](https://img.shields.io/badge/tests-39%20passing-brightgreen)

---

## Features

| Feature | Detail |
|---|---|
| **Auto-discovery** | Connect SQLite, PostgreSQL, or MySQL — scan schemas, tables, columns & row counts automatically |
| **PII Detection** | 18 PII categories detected from column names *and* sampled values (EMAIL, SSN, PHONE, CREDIT_CARD, …) |
| **Column-level Lineage** | Register SQL jobs → sqlglot parses them → column-to-column lineage edges stored and visualized |
| **Interactive Lineage Graph** | Cytoscape.js + dagre layout — DAG view with upstream/downstream traversal, PII highlights, edge tooltips |
| **Asset Browser** | Tree view by source → schema → table; column details with data types, sample values, PII tags |
| **PII Report** | Full catalog of PII columns with category breakdown, filtering, and CSV export |
| **REST API** | FastAPI with auto-generated Swagger docs at `/docs` |
| **Demo Seeder** | One-command seed with a 3-layer data warehouse (raw → staging → reporting) |

---

## Architecture

```
data-catalog-lineage/
├── main.py                 # FastAPI app entry point
├── catalog/
│   ├── api.py              # REST routes (/api/*)
│   ├── models.py           # SQLAlchemy ORM (DataSource, Table, Column, LineageJob, ColumnLineage)
│   ├── schemas.py          # Pydantic request/response models
│   ├── database.py         # SQLite catalog store setup
│   ├── discovery.py        # Auto-discover DB assets via SQLAlchemy inspection
│   ├── pii_detector.py     # Pattern-based PII tagging (name + value sampling)
│   └── lineage.py          # SQL → column-level lineage via sqlglot AST parsing
├── static/
│   ├── index.html          # Dashboard
│   ├── assets.html         # Asset browser (tree + column panel)
│   ├── lineage.html        # Lineage explorer (Cytoscape graph)
│   ├── pii.html            # PII report with CSV export
│   └── app.js / style.css  # Shared JS helpers & dark-theme CSS
├── demo/
│   └── seed.py             # Seeds 3 SQLite DBs + registers lineage jobs
└── tests/
    ├── test_discovery.py   # 8 tests: table/column discovery, PII, row counts
    ├── test_lineage.py     # 10 tests: SQL parsing, column ref resolution
    └── test_pii_detector.py # 21 tests: name patterns, value patterns, combined
```

---

## Quick Start

### 1. Install dependencies

```bash
python -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt
```

### 2. Start the server

```bash
python main.py
# or: uvicorn main:app --reload
```

Open **http://localhost:8000**

### 3. Load the demo (optional but recommended)

In a second terminal:

```bash
python demo/seed.py
```

This creates three SQLite databases (`demo_raw.db`, `demo_staging.db`, `demo_reporting.db`) with realistic data, registers them as sources, scans them, then registers four lineage jobs so you immediately see a full raw → staging → reporting lineage graph.

### 4. Run tests

```bash
pytest tests/ -v
```

---

## PII Categories Detected

Detection runs on **column names** (regex) and **sampled values** (pattern matching):

`NAME` · `EMAIL` · `PHONE` · `SSN` · `CREDIT_CARD` · `ADDRESS` · `ZIP_CODE` · `DATE_OF_BIRTH` · `PASSPORT` · `DRIVERS_LICENSE` · `IP_ADDRESS` · `GENDER` · `SENSITIVE_DEMOGRAPHIC` · `FINANCIAL` · `BANK_ACCOUNT` · `CREDENTIAL` · `HEALTH` · `GEO_LOCATION` · `USER_ID`

---

## Registering a Lineage Job

Any SQL `INSERT INTO … SELECT` or `CREATE TABLE … AS SELECT` is supported:

```bash
curl -X POST http://localhost:8000/api/jobs \
  -H "Content-Type: application/json" \
  -d '{
    "name": "enrich_customers",
    "job_type": "sql",
    "dialect": "postgres",
    "sql_query": "INSERT INTO rpt.customers (id, full_name, email)\n SELECT c.id, c.first_name || \" \" || c.last_name, c.email FROM stg.customers c"
  }'
```

sqlglot parses the SQL and stores column-to-column edges automatically. Supported dialects: `sqlite`, `mysql`, `postgres`, `bigquery`, `snowflake`, `spark`, `duckdb`.

---

## Supported Data Sources

| Engine | Connection String Example |
|---|---|
| SQLite | `sqlite:///path/to/file.db` |
| PostgreSQL | `postgresql://user:pass@host:5432/dbname` |
| MySQL | `mysql+pymysql://user:pass@host:3306/dbname` |

---

## API Reference

Full interactive docs at **http://localhost:8000/docs**

| Method | Endpoint | Description |
|---|---|---|
| GET | `/api/sources` | List all data sources |
| POST | `/api/sources` | Register a new source |
| POST | `/api/sources/{id}/scan` | Auto-discover & scan assets |
| GET | `/api/sources/{id}/schemas` | List schemas with tables/columns |
| GET | `/api/tables/{id}` | Get table detail with columns |
| PATCH | `/api/columns/{id}` | Update column description / PII tags |
| GET | `/api/jobs` | List lineage jobs |
| POST | `/api/jobs` | Register a lineage job (auto-parses SQL) |
| GET | `/api/lineage/column/{id}` | Column lineage graph (upstream + downstream) |
| GET | `/api/lineage/table/{id}` | Full table lineage graph |
| GET | `/api/search?q=` | Search tables and columns |
| GET | `/api/pii-report` | All PII-tagged columns |
| GET | `/api/stats` | Catalog summary stats |

---

## Tech Stack

- **Backend**: Python 3.11+, FastAPI, SQLAlchemy, SQLite (catalog store)
- **SQL Parsing**: [sqlglot](https://github.com/tobymao/sqlglot) — multi-dialect AST-based lineage extraction
- **Frontend**: Vanilla JS, [Cytoscape.js](https://cytoscape.org/) + dagre layout for the lineage graph
- **Testing**: pytest (39 tests)
