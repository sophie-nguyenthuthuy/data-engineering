# 💰 Savings Rate Aggregation Engine

> Scheduled scraping + normalization pipeline for Vietnamese bank savings rates.  
> Extensible backend for **TiếtKiệm+** — with historical tracking and trend analysis.

[![CI](https://github.com/YOUR_USERNAME/savings-rate-engine/actions/workflows/ci.yml/badge.svg)](https://github.com/YOUR_USERNAME/savings-rate-engine/actions/workflows/ci.yml)
[![Python 3.11+](https://img.shields.io/badge/python-3.11%2B-blue)](https://www.python.org/)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](LICENSE)

---

## What it does

| Layer | Description |
|---|---|
| **Scrapers** | Per-bank modules that fetch live HTML/JSON from 7 Vietnamese banks |
| **Normalizer** | Converts raw term labels (`"6 tháng"`, `"6M"`, `"180 ngày"`) → canonical `term_days` |
| **Storage** | SQLAlchemy + SQLite (swappable to Postgres) with full snapshot history |
| **Analyzer** | Trend computation (Δ7d / Δ30d / Δ90d), direction signals, cross-bank ranking |
| **API** | FastAPI REST endpoints — current rates, best rates, trends, comparison tables |
| **Scheduler** | APScheduler-based background job (default: every 6 hours) |
| **CLI** | `sre` command — scrape, serve, compare, trends |

### Banks covered

| Code | Bank |
|---|---|
| VCB | Vietcombank |
| BIDV | BIDV |
| CTG | VietinBank |
| TCB | Techcombank |
| MBB | MB Bank |
| ACB | ACB |
| VPB | VPBank |

Adding a new bank takes ~40 lines — see [Adding a scraper](#adding-a-scraper).

---

## Quick start

```bash
# 1. Clone & install
git clone https://github.com/YOUR_USERNAME/savings-rate-engine.git
cd savings-rate-engine
python -m venv .venv && source .venv/bin/activate
pip install -e ".[dev]"

# 2. Configure
cp .env.example .env
# Edit .env — or leave defaults (SQLite, mock data off)

# 3. Seed DB + run first scrape
sre scrape

# 4. Start the API
sre serve
# → http://localhost:8000/docs
```

### With Docker

```bash
docker compose up
# API  → http://localhost:8000/docs
# Scheduler runs every 6 h automatically
```

---

## CLI reference

```
sre scrape                          # scrape all banks
sre scrape --banks VCB,BIDV,TCB     # subset only
sre rates                           # show latest rates from DB
sre rates --bank VCB --term 180     # filter by bank + term
sre compare --term 180              # rank all banks for 180-day term
sre trends VCB --term 365           # rate history + delta for VCB/12m
sre schedule --interval 6           # start background scheduler (blocks)
sre serve --port 8000               # start FastAPI server
```

---

## API endpoints

```
GET /health                         → service liveness
GET /banks                          → list all tracked banks
GET /banks/{code}                   → single bank info + snapshot count

GET /rates                          → latest rates, all banks
GET /rates?bank_code=VCB            → latest rates, one bank
GET /rates?term_days=180            → filter by term
GET /rates/best?term_days=180       → top-N rates for a term
GET /rates/terms                    → canonical term list in DB
GET /rates/{bank_code}              → all rates for one bank

GET /analysis/trends/{bank_code}?term_days=365   → time series + deltas
GET /analysis/compare?term_days=180              → cross-bank ranking
GET /analysis/best-table?terms=30,90,180,365     → multi-term dashboard
```

Interactive docs: `http://localhost:8000/docs`

---

## Project structure

```
savings-rate-engine/
├── src/savings_engine/
│   ├── scrapers/          # Per-bank scrapers (vcb, bidv, vietinbank, …)
│   │   ├── base.py        # BaseScraper with retry + mock fallback
│   │   └── registry.py    # bank_code → scraper class map
│   ├── models/
│   │   ├── db_models.py   # SQLAlchemy ORM: Bank, RateSnapshot, RateRecord
│   │   └── schemas.py     # Pure dataclasses: RateEntry, NormalizedRate, …
│   ├── storage/
│   │   ├── database.py    # Engine, SessionLocal, init_db, bank seed
│   │   └── repository.py  # All DB queries in one place
│   ├── normalizer.py      # term_label → term_days canonical conversion
│   ├── analyzer/
│   │   ├── trends.py      # TrendSummary, Δ7d/30d/90d, direction signal
│   │   └── comparisons.py # Cross-bank ranking
│   ├── pipeline.py        # Orchestrator: scrape → normalize → persist
│   ├── scheduler.py       # APScheduler blocking scheduler
│   ├── cli.py             # Typer CLI (sre command)
│   └── api/
│       ├── app.py         # FastAPI factory
│       └── routes/        # banks.py, rates.py, analysis.py
├── tests/
│   ├── conftest.py        # In-memory SQLite fixtures
│   ├── test_normalizer.py
│   ├── test_scrapers.py
│   ├── test_repository.py
│   ├── test_analyzer.py
│   └── test_pipeline.py
├── alembic/               # DB migration scripts
├── .github/workflows/     # CI: lint + test + docker build
├── Dockerfile
├── docker-compose.yml
└── pyproject.toml
```

---

## Adding a scraper

1. Create `src/savings_engine/scrapers/mybank.py`:

```python
from .base import BaseScraper, ScraperError
from savings_engine.models.schemas import RateEntry

class MyBankScraper(BaseScraper):
    bank_code = "MYB"
    bank_name = "My Bank"

    def _fetch_rates(self) -> list[RateEntry]:
        resp = self._get("https://mybank.vn/api/rates")
        # parse resp.json() → list of RateEntry
        ...

    def _mock_rates(self) -> list[RateEntry]:
        return [
            RateEntry(bank_code="MYB", term_label="6 tháng", rate_pa=5.5),
            RateEntry(bank_code="MYB", term_label="12 tháng", rate_pa=6.0),
        ]
```

2. Register in `registry.py`:

```python
from .mybank import MyBankScraper
SCRAPER_REGISTRY["MYB"] = MyBankScraper
```

3. Add the bank row to `_seed_banks()` in `storage/database.py`.

That's it — the pipeline, API, and CLI pick it up automatically.

---

## Configuration

All settings come from environment variables (or `.env`):

| Variable | Default | Description |
|---|---|---|
| `DATABASE_URL` | `sqlite:///./data/rates.db` | Any SQLAlchemy URL |
| `USE_MOCK_DATA` | `false` | Skip live HTTP, return mock data |
| `SCRAPE_INTERVAL_HOURS` | `6` | Scheduler cadence |
| `REQUEST_TIMEOUT_SECONDS` | `30` | Per-request timeout |
| `REQUEST_RETRY_ATTEMPTS` | `3` | Tenacity retry count |
| `API_PORT` | `8000` | FastAPI listen port |
| `LOG_LEVEL` | `INFO` | Python logging level |

---

## Development

```bash
# Run tests
pytest -v

# Lint
ruff check src/ tests/

# DB migrations (after model changes)
alembic revision --autogenerate -m "add column X"
alembic upgrade head
```

---

## License

MIT © TiếtKiệm+
