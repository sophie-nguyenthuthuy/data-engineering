# inflation-crawler

A modern, local-first redesign of [uhussain/WebCrawlerForInflation](https://github.com/uhussain/WebCrawlerForInflation). Measures inflation from online product prices archived in [Common Crawl](https://commoncrawl.org/), compares against the BLS CPI, and renders a dashboard — all with no AWS account, no EMR, and no Spark cluster.

## What changed vs. the original

| Original (2019)                      | This project                                                    |
|--------------------------------------|-----------------------------------------------------------------|
| AWS Athena over CC index             | **DuckDB** httpfs over the same Parquet index, runs locally     |
| EMR + Spark, downloads full WARCs    | **Async httpx** + HTTP Range fetches a single record (~10 KB)   |
| BeautifulSoup regex for prices       | **JSON-LD / microdata / OpenGraph** extractors, heuristic fallback, optional LLM |
| Pandas (single-thread)               | **Polars** (multi-threaded, log-return inflation)               |
| DynamoDB local / S3 Parquet          | **DuckDB** single-file, queryable in place                      |
| Dash + Plotly server                 | **FastAPI + Chart.js** single-page dashboard                    |
| 7 CLI steps across README            | **One CLI (`ic`)** with `ingest / fetch / cpi / analyze / serve` |
| AWS only                             | **Docker compose** or `pip install -e .`                        |

## Pipeline

```
 Common Crawl CC-Index (Parquet on S3)
             │  DuckDB httpfs (anonymous)
             ▼
    ic ingest  ──► data/extracted/index_*.parquet
             │
             ▼
    ic fetch  ──► HTTP Range GET → WARC record → HTML
             │         │
             │         ▼
             │    extract_product()  [JSON-LD ▸ microdata ▸ OG ▸ heuristic]
             ▼
        DuckDB: products table  ◄── BLS CPI (ic cpi)
             │
             ▼
    ic analyze  (Polars log-return aggregation)
             │
             ▼
    ic serve    (FastAPI + Chart.js at :8000)
```

## Quickstart

### Local

```bash
cd ~/inflation-crawler
python3 -m venv .venv && source .venv/bin/activate
pip install -e ".[dev]"

# Run the synthetic-data demo (no network to Common Crawl needed)
bash scripts/run_demo.sh
# → open http://127.0.0.1:8000
```

### Against real Common Crawl

```bash
# 1. Pull ~5k Walmart product pages from the March 2024 crawl.
ic ingest --crawl CC-MAIN-2024-10 --host '%walmart.com' --limit 5000

# 2. Fetch the WARC records and extract products (one DB hit each, range GETs).
ic fetch --index data/extracted/index_CC-MAIN-2024-10.parquet

# 3. Pull official CPI for comparison.
ic cpi --start-year 2023 --end-year 2024

# 4. See the numbers.
ic analyze --category laptops --year 2024

# 5. Browse the dashboard.
ic serve
```

### Docker

```bash
docker compose up --build api
# demo profile seeds data then serves:
docker compose --profile demo up --build
```

## Inflation methodology

For each product we compute monthly median price, then month-over-month **log returns**. Category-level monthly inflation is the unweighted mean of log returns across products present in both months (avoids spurious spikes from items entering/leaving the catalog). The annualized rate is `exp(Σ monthly log returns) − 1`.

Compared to the arithmetic-mean approach in the original, this is substantially less sensitive to compositional changes and one-day promotional prices.

## Project layout

```
src/inflation_crawler/
  ingest.py     DuckDB query against CC-Index Parquet on S3
  fetch.py      Async ranged WARC record fetcher
  extract.py    JSON-LD / microdata / OpenGraph / heuristic price extraction
  store.py      DuckDB schema + upserts
  cpi.py        BLS API client
  analyze.py    Polars aggregation + log-return inflation
  api.py        FastAPI app
  cli.py        `ic` command
  dashboard/    Static HTML + Chart.js
tests/          pytest with offline HTML fixtures
scripts/        run_demo.sh (end-to-end with synthetic data)
```

## Configuration

Everything is overridable via `IC_*` env vars (see `.env.example`). Useful ones:

- `IC_FETCH_CONCURRENCY` — parallel range-GETs (default 16)
- `IC_BLS_API_KEY` — removes the 25 req/day anonymous limit
- `IC_DATA_DIR`, `IC_DB_PATH` — where DuckDB + Parquet files live

## Development

```bash
pytest              # unit tests with offline fixtures
ruff check src      # lint
```

## License

MIT
