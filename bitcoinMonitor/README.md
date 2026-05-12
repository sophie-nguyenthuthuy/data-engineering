# bitcoinMonitor

Near real-time ETL that polls Bitcoin (and selected crypto) spot prices from the CoinGecko public API, lands them in SQLite, and serves a live dashboard via FastAPI.

## Architecture

```
CoinGecko API ──poll 15s──▶ ingest.py ──▶ SQLite (prices.db) ──▶ FastAPI /prices ──▶ dashboard (Chart.js)
```

- **Extract** — `src/ingest.py` polls CoinGecko `/simple/price` on a configurable interval
- **Transform** — light: normalise timestamps, dedupe by (asset, ts), upsert
- **Load** — SQLite (single-file, zero-config). Swap to Postgres/TimescaleDB by editing `db.py`
- **Serve** — FastAPI exposes `/prices` (JSON) and `/` (static dashboard)

## Quick start

```bash
pip install -r requirements.txt
# Terminal 1 — ingester
python -m src.ingest
# Terminal 2 — API + dashboard
uvicorn src.api:app --reload
# Open http://localhost:8000
```

Or with Docker:

```bash
docker compose up --build
```

## Config

Env vars (see `.env.example`):

| Var | Default | Notes |
|---|---|---|
| `ASSETS` | `bitcoin,ethereum,solana` | CoinGecko IDs, comma-separated |
| `VS_CURRENCY` | `usd` | quote currency |
| `POLL_SECONDS` | `15` | ingest interval |
| `DB_PATH` | `data/prices.db` | SQLite file |

## Schema

```sql
CREATE TABLE prices (
  asset       TEXT NOT NULL,
  ts          INTEGER NOT NULL,   -- unix epoch seconds
  price       REAL NOT NULL,
  vs_currency TEXT NOT NULL,
  PRIMARY KEY (asset, ts, vs_currency)
);
CREATE INDEX idx_prices_asset_ts ON prices(asset, ts DESC);
```

## Endpoints

- `GET /prices?asset=bitcoin&limit=500` → array of `{ts, price}`
- `GET /assets` → list of tracked assets
- `GET /health` → `{"ok": true, "rows": N}`
- `GET /` → dashboard

## Status

MVP. No auth, single-process, SQLite. Roadmap: TimescaleDB sink, Kafka tee, anomaly alerts.
