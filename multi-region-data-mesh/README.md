# Multi-Region Active-Active Data Mesh Node

> A production-grade "data product" domain node that replicates across two independent regions with pluggable conflict resolution, local reads, and a live replication-lag dashboard — modelled on real global fintech infrastructure.

---

## Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                    Data Mesh Network                        │
│                                                             │
│   ┌──────────────────┐         ┌──────────────────┐        │
│   │    Region A       │◄──────►│    Region B       │        │
│   │  (localhost:8001) │  async │  (localhost:8002) │        │
│   │                  │  repl  │                  │        │
│   │  ┌────────────┐  │        │  ┌────────────┐  │        │
│   │  │  FastAPI   │  │        │  │  FastAPI   │  │        │
│   │  │  Consumer  │  │        │  │  Consumer  │  │        │
│   │  │    API     │  │        │  │    API     │  │        │
│   │  └─────┬──────┘  │        │  └─────┬──────┘  │        │
│   │        │         │        │        │         │        │
│   │  ┌─────▼──────┐  │        │  ┌─────▼──────┐  │        │
│   │  │ Replication│  │        │  │ Replication│  │        │
│   │  │   Engine   │  │        │  │   Engine   │  │        │
│   │  └─────┬──────┘  │        │  └─────┬──────┘  │        │
│   │        │         │        │        │         │        │
│   │  ┌─────▼──────┐  │        │  ┌─────▼──────┐  │        │
│   │  │  SQLite DB │  │        │  │  SQLite DB │  │        │
│   │  │ (regional) │  │        │  │ (regional) │  │        │
│   │  └────────────┘  │        │  └────────────┘  │        │
│   └──────────────────┘        └──────────────────┘        │
└─────────────────────────────────────────────────────────────┘
```

**Key properties:**
- **Active-active** — both regions accept reads AND writes simultaneously
- **Local reads** — consumers always query their regional node; zero cross-region latency
- **Async replication** — background pull every 2 s + immediate push after each write
- **No shared storage** — each region has a fully independent SQLite database
- **Vector clocks** — causal ordering to detect genuine concurrent conflicts vs. stale replicas

---

## Conflict Resolution Strategies

| Strategy | How it works | Best for |
|----------|-------------|----------|
| `lww` | **Last-Write-Wins** — the record with the highest wall-clock timestamp survives | Simple KV data, low conflict rate |
| `crdt` | **CRDT PN-Counter** — per-region credit/debit accumulators merged element-wise; balance is always derived; no update is ever lost | Financial balances, counters |
| `business` | **Business Rules** — balance via CRDT, tags via OR-Set union, metadata per-key LWW, owner first-write-wins | Complex domain objects with mixed field semantics |

Switch strategy via `MESH_CONFLICT_STRATEGY=lww|crdt|business`.

---

## Quick Start

### Prerequisites
- Docker & Docker Compose
- Python 3.12+ (for local dev / tests)

### Run with Docker Compose

```bash
git clone https://github.com/YOUR_USERNAME/multi-region-data-mesh.git
cd multi-region-data-mesh

# Start both regions
docker compose up --build -d

# Check they're healthy
curl http://localhost:8001/ping   # region-a
curl http://localhost:8002/ping   # region-b
```

Open the live dashboards:
- **Region A:** http://localhost:8001/dashboard
- **Region B:** http://localhost:8002/dashboard

Interactive API docs:
- http://localhost:8001/docs
- http://localhost:8002/docs

### Change Conflict Strategy

Edit `docker-compose.yml` and set `MESH_CONFLICT_STRATEGY` to `lww`, `crdt`, or `business`, then:

```bash
docker compose up -d
```

---

## Demo Scripts

### Seed sample accounts

```bash
pip install httpx
python scripts/seed_data.py
```

### Simulate concurrent conflicts

```bash
# Run with LWW (default)
python scripts/simulate_conflict.py

# Run with CRDT (no balance loss)
python scripts/simulate_conflict.py --strategy crdt

# Run with business rules
python scripts/simulate_conflict.py --strategy business
```

The script:
1. Creates an account on region-a
2. Waits for replication to region-b
3. Simultaneously writes to **both** regions to produce a real concurrent conflict
4. Waits for convergence
5. Prints the final state and health metrics from both nodes

---

## API Reference

### Consumer API

| Method | Path | Description |
|--------|------|-------------|
| `POST` | `/accounts` | Create account (written locally, replicated async) |
| `GET`  | `/accounts` | List all accounts (local read) |
| `GET`  | `/accounts/{id}` | Get account (local read) |
| `PATCH`| `/accounts/{id}/balance` | Apply balance delta (CRDT-safe) |
| `PUT`  | `/accounts/{id}/tags` | Replace tags |

### Health & Observability

| Method | Path | Description |
|--------|------|-------------|
| `GET`  | `/health` | Full region health + replication lag |
| `GET`  | `/ping` | Liveness probe |
| `GET`  | `/dashboard` | Live HTML dashboard |

### Internal Replication (peer-to-peer)

| Method | Path | Description |
|--------|------|-------------|
| `GET`  | `/internal/records?since=<ts>` | Pull delta records from a peer |
| `POST` | `/internal/records` | Push records from a peer |

---

## Health Dashboard

The `/dashboard` endpoint renders a live HTML page that auto-refreshes every 5 seconds showing:

- **Total accounts** in this regional store
- **Max replication lag** with a colour-coded gauge (green < 0.5 s, yellow < 2 s, red ≥ 2 s)
- **Conflicts resolved** count
- **Records replicated** in/out
- **Active conflict strategy** with description
- **Per-peer status** (reachable, lag, last sync)
- **Recent conflict event log** (last 10) with resolution labels

---

## Running Tests

```bash
pip install -r requirements.txt
pytest -v
```

Test coverage:
- `test_vector_clock.py` — causality semantics (dominates, concurrent, merge)
- `test_conflict_resolution.py` — all three strategies with edge cases
- `test_store.py` — SQLite upsert, conflict log, replication log, delta queries
- `test_api.py` — full HTTP layer via FastAPI test client

---

## Project Structure

```
multi-region-data-mesh/
├── docker-compose.yml          # Two-region local setup
├── Dockerfile                  # Single-node image
├── requirements.txt
├── pytest.ini
├── src/
│   ├── main.py                 # FastAPI app + lifespan
│   ├── config.py               # Env-based settings
│   ├── models.py               # Pydantic models + VectorClock
│   ├── store/
│   │   └── database.py         # SQLite regional store
│   ├── replication/
│   │   ├── engine.py           # Async pull+push replication loop
│   │   └── strategies.py       # LWW / CRDT / Business rule resolvers
│   ├── api/
│   │   ├── routes.py           # Consumer + internal replication routes
│   │   └── health.py           # /health + /ping
│   └── dashboard/
│       └── templates/
│           └── dashboard.html  # Live Jinja2 dashboard
├── tests/
│   ├── test_vector_clock.py
│   ├── test_conflict_resolution.py
│   ├── test_store.py
│   └── test_api.py
└── scripts/
    ├── seed_data.py            # Seed both regions with sample accounts
    └── simulate_conflict.py    # Produce + observe concurrent conflicts
```

---

## Design Decisions

### Why vector clocks instead of just timestamps?
Wall-clock timestamps are unreliable across nodes (clock skew, NTP drift). Vector clocks give us causal ordering: we can definitively say whether write B *happened after* write A, or whether they were *concurrent* — which is the only case requiring conflict resolution.

### Why SQLite?
Each region runs a fully independent database — no shared state anywhere. SQLite with WAL mode gives durable local writes with no external dependencies. In production you'd swap this for PostgreSQL, DynamoDB, or Spanner per region.

### Why a pull + push hybrid?
- **Pull** (periodic) guarantees convergence even if a push was dropped
- **Push** (after every write) reduces replication lag to near-zero under normal conditions
- Together they give you eventual consistency with low latency

### CRDT PN-Counter for balances
A simple `balance = float` field breaks under concurrent writes — you'd lose one of the updates. The PN-Counter pattern (per-region credit/debit accumulators, merged with element-wise max) is mathematically proven to converge and never lose a transaction, making it correct for financial data.

---

## Environment Variables

| Variable | Default | Description |
|----------|---------|-------------|
| `MESH_REGION_ID` | `region-a` | Unique identifier for this node |
| `MESH_PORT` | `8000` | HTTP port |
| `MESH_DB_PATH` | `/tmp/mesh.db` | SQLite database file path |
| `MESH_PEER_URLS` | `""` | Comma-separated peer base URLs |
| `MESH_REPLICATION_INTERVAL_SECONDS` | `2.0` | Seconds between pull cycles |
| `MESH_CONFLICT_STRATEGY` | `lww` | `lww` \| `crdt` \| `business` |
