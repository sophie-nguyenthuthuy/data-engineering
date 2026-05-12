# ⚡ Real-Time Systemic Risk Monitor

A streaming interbank contagion-risk detection system that ingests synthetic fund-flow transactions, builds a live directed graph of bilateral exposures in **Memgraph**, runs graph algorithms to detect systemic risk patterns, and surfaces structured alerts through a REST/WebSocket API and an interactive D3 dashboard.

---

## Architecture

```
┌──────────────────────────────────────────────────────────────────┐
│                      Systemic Risk Monitor                       │
│                                                                  │
│  TransactionGenerator                                            │
│  (synthetic interbank flows, ~5 tx/s)                            │
│          │                                                       │
│          ▼                                                       │
│  MemgraphClient  ◄──────────────────────────────────────────┐   │
│  • TRANSFERS edges (raw)                                     │   │
│  • NET_EXPOSURE edges (materialized, updated each tx)        │   │
│          │                                                   │   │
│  [every 5 s]                                                 │   │
│          ▼                                                   │   │
│  ┌───────────────────────────────┐                          │   │
│  │       Risk Analyzer           │                          │   │
│  │  • Cycle detection (Johnson's)│                          │   │
│  │  • Betweenness / PageRank     │                          │   │
│  │  • HHI + Gini concentration   │                          │   │
│  │  • Cascade simulation         │                          │   │
│  └──────────────┬────────────────┘                          │   │
│                 ▼                                            │   │
│           AlertEngine                                        │   │
│           (CRITICAL / HIGH / MEDIUM / INFO)                  │   │
│                 │                                            │   │
│        ┌────────┴────────┐                                   │   │
│        ▼                 ▼                                   │   │
│    REST API          WebSocket ──► Live Dashboard (D3)        │   │
│    (FastAPI)         broadcast                               │   │
└──────────────────────────────────────────────────────────────────┘
```

---

## Features

| Capability | Detail |
|---|---|
| **Streaming graph ingestion** | Synthetic transactions at configurable rate (default 5 tx/s); raw `TRANSFERS` edges + materialised `NET_EXPOSURE` aggregates written to Memgraph via Bolt |
| **Circular dependency detection** | Johnson's algorithm finds all simple cycles ≥ 3 hops; scored by total notional and bottleneck exposure |
| **Liquidity concentration** | Herfindahl-Hirschman Index (HHI) + Gini coefficient on outbound exposure distribution |
| **Systemic node identification** | Weighted betweenness centrality + PageRank; flags institutions whose removal would partition the network |
| **Contagion cascade simulation** | Threshold-cascade model — simulate any institution failing at configurable shock level; computes cascade depth and % of network affected |
| **Structured alerts** | Four severity levels (CRITICAL / HIGH / MEDIUM / INFO) across four categories (CYCLE / CONCENTRATION / SYSTEMIC_NODE / CONTAGION) |
| **REST API** | Full graph snapshot, latest metrics, alert log, per-institution detail, on-demand contagion simulation |
| **Live dashboard** | D3 force-directed graph with drag, zoom, tooltips; double-click a node to run a live cascade simulation and see results highlighted in-graph |

---

## Risk Algorithms

### 1 — Cycle Detection
Uses **Johnson's algorithm** (`networkx.simple_cycles`) on the `NET_EXPOSURE` directed graph.  
A cycle A→B→C→A means institution A is ultimately exposed to itself through intermediaries — the classic daisy-chain lending pattern that amplifies shocks.

```
risk_score = min(1.0, total_cycle_exposure / $10B)
```

### 2 — Liquidity Concentration (HHI + Gini)
Treats each institution's total outbound exposure as its "market share":

```
HHI = Σ sᵢ²    (0 = perfectly distributed, 1 = monopoly)
alert threshold: HHI > 0.25
```

### 3 — Betweenness Centrality & PageRank
Identifies **too-central-to-fail** institutions.  
Betweenness ≥ 35% → `SYSTEMIC_NODE` alert.  
PageRank weights importance by the importance of counterparties.

### 4 — Threshold Cascade Simulation
BFS from a seed institution:
- Seed suffers a 30% liquidity shock (configurable)
- Each downstream institution fails if the fraction of its expected inflow that disappears ≥ 50% (configurable)
- Cascade repeats until no new failures

---

## Quick Start

### Prerequisites
- Docker + Docker Compose
- (Optional) Python 3.12+ for local dev

### Run with Docker Compose

```bash
git clone https://github.com/sophie-nguyenthuthuy/systemic-risk-monitor.git
cd systemic-risk-monitor

# Optional: override settings
cp .env.example .env

docker compose up --build
```

| Service | URL |
|---|---|
| **Dashboard** | http://localhost:8000 |
| **API docs** | http://localhost:8000/docs |
| **Memgraph Lab** | http://localhost:3000 |

### Run locally (no Docker)

```bash
# 1. Start Memgraph
docker run -p 7687:7687 memgraph/memgraph:latest

# 2. Install deps
python -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt

# 3. Run
cp .env.example .env
python -m src.main
```

---

## API Reference

### REST

| Method | Path | Description |
|---|---|---|
| `GET` | `/api/health` | Liveness check |
| `GET` | `/api/graph` | All nodes + NET_EXPOSURE edges |
| `GET` | `/api/metrics` | Latest risk metrics snapshot |
| `GET` | `/api/alerts?limit=50` | Recent alert log |
| `GET` | `/api/institutions/{id}` | Single institution detail + exposures |
| `POST` | `/api/simulate/{id}?shock_pct=0.30` | Run contagion cascade from institution |

### WebSocket

Connect to `ws://localhost:8000/ws` to receive real-time JSON frames:

```jsonc
// Every 5 seconds:
{
  "type": "metrics",
  "data": {
    "tx_count": 1420,
    "node_count": 20,
    "edge_count": 87,
    "cycles": [...],
    "concentration": { "hhi": 0.18, "gini": 0.61, "is_concentrated": false },
    "top_systemic_nodes": [...],
    "worst_cascade": { "seed": "BANK-01", "fraction_failed": 0.35, ... },
    "recent_alerts": [...]
  }
}
```

---

## Configuration

All settings via environment variables (see `.env.example`):

| Variable | Default | Description |
|---|---|---|
| `MEMGRAPH_HOST` | `localhost` | Memgraph hostname |
| `MEMGRAPH_PORT` | `7687` | Memgraph Bolt port |
| `NUM_INSTITUTIONS` | `20` | Synthetic institution count |
| `TRANSACTION_INTERVAL_MS` | `200` | ms between transactions |
| `CONCENTRATION_HHI_THRESHOLD` | `0.25` | HHI alert trigger |
| `BETWEENNESS_THRESHOLD` | `0.35` | Systemic node alert trigger |
| `LIQUIDITY_SHOCK_PCT` | `0.30` | Fraction of outflow lost on failure |
| `CONTAGION_CASCADE_THRESHOLD` | `0.50` | Cascade propagation threshold |

---

## Project Structure

```
systemic-risk-monitor/
├── docker-compose.yml
├── Dockerfile
├── requirements.txt
├── .env.example
├── dashboard/
│   └── index.html              # D3 live dashboard
└── src/
    ├── main.py                 # Orchestrator / entry point
    ├── config.py               # Pydantic settings
    ├── generator/
    │   └── transaction_generator.py   # Synthetic flow generator
    ├── graph/
    │   └── memgraph_client.py  # Bolt driver, schema, ingest
    ├── algorithms/
    │   ├── cycle_detection.py  # Johnson's cycle detection
    │   ├── centrality.py       # Betweenness, PageRank, HHI, Gini
    │   └── contagion.py        # Threshold cascade simulation
    ├── alerts/
    │   └── alert_engine.py     # Alert evaluation & dispatch
    └── api/
        └── server.py           # FastAPI REST + WebSocket
```

---

## Dashboard

The live dashboard at `http://localhost:8000` shows:

- **Force-directed graph** — nodes sized by tier, edges weighted by net bilateral exposure
- **Risk metrics panel** — HHI, Gini, cycle count, systemic node count, worst-case cascade spread
- **Alert feed** — colour-coded severity stream (CRITICAL → red, HIGH → orange, MEDIUM → yellow)
- **Interactive simulation** — double-click any institution node to run a live cascade simulation; affected nodes flash red/orange

---

## License

MIT
