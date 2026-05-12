# Differential Privacy Budget Manager

A query gateway that tracks cumulative privacy budget (ε) consumed per dataset per analyst. Implements **Laplace** and **Gaussian** mechanisms with a data-owner budget allocation UI. Built for banking and healthcare data pipelines.

## Features

- **Privacy budget tracking** — per (dataset, analyst) pair with precise ε/δ accounting
- **Laplace mechanism** — Lap(0, Δf/ε) noise for pure ε-DP
- **Gaussian mechanism** — N(0, σ²) noise for (ε, δ)-DP with calibrated σ
- **Exhaustion policies** — block queries or inject heavy noise when budget is spent
- **Data-owner UI** — grant, adjust, and reset budgets; register datasets and analysts
- **Query gateway** — submit queries with mechanism override, automatic sensitivity lookup
- **Audit log** — full history of every query with noise added and budget remaining

## Stack

| Layer    | Technology         |
|----------|--------------------|
| Backend  | FastAPI + SQLAlchemy + SQLite |
| Frontend | React 18 + TypeScript + Vite |
| Charts   | Recharts            |
| Noise    | NumPy               |

## Quick Start

### Backend

```bash
cd backend
python -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt

# Seed demo data (patients, banking, salaries + analysts + budgets)
python seed_data.py

# Start API server
uvicorn app.main:app --reload
# → http://localhost:8000
# → http://localhost:8000/docs  (Swagger UI)
```

### Frontend

```bash
cd frontend
npm install
npm run dev
# → http://localhost:5173
```

## Architecture

```
┌─────────────────┐      HTTP/JSON      ┌──────────────────────────┐
│   React UI      │ ──────────────────► │  FastAPI Gateway          │
│                 │                     │                            │
│  Dashboard      │                     │  POST /queries/            │
│  Query Gateway  │                     │   ├─ check budget          │
│  Allocations    │                     │   ├─ apply Laplace/Gauss   │
│  Datasets       │                     │   └─ debit ε, log result   │
└─────────────────┘                     │                            │
                                        │  GET  /budgets/summary/all │
                                        │  POST /budgets/            │
                                        │  PATCH /budgets/{id}       │
                                        └──────────┬───────────────┘
                                                   │
                                            SQLite (dp_budget.db)
```

## Privacy Mechanisms

### Laplace Mechanism (pure ε-DP)
```
noise ~ Laplace(0, Δf / ε)
```
Standard deviation = √2 · Δf / ε. Use for COUNT, SUM, HISTOGRAM queries.

### Gaussian Mechanism ((ε, δ)-DP)
```
σ = Δf · √(2 ln(1.25/δ)) / ε
noise ~ N(0, σ²)
```
Requires δ > 0. Provides tighter utility for MEAN queries at the cost of a small δ failure probability.

## API Reference

| Method | Path | Description |
|--------|------|-------------|
| POST | `/queries/` | Submit a query through the privacy gateway |
| GET | `/queries/logs` | Retrieve audit log |
| GET | `/budgets/summary/all` | Dashboard summary |
| POST | `/budgets/` | Create budget allocation |
| PATCH | `/budgets/{id}` | Adjust total ε or policy |
| POST | `/budgets/{id}/reset` | Reset consumed ε to 0 |
| GET | `/datasets/` | List datasets |
| GET | `/analysts/` | List analysts |

Full interactive docs at `http://localhost:8000/docs`.

## Demo Data

The seed script loads:

| Dataset | Owner | Sensitivity |
|---------|-------|-------------|
| Patient Records | owner-1 | 500 |
| Banking Transactions | owner-1 | 10,000 |
| Employee Salaries | owner-2 | 250,000 |

Four analysts (alice, bob, carol, dave) with varying budgets and exhaustion policies.
