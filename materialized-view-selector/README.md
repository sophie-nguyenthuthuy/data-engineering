# Self-Optimizing Materialized View Selector

Solves the **view selection problem** (NP-hard) for BigQuery and Snowflake workloads using a **greedy + simulated annealing** approach. Automatically creates, maintains, and drops materialized views, then tracks actual savings vs. predicted to continuously calibrate the cost model.

---

## How it works

```
Query worklog
     │
     ▼
QueryAnalyzer          ← SQL fingerprinting, CTE/subquery extraction
     │
     ▼
CandidateView[]        ← one view per recurring pattern
     │
     ▼
CostModel              ← benefit / storage / maintenance estimates
     │  ↑ calibration feedback (actual ÷ predicted EMA)
     ▼
Optimizer              ← Greedy seed → Simulated Annealing
     │
     ▼
ViewScheduler          ← CREATE / DROP views in warehouse
                       ← measure actual savings → recalibrate
```

### Optimization problem

Given a workload **W** and a storage budget **B**:

```
maximize   Σ_{v ∈ S}  (benefit(v) − maintenance_cost(v))
subject to Σ_{v ∈ S}  storage_bytes(v)  ≤  B
           S ⊆ CandidateViews
```

**Greedy** — O(n log n): sort by `net_benefit / storage_bytes`, take until budget exhausted.  
**Simulated Annealing** — stochastic local search seeded from greedy, neighbourhood operators: add / remove / swap.

### Cost model calibration

After each measurement period the scheduler compares predicted vs. actual savings per view and updates an **EMA calibration ratio**:

```
ratio_new = α × (actual / predicted) + (1 − α) × ratio_old   (α = 0.1)
future_estimate = raw_estimate × ratio
```

Calibration ratios are persisted to `.mv_calibration.json` and survive restarts.

---

## Installation

```bash
pip install -e ".[sql]"                  # core + sqlglot (recommended)
pip install -e ".[bigquery,sql]"         # + BigQuery adapter
pip install -e ".[snowflake,sql]"        # + Snowflake adapter
pip install -e ".[all,dev]"              # everything + dev tools
```

---

## Quick start

### 1. Analyse a local worklog (no warehouse needed)

Create a JSONL file where each line is a query record:

```jsonl
{"sql": "SELECT user_id, SUM(revenue) FROM orders GROUP BY user_id", "cost_usd": 2.5, "bytes_processed": 500000000, "duration_ms": 1200, "executed_at": "2025-01-15T10:00:00"}
{"sql": "SELECT user_id, SUM(revenue) FROM orders GROUP BY user_id", "cost_usd": 2.3, "bytes_processed": 490000000, "duration_ms": 1100, "executed_at": "2025-01-15T11:00:00"}
```

Then run:

```bash
mv-selector analyse --worklog queries.jsonl --budget-gb 200 --warehouse bigquery
```

### 2. Full cycle against BigQuery

```bash
export GOOGLE_APPLICATION_CREDENTIALS=/path/to/key.json

mv-selector run \
  --warehouse bigquery \
  --project my-gcp-project \
  --dataset analytics.mv_auto \
  --budget-gb 500
```

### 3. Full cycle against Snowflake

```bash
export SNOWFLAKE_PASSWORD=...

mv-selector run \
  --warehouse snowflake \
  --account myaccount.us-east-1 \
  --user analyst \
  --dataset ANALYTICS.PUBLIC
```

### 4. Status

```bash
mv-selector status --warehouse bigquery --project my-gcp-project
```

---

## Python API

```python
from mv_selector import ViewScheduler, SchedulerConfig
from mv_selector.adapters.bigquery import BigQueryAdapter

adapter = BigQueryAdapter(project="my-gcp-project")
cfg = SchedulerConfig(
    budget_bytes=500 * 1024**3,
    target_dataset_or_schema="analytics.mv_auto",
    lookback_days=30,
)
scheduler = ViewScheduler(adapter=adapter, config=cfg)
result = scheduler.run_cycle()

print(f"Selected {len(result.selected)} views")
print(f"Net benefit: ${result.net_benefit_usd:.2f}/mo")
```

Use the optimizer directly (no warehouse required):

```python
from mv_selector.optimizer import AnnealingSelector, GreedySelector

greedy = GreedySelector().select(candidates, budget_bytes=200 * 1024**3)
sa = AnnealingSelector(max_iterations=50_000, seed=42)
result = sa.select(candidates, budget_bytes=200 * 1024**3, greedy_seed=greedy.selected)
```

---

## Running tests

```bash
pip install -e ".[dev,sql]"
pytest
```

---

## Architecture

```
src/mv_selector/
├── models.py            QueryRecord, CandidateView, MaterializedView, OptimizationResult
├── query_analyzer.py    SQL fingerprinting, CTE/subquery extraction → CandidateView[]
├── cost_model.py        Benefit/cost estimation + EMA calibration
├── optimizer.py         GreedySelector, AnnealingSelector
├── scheduler.py         ViewScheduler — end-to-end orchestration + state persistence
├── adapters/
│   ├── base.py          BaseAdapter interface
│   ├── bigquery.py      BigQuery: history fetch, CREATE/DROP MV, savings measurement
│   └── snowflake.py     Snowflake: same interface
└── worklog/
    ├── store.py          SQLite worklog (dedup by fingerprint, frequency tracking)
    └── collector.py      Pulls from adapter → store
```

---

## Configuration

Copy `config.yaml` to `config.local.yaml` and adjust:

| Key | Default | Description |
|---|---|---|
| `optimization.budget_gb` | 500 | Total storage budget for all MVs |
| `optimization.lookback_days` | 30 | Days of query history to analyse |
| `optimization.min_query_frequency` | 3 | Min appearances for a pattern to be a candidate |
| `optimization.sa_max_iterations` | 50 000 | SA iteration cap |
| `optimization.min_net_benefit_usd` | 1.0 | Drop views below this monthly net benefit |

---

## License

MIT
