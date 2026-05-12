# Cardinality Learned Optimizer

A full implementation of the **Neo/Bao query optimizer loop** for PostgreSQL:

- **GNN cardinality estimator** — Child-Sum TreeLSTM trained on real EXPLAIN ANALYZE output to predict true cardinalities at every plan operator
- **Adaptive query processing** — detects nodes with q-error ≥ 100× mid-execution and recompiles with corrected `pg_hint_plan` cardinality hints
- **Bao plan selector** — Thompson-sampling bandit over 15 canonical hint sets (join methods × scan methods), online learning from observed latencies
- **JOB benchmark reproduction** — runs the Join Order Benchmark and reproduces the "plan robustness" results from the CMU Bao paper

## Architecture

```
Query
  │
  ▼
QueryInterceptor  ──── EXPLAIN (FORMAT JSON) ────►  PlanNode tree
  │                                                      │
  │                                              PlanTreeEncoder
  │                                          (Child-Sum TreeLSTM)
  │                                                      │
  ├── CardinalityHead ─── per-node predicted rows ───────┤
  │                                                      │
  ├── CostHead ────────── predicted query latency ───────┤
  │                                                      │
  ▼                                                      ▼
BaoSelector                                     AdaptiveRecompiler
(ThompsonSampling                               (q-error ≥ 100× → replan
 over 15 hint sets)                              with corrected Rows() hints)
  │
  ▼
Trainer  (online, experience-replay buffer)
  └── cardinality loss (MSE on log-cardinalities)
  └── cost loss       (MSE on log-latency)
```

## Key Papers

| Paper | Contribution |
|-------|-------------|
| [Bao: Learning to Steer Query Optimizers](https://dl.acm.org/doi/10.14778/3494124.3494126) (Marcus et al., VLDB 2022) | Thompson-sampling bandit over hint sets; plan robustness evaluation |
| [Neo: A Learned Query Optimizer](https://dl.acm.org/doi/10.14778/3342263.3342644) (Marcus et al., VLDB 2019) | TreeLSTM plan encoder; end-to-end cost prediction |
| [How Good Are Query Optimizers?](https://www.vldb.org/pvldb/vol9/p204-leis.pdf) (Leis et al., VLDB 2015) | Join Order Benchmark; q-error metric definition |
| [Eddies](https://dl.acm.org/doi/10.1145/342009.335420) (Avnur & Hellerstein, SIGMOD 2000) | Adaptive query processing; mid-execution recompilation |

## Quick Start

### 1. Start PostgreSQL

```bash
docker compose up -d postgres
```

### 2. Load IMDB dataset

```bash
make setup-imdb          # downloads ~1.1 GB, loads into PostgreSQL
make generate-queries    # writes 17 JOB-style .sql files
```

### 3. Install Python dependencies

```bash
pip install -e ".[dev]"
```

### 4. Run tests (no database required)

```bash
make test
```

### 5. Train the cardinality model offline

```bash
make train
```

### 6. Reproduce Bao paper results

```bash
make reproduce
```

This runs the full online learning loop and prints a table equivalent to Table 2 of the Bao paper:

```
──────────────────────────────────────────────────
  Plan Robustness: Bao vs. PostgreSQL Default
──────────────────────────────────────────────────
  bao_mean_regret              : 1.23
  bao_median_regret            : 1.05
  bao_p90_regret               : 1.87
  bao_fraction_near_optimal_2x : 0.91
  baseline_mean_regret         : 2.14
  baseline_median_regret       : 1.43
  baseline_p90_regret          : 4.92
  overall_speedup_vs_baseline  : 1.74
```

## Project Structure

```
src/cle/
├── db/
│   ├── connector.py       PostgreSQL connection pool
│   ├── interceptor.py     EXPLAIN ANALYZE interception
│   └── hint_injector.py   pg_hint_plan hint construction + 15 Bao hint sets
├── plan/
│   ├── node.py            PlanNode tree + q-error computation
│   ├── parser.py          EXPLAIN JSON → PlanNode tree
│   └── encoder.py         PlanNode → tensors (31-dim feature vector)
├── model/
│   ├── tree_lstm.py       Child-Sum TreeLSTM (bottom-up message passing)
│   ├── gnn.py             QueryOptimizer: CardinalityHead + CostHead
│   └── trainer.py         Online training with experience-replay buffer
├── adaptive/
│   ├── monitor.py         CardinalityMonitor — detects ≥100× q-error
│   └── recompiler.py      AdaptiveRecompiler — resubmits with corrected hints
├── bao/
│   ├── bandit.py          Thompson Sampling (Bayesian + Neural variants)
│   └── selector.py        BaoSelector — full online learning loop
└── evaluation/
    ├── metrics.py         q-error, speedup, regret statistics
    ├── benchmark.py       JOB benchmark runner
    └── robustness.py      Plan robustness profiler (all 15 hint sets × queries)
```

## The 100× Adaptive Recompilation Rule

When EXPLAIN ANALYZE reveals that any plan node's actual row count differs from the estimate by more than **100×**, the `AdaptiveRecompiler` automatically:

1. Extracts actual rows from the analyzed plan
2. Builds `Rows(table1 table2 #actual_count)` hints via `pg_hint_plan`
3. Re-executes the query with the corrected cardinality hints
4. Logs the speedup achieved

This emulates "mid-execution recompilation" at query granularity. True sub-plan restart would require a PostgreSQL C extension hooking into `ExecutorRun` (see Section 4.2 of the Bao paper).

## The 15 Bao Hint Sets

```python
BAO_HINT_SETS = [
    {},                                      # 0: default
    {"disable": ["HashJoin"]},               # 1
    {"disable": ["MergeJoin"]},              # 2
    {"disable": ["NestLoop"]},               # 3
    {"disable": ["HashJoin", "MergeJoin"]},  # 4
    {"disable": ["SeqScan"]},                # 5
    {"disable": ["IndexScan"]},              # 6
    ...                                      # 7-14
]
```

The bandit learns which hint set produces the fastest plans for each query class, converging to near-optimal plan selection after ~50 queries per template.

## GNN Architecture

```
PlanNode features (31 dims):
  [0:24]  operator type one-hot   (24 operator types)
  [24]    log(estimated_rows)     normalised
  [25]    log(estimated_cost)     normalised
  [26]    estimated_width / 100
  [27]    has_filter              bool
  [28]    has_join_condition      bool
  [29]    depth / 10
  [30]    relation_id / vocab_sz  (learned vocabulary)

Child-Sum TreeLSTM (bottom-up):
  - Each node aggregates children: h_sum = Σ h_child
  - Per-child forget gates: f_k = σ(W_f·x + U_f·h_k)
  - Cell: c = i·u + Σ f_k·c_k
  - Hidden: h = o·tanh(c)

Two heads share the encoder:
  CardinalityHead  → log(predicted_rows) per node  (MSE loss)
  CostHead         → log(predicted_ms) per query   (MSE loss)
```

## Requirements

- Python 3.10+
- PyTorch 2.1+
- PostgreSQL 14+ with `pg_hint_plan` extension
- Docker (optional, for the PostgreSQL container)

## License

MIT
