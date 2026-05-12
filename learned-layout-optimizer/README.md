# learned-layout-optimizer

A background agent that **continuously reorganizes physical data layout** — Z-order, Hilbert curve, learned sort orders — by treating layout as a **reinforcement-learning problem**. Reward: query speedup. Cost: reorganization I/O. The neural layout policy generalizes to unseen query predicates.

> **Status:** Design / spec phase.

## Why

Static Z-ordering picks one fixed dimension priority and lives with it forever. Real workloads drift: a column hot today is cold next month. Static layouts pay the worst-case of every workload they've ever seen. An RL agent can re-converge to the new optimum.

The bet: the *amortised* benefit of online retuning exceeds the I/O cost of layout rewrites.

## Architecture

```
            ┌────────────────────────────────────────┐
            │              Query workload            │
            │    (predicates, projections, joins)    │
            └─────────────────┬──────────────────────┘
                              │
                              ▼
                  ┌───────────────────────┐
                  │  Workload profiler    │   per-column access frequency,
                  │                       │   predicate selectivity, join keys
                  └─────────┬─────────────┘
                            │
                            ▼
                  ┌───────────────────────┐
                  │  Policy network       │   state: profile + current layout
                  │  (graph NN over       │   action: {keep, rewrite_zorder(cols),
                  │   table columns)      │            rewrite_hilbert(cols),
                  │                       │            rewrite_sortkey(col), ...}
                  └─────────┬─────────────┘
                            │
                            ▼
                  ┌───────────────────────┐
                  │  Layout executor      │   rewrites Parquet files
                  │  (Delta / Iceberg)    │
                  └─────────┬─────────────┘
                            │
                            ▼
                       reward signal
                  (next-window query speedup - I/O cost)
```

## Components

| Module | Role |
|---|---|
| `src/profiler/` | Parses query plans → per-column access stats |
| `src/state/` | Snapshot of (workload profile, current layout) as feature vector |
| `src/policy/gnn.py` | Graph neural net over column-correlation graph |
| `src/actions/` | Z-order / Hilbert / sort-key rewriters (Delta + Iceberg) |
| `src/reward/` | Replay query log against new layout in shadow; measure latency delta |
| `src/training/` | Offline PPO trainer on logged workload + simulated layouts |
| `src/agent/` | Production loop: act → measure → update |

## State representation

Per-table state vector:

```
{
  schema:         [(col_name, dtype, ndv)],
  layout:         {sort_key, zorder_cols, partition_cols},
  workload (1h):  {
    predicate_freq:    col → P(filtered),
    predicate_selec:   col → avg selectivity,
    range_pred_freq:   col → P(range),
    join_keys:         col → join frequency,
  },
  storage:        {file_count, avg_file_size, total_bytes},
  recent_actions: [(timestamp, action, observed_reward)]
}
```

Encoded into a graph: nodes = columns, edges = pairwise correlation in queries, fed into a GNN.

## Action space

| Action | Meaning |
|---|---|
| `noop` | keep current layout |
| `rewrite_zorder(cols)` | full Z-order across cols |
| `rewrite_hilbert(cols)` | Hilbert curve (better than Z-order for 3+ dims, locally) |
| `rewrite_sortkey(col)` | single-key clustered sort |
| `repartition(col, bins)` | physical partition by col |
| `local_resort(file_id)` | per-file sort, no global rewrite (cheap) |

Each carries an estimated I/O cost (rows × bytes × rewrite_factor).

## Reward

```
reward = Σ_q (latency_baseline(q) - latency_new(q)) - α * I/O_cost
```

Measured by **shadow replay**: every layout candidate is evaluated by re-running last hour's query log against a small sample of the data; estimated speedup extrapolated by query selectivity.

`α` is a knob trading off responsiveness vs. churn.

## Generalisation test

Held-out test: train on weeks 1–3 of a workload, evaluate on week 4. Compare against:
- static Z-order (one-time tuning)
- random layouts (lower bound)
- oracle (best layout chosen with full week 4 knowledge — upper bound)

Target: ≥ 70% of oracle gap closed, vs. ~20% for static Z-order.

## Benchmarks

- **TPC-DS**, shifted-workload variant: dimension priorities change every 1000 queries.
- **NYC taxi**: temporal locality + spatial range queries.
- **GitHub Archive**: predicate distribution drifts as repos popularise/die.

## References

- Sun et al., "An End-to-End Learning Approach to Database Index Tuning" (VLDB 2020)
- Kraska et al., "The Case for Learned Index Structures" (SIGMOD 2018) — adjacent theme
- Z-order: Morton (1966); Hilbert curve: Hilbert (1891), Faloutsos (1986)

## Roadmap

- [ ] Workload profiler from query logs (DuckDB / Spark / BQ)
- [ ] State encoder (per-table feature vector + column-correlation graph)
- [ ] GNN policy + PPO trainer
- [ ] Layout action implementations (Delta + Iceberg)
- [ ] Shadow-replay reward signal
- [ ] Drift detection → policy refresh
- [ ] TPC-DS shifted-workload benchmark
