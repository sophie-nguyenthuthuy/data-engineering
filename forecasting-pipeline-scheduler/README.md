# forecasting-pipeline-scheduler

A pipeline scheduler that

- forecasts per-task runtimes from a **lognormal** model fit online and
  guards them with a two-sided **CUSUM** drift detector;
- dispatches DAGs through a **critical-path-first list scheduler** with
  worker-best-fit placement;
- on small DAGs (≤ 12 tasks) refines to the exact optimum with
  **branch-and-bound** + critical-path lower-bound pruning;
- and reports **shadow-mode regret** against a FCFS baseline across a
  workload of synthetic DAGs.

[![Python](https://img.shields.io/badge/python-3.10%20%7C%203.11%20%7C%203.12-blue.svg)](#)
[![License: MIT](https://img.shields.io/badge/license-MIT-green.svg)](LICENSE)

## Why

Airflow-style topological-order FCFS dispatch is a reasonable default,
but for workloads with predictable runtime distributions a
queuing-/critical-path-aware scheduler typically shaves 10–30 % off
makespan by dispatching long jobs earlier and packing short jobs into
the gaps. The general problem is NP-hard; this project ships a fast
approximation with an exact small-case solver and the bookkeeping
needed to validate adoption.

## Architecture

```
   Job arrivals + DAG edges
              │
              ▼
   ┌────────────────────────────┐
   │   LognormalForecaster      │  per-task μ, σ from observed runtimes
   │   + CUSUMDetector (drift)  │  forecast invalidated on drift
   └────────────┬───────────────┘
                │
                ▼
   ┌────────────────────────────┐
   │   list_schedule            │  critical-path-first list scheduler
   │   (b_level priority)       │  worker-best-fit placement
   └────────────┬───────────────┘
                │
                ▼
   ┌────────────────────────────┐
   │   branch_and_bound         │  exact optimum on small DAGs
   │   (max_tasks ≤ 12,         │  CP + finish lower-bound pruning
   │    time_limit_ms)          │  falls back to list_schedule
   └────────────┬───────────────┘
                │
                ▼
   ┌────────────────────────────┐
   │   regret_over_dags         │  vs. baseline_fcfs_schedule
   │   mean / median / p95      │  across a workload of DAGs
   └────────────────────────────┘
```

## Install

```bash
pip install -e ".[dev]"
```

Python 3.10+. **Zero runtime dependencies** — stdlib only.

## CLI

```bash
fpsctl info                                                  # version
fpsctl bench    --layers 5 --width 4 --workers 3             # three schedulers
fpsctl regret   --n-dags 200 --layers 5 --workers 3          # aggregate regret
fpsctl forecast --samples 2000 --mu 2.0 --sigma 0.5          # lognormal fit
```

Example `fpsctl bench`:

```
tasks=8  workers=2  seed=0
  baseline_makespan      = 7.1887
  list_schedule_makespan = 6.8290
  branch_and_bound       = 6.5684
```

Example `fpsctl regret`:

```
n_dags=50  workers=3  mean_regret=+1.2206  median_regret=+0.6266
p95_regret=+4.7161  mean_speedup=1.0753  positive_fraction=0.760
```

`positive_fraction=0.760` means the smarter scheduler beat baseline on
76 % of DAGs — the metric to chart over time before flipping
production.

## Library

```python
from fps.dag                    import DAG, Task
from fps.forecast.lognormal     import LognormalForecaster
from fps.forecast.cusum         import CUSUMDetector
from fps.scheduler.list_sched   import list_schedule
from fps.scheduler.baseline     import baseline_fcfs_schedule
from fps.scheduler.branch_bound import branch_and_bound
from fps.scheduler.common       import makespan, assert_valid_schedule
from fps.shadow                 import regret, regret_over_dags
from fps.bench                  import random_layered_dag

# 1. Build a DAG.
dag = DAG()
dag.add(Task("extract", duration=4.0))
dag.add(Task("transform", duration=3.0, deps=("extract",)))
dag.add(Task("load", duration=1.0, deps=("transform",)))

# 2. Schedule it.
plan = list_schedule(dag, num_workers=2)
assert_valid_schedule(dag, plan, num_workers=2)  # raises on any invariant
print(makespan(plan))

# 3. Forecast runtimes online; reset on drift.
fc = LognormalForecaster()
cd = CUSUMDetector(mean=4.0, sigma=0.5)
for sample in stream_of_observed_runtimes:
    fc.observe("extract", sample)
    if cd.update(sample):
        fc.reset("extract")
        cd.reset()

# 4. Shadow-mode regret over a workload.
dags = [random_layered_dag(seed=i) for i in range(200)]
agg = regret_over_dags(dags, num_workers=3)
print(agg.mean_regret, agg.mean_speedup, agg.positive_fraction())
```

## Components

| Module                            | Role                                                                  |
| --------------------------------- | --------------------------------------------------------------------- |
| `fps.dag`                         | `Task`, `DAG`, `CycleError`; Kahn topo + critical-path                |
| `fps.forecast.lognormal`          | `LognormalForecaster`, `TaskStats`, Acklam inverse-Φ                  |
| `fps.forecast.cusum`              | `CUSUMDetector` — two-sided Page CUSUM over standardised residuals    |
| `fps.scheduler.common`            | `ScheduledTask`, `Schedule`, `makespan`, `assert_valid_schedule`      |
| `fps.scheduler.list_sched`        | Critical-path-first (b-level) list scheduler                          |
| `fps.scheduler.baseline`          | Topological-order FCFS shadow baseline                                |
| `fps.scheduler.branch_bound`      | B&B with CP lower-bound pruning + time-limit fallback                 |
| `fps.shadow`                      | `regret`, `regret_over_dags`, `RegretReport`, `AggregateRegret`       |
| `fps.bench`                       | `random_layered_dag` — lognormal-duration layered DAG generator       |
| `fps.cli`                         | `fpsctl info | bench | regret | forecast`                            |

## Schedule invariants

`assert_valid_schedule(dag, schedule, num_workers)` enforces:

1. **Completeness** — every DAG task is in the schedule.
2. **Dependency order** — every task starts ≥ the latest finish of its
   upstream deps (within `tol = 1e-9`).
3. **Single-assignment per worker** — no two tasks overlap on the same
   worker.

All three schedulers (FCFS, list, B&B) are property-tested against this
invariant on randomly generated DAGs.

## Branch-and-bound

```
upper bound  = list_schedule(dag, workers)              # incumbent
lower bound  = max(current_finish, max(b_level over remaining tasks))
prune        when lower_bound ≥ best_makespan
```

`max_tasks = 12` keeps the search tree small enough that the
`time_limit_ms` (100 ms default) is rarely tight; for larger DAGs the
function silently returns the list-schedule result so the caller is
guaranteed a valid schedule.

## Forecasting + drift

`LognormalForecaster` keeps `(n, Σ log d, Σ log² d)` per task and
recovers `(μ̂, σ̂²)` by MLE on the log-durations.
`LognormalForecaster.p95(task)` uses an Acklam inverse-Φ approximation
(verified against `Φ⁻¹(0.975) ≈ 1.96` and `Φ⁻¹(0.025) ≈ -1.96` in the
tests).

`CUSUMDetector` is a two-sided Page (1954) CUSUM with reference value
`k` (default 0.5) and decision threshold `h` (default 5). The drift
tests confirm it fires within 60 samples on a 2 σ shift in either
direction and does *not* fire on 500 in-distribution samples.

## Quality

```bash
make lint        # ruff   (E, W, F, I, B, UP, SIM, RUF, TC)
make format      # ruff format
make type        # mypy --strict
make test        # 64 tests
make bench       # CLI single-DAG comparison
make regret      # CLI 200-DAG aggregate
make forecast    # CLI lognormal fit
make docker      # production image
```

- **64 tests**, 0 failing; includes 2 Hypothesis properties (list
  schedule is always valid; makespan ≥ critical-path length).
- `mypy --strict` clean over 13 source files.
- Python 3.10 / 3.11 / 3.12 CI matrix + Docker build smoke step.
- Multi-stage slim Docker image, non-root `fps` user.
- **Zero runtime dependencies.**

## References

- Pinedo. *Scheduling: Theory, Algorithms, and Systems.* 5th ed., 2016.
- Hu. *Parallel sequencing and assembly line problems.* OR 1961. (b-level)
- Kahn. *Topological sorting of large networks.* CACM 1962.
- Page. *Continuous inspection schemes.* Biometrika 1954. (CUSUM)
- Verma et al. *Borg.* EuroSys 2015.

## License

MIT — see [LICENSE](LICENSE).
