# forecasting-pipeline-scheduler

A Kubernetes-native pipeline scheduler that models the entire job dependency graph as a **Jackson queuing network**, solves for an optimal dispatch order that minimizes makespan, and continuously re-solves as new jobs arrive and durations drift from forecasts. Runs in **shadow mode** alongside Airflow's default to measure regret.

> **Status:** Design / spec phase.

## Why

Airflow's default scheduler dispatches in DAG order with greedy slot filling. That's not bad — but it's also not optimal. For workloads with predictable runtime distributions, a **queuing-theory-aware** scheduler can shave 10–30 % off makespan by dispatching long jobs earlier and short jobs into the gaps.

The problem is NP-hard in general; the project is to build a fast-enough approximation that stays useful under live traffic.

## Architecture

```
                Job arrivals + DAG edges
                          │
                          ▼
              ┌────────────────────────────┐
              │  Runtime forecaster         │   per-task historical
              │  (per-task duration dist)   │   distribution (e.g., LogNorm)
              └────────────┬───────────────┘
                           │
                           ▼
              ┌────────────────────────────┐
              │  Jackson-network model      │   stations = executor pools,
              │                             │   service rates from forecasts
              └────────────┬───────────────┘
                           │
                           ▼
              ┌────────────────────────────┐
              │  Scheduler                 │   list scheduling on critical path,
              │  - critical-path heuristic │   branch-and-bound on small subgraphs
              │  - B&B on subgraphs ≤ 16   │
              │  - re-solve every 5 s      │
              └────────────┬───────────────┘
                           │
                           ▼
              ┌────────────────────────────┐
              │  Dispatcher                │   submits to K8s job API
              └────────────┬───────────────┘
                           │
                  shadow comparison
                           │
              ┌────────────▼───────────────┐
              │  Regret measurement        │   actual makespan vs.
              │  vs. Airflow baseline      │   counterfactual baseline
              └────────────────────────────┘
```

## Components

| Module | Role |
|---|---|
| `src/forecast/duration.py` | Per-task duration distribution, online-updated |
| `src/model/jackson.py` | Jackson-network steady-state (open + closed) |
| `src/scheduler/critical_path.py` | List-scheduling on critical-path-first heuristic |
| `src/scheduler/branch_bound.py` | B&B for subgraphs ≤ 16 tasks (exact small-case optimum) |
| `src/scheduler/loop.py` | Re-solve every Δt; partial solution carryover |
| `src/dispatcher/k8s.py` | Kubernetes Job submission via batch API |
| `src/shadow/` | Run alongside baseline scheduler; produce regret reports |
| `src/airflow_plugin/` | Pluggable scheduler exposed as Airflow Executor |

## The optimization

State: DAG `G = (V, E)`, per-task duration RVs `D_v`, executor pools with capacities `c_p`, current time `t`.

Find dispatch order π that minimises:

```
E[ makespan(π, G, D) ]
```

NP-hard. Approach:

1. **Forecast** each `D_v` from its historical distribution. Use the p95 for conservative scheduling, the mean for expected-makespan.
2. **Decompose** the DAG into chains of ≤ 16 nodes (where exact B&B is feasible) and a coarse skeleton connecting them.
3. **B&B** each chain: schedule the longest-CP-first list-schedule, then refine via depth-bounded search.
4. **Stitch** chains using a higher-level Jackson approximation: each chain is a station, throughput = 1 / E[CP-length].
5. **Re-solve every 5 s**: warm-start from previous solution, only revisit chains where a forecast deviated by > 20 %.

## Shadow mode

Critical for adoption. The scheduler runs alongside Airflow:

- Both see the same DAG arrivals.
- Both produce a dispatch order.
- Only Airflow actually dispatches.
- Our scheduler logs what *it* would have done.
- Periodically, replay the actual job runtimes on our schedule (offline simulation) → counterfactual makespan.
- Report **regret = actual makespan (Airflow) − counterfactual makespan (us)**.

If regret < 0 consistently across workloads, ship it.

## Hard parts

1. **Sub-100 ms re-solve.** Forecasts and B&B together must stay under 100 ms or scheduler latency dominates. Decomposition + warm-start are essential.
2. **Forecast drift.** Job code changes mean past distributions stop predicting future runtimes. Detect with online CUSUM; reset distribution.
3. **Cluster contention model.** Real K8s adds delays from image pulls, node taints, autoscaling. Model these as additional stations in the Jackson network with measured service rates.

## References

- Pinedo, *Scheduling: Theory, Algorithms, and Systems* (5th ed., 2016)
- Jackson, "Networks of Waiting Lines" (Operations Research 1957)
- Borkar et al., "Apollo: Scalable and Coordinated Scheduling for Cloud-Scale Computing" (OSDI 2014)
- Verma et al., "Large-scale cluster management at Google with Borg" (EuroSys 2015)

## Roadmap

- [ ] Per-task duration forecaster (lognormal w/ online MLE)
- [ ] Jackson-network steady-state solver
- [ ] Critical-path list scheduler
- [ ] B&B for subgraphs ≤ 16
- [ ] DAG decomposition into chains
- [ ] K8s dispatcher
- [ ] Airflow Executor plugin
- [ ] Shadow-mode + regret measurement
- [ ] CUSUM drift detection on forecasts
