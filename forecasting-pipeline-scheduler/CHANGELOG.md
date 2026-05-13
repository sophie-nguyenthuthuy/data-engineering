# Changelog

All notable changes to **forecasting-pipeline-scheduler** are documented in this file.
Format: [Keep a Changelog](https://keepachangelog.com/en/1.1.0/).

## [0.1.0] — 2026-05-13

### Added

- **DAG primitives** (`fps.dag`)
  - Frozen `Task(id, duration, deps)` with construction-time validation
    (non-empty id, non-negative duration, no self-loop, no duplicate
    deps).
  - `DAG` with deterministic Kahn topological sort that raises
    `CycleError` on a cycle or unknown dependency, plus
    `critical_path_length()` returning (CP length, EFT per task).
- **Forecasting** (`fps.forecast`)
  - `LognormalForecaster` — per-task online MLE over log-durations;
    `mean`, `quantile(q)`, `p95` with default fallback when a task has
    no observations.
  - `TaskStats` exposes `(n, sum_log, sum_log_sq, mu, sigma)` plus
    `reset()`.
  - Internal Acklam inverse-Φ (verified against the standard 1.96 and
    -1.96 quantiles in the test suite).
  - `CUSUMDetector` — two-sided Page CUSUM with parameters `k`, `h`;
    `update()` returns whether drift has fired; `reset()` clears.
- **Schedulers** (`fps.scheduler`)
  - Common `ScheduledTask`, `Schedule`, `makespan`,
    `assert_valid_schedule` (completeness + dep-order +
    single-assignment per worker).
  - `list_schedule` — critical-path-first (b-level) priority with
    worker-best-fit placement.
  - `baseline_fcfs_schedule` — topological-order FCFS with
    least-loaded worker placement (shadow baseline).
  - `branch_and_bound` — exact small-DAG solver (`max_tasks=12`)
    with CP lower-bound pruning and `time_limit_ms` fallback.
- **Shadow-mode regret** (`fps.shadow`)
  - `regret(dag, num_workers)` → `RegretReport(baseline_makespan,
    our_makespan, regret, speedup)`.
  - `regret_over_dags(dags, num_workers)` → `AggregateRegret` with
    mean / median / p95 regret, mean speedup, and the
    `positive_fraction` of DAGs where we beat baseline.
- **Synthetic workloads** (`fps.bench`)
  - `random_layered_dag` — lognormal-duration layered DAG generator
    with configurable layers, layer width, and parent fan-in.
- **CLI** (`fpsctl`)
  - `info`, `bench`, `regret`, `forecast` subcommands.
- **Quality**
  - **64 pytest tests** including:
    - 2 Hypothesis property tests (`list_schedule` always produces a
      valid schedule; `makespan ≥ critical_path_length`).
    - Invariant tests for `assert_valid_schedule` rejecting missing
      tasks, dep-order violations, and worker overlaps.
    - Lognormal MLE convergence on 2 000 lognormal samples
      (`abs(μ̂ − μ)`, `abs(σ̂ − σ)` < 0.05).
    - CUSUM fires within 60 samples on ±2 σ drifts; does not fire on
      500 in-distribution samples.
  - mypy `--strict` clean over 13 source files.
  - Multi-stage slim Dockerfile, non-root `fps` user.
  - GitHub Actions matrix (Python 3.10 / 3.11 / 3.12) + Docker build
    smoke step.

### Notes

- **Zero runtime dependencies.** The package uses stdlib only — no
  numpy, no scipy. The lognormal forecaster reimplements the inverse
  normal CDF (Acklam) inline.
- `branch_and_bound` always returns a valid schedule: when the DAG is
  larger than `max_tasks` it returns the list-scheduler output; when
  the time limit expires mid-search it returns the best schedule
  found so far (initialised from `list_schedule` so worst-case = LS).
- `assert_valid_schedule` is the property-test oracle used to guard
  every scheduler's output across randomly generated layered DAGs.
- Hypothesis is configured for `max_examples=25` on the schedule
  invariant property; it shrinks any failing DAG to a minimal
  reproducer.
