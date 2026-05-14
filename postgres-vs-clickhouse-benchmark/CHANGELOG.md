# Changelog

## [0.1.0] — 2026-05-13

### Added
- `Query` + `Workload` (validated id/name/SQL, duplicate-id rejection).
- `TPCH_QUERIES` — 10 representative TPC-H queries (Q1, Q3, Q4, Q5,
  Q6, Q10, Q11, Q12, Q14, Q19).
- `NY_TAXI_QUERIES` — 5 typical NY-taxi queries.
- `Engine` ABC + `EngineError`.
- `SQLiteEngine` — stdlib reference engine.
- `InjectableEngine` — wraps any `(sql) → rows` callable; production
  callers plug in psycopg2 / clickhouse-driver / DuckDB.
- `LatencyStats` + `summarise` (nearest-rank p50/p95/p99 percentiles
  with construction-time validation).
- `BenchmarkRunner.run()` with warmup, repeat, trim parameters and
  injectable clock for deterministic tests.
- `build_comparison(results, baseline)` → `ComparisonReport`
  with `winners()` per query and `speedup_vs_baseline`.
- CLI `pvcctl info | list-queries | demo`.
- 35 tests including 1 Hypothesis property (p50 ≤ p95 ≤ p99 ≤ max).
- mypy `--strict` clean over 13 source files; ruff clean.
- Multi-stage slim Docker image, non-root `pvc` user; GHA matrix.
- Zero runtime dependencies.
