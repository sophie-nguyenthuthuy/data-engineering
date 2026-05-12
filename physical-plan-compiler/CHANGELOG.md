# Changelog

## [0.1.0] — Initial public release

### Added

- Typed IR (`types`, `schema`, `expr`, `logical`, `physical`)
- `sqlglot`-based SQL frontend supporting SELECT/JOIN/WHERE/GROUP BY/HAVING
  + scalar aggregates (COUNT, SUM, AVG, MIN, MAX)
- Cascades core: `Memo`, `Group`, `GroupExpression`, top-down memoized search
  with dominance pruning
- Transformation rules: `PredicatePushdownThroughJoin`, `JoinCommutativity`
- Implementation rules per engine (Spark, dbt, DuckDB, Flink)
- Calibrated cost model with engine memory caps + spill penalties
- 12 cross-engine conversion edges with realistic setup + per-byte costs
- Code generators: Spark (PySpark), dbt (SQL model), DuckDB (Python), Flink
  (SQL), Dagster (orchestration manifest)
- CLI: `ppc compile`, `ppc explain`
- TPC-H planner benchmark (Q1, Q3, Q6 at SF=1, 10, 100, 1000)
- 64 tests including Hypothesis property-based tests
- Mypy strict, ruff lint, GitHub Actions CI matrix (3.10/3.11/3.12)
- Dockerfile + docker-compose for reproducible local runs

### Limitations

- Single-table inner joins only; outer joins raise `SqlParseError`
- No subqueries or CTEs
- No bushy-tree join reordering (only commutativity)
- Cost model parameters are calibrated by community knowledge, not measured
- No window functions
