# physical-plan-compiler

[![CI](https://github.com/sophie-nguyenthuthuy/data-engineering/actions/workflows/ppc.yml/badge.svg)](https://github.com/sophie-nguyenthuthuy/data-engineering/actions)

A **Cascades-style cost-based query optimizer** that compiles SQL into an
executable physical plan on one of four engines — Spark, dbt, DuckDB, or
Flink — selecting the engine **based on calibrated cost**, with explicit
cross-engine conversion operators and a learned-friendly cost-model
interface.

```text
SQL → sqlglot → Logical IR → Cascades search → Physical Plan → Codegen (Spark / dbt / DuckDB / Flink / Dagster)
                                  │
                                  ▼
                          Memo · Groups · Rules · Properties · Calibrated cost
```

64 tests pass; sub-millisecond plan times on TPC-H queries; correctly routes
small workloads to DuckDB and large workloads to Spark / dbt.

## Install

```bash
pip install -e .
# or with dev tools
pip install -e ".[dev]"
```

## Quick start

```bash
# Explain
ppc explain --sql examples/q1.sql --catalog examples/tpch_catalog.yaml

# Compile to engine artefact
ppc compile --sql examples/q1.sql --catalog examples/tpch_catalog.yaml --emit duckdb
ppc compile --sql examples/q3.sql --catalog examples/tpch_catalog.yaml --emit spark
ppc compile --sql examples/q3.sql --catalog examples/tpch_catalog.yaml --emit dagster -o pipeline.yml
```

Python API:

```python
from ppc import compile_sql
from ppc.frontend.catalog import Catalog
from ppc.ir.schema import Column, Schema, Stats
from ppc.ir.types import INT64, DOUBLE, STRING

cat = Catalog()
cat.register("orders", Schema.of(
    Column("o_orderkey",   INT64,  Stats(ndv=1_500_000)),
    Column("o_totalprice", DOUBLE),
    Column("o_status",     STRING, Stats(ndv=3)),
    rows=1_500_000,
))

plan = compile_sql(
    "SELECT o_status, SUM(o_totalprice) FROM orders WHERE o_totalprice > 100 GROUP BY o_status",
    cat,
)
print(plan.explain())
print(f"engine={plan.root.engine}  cost={plan.total_cost:.2f}")
```

## Architecture

### IR (`src/ppc/ir/`)

| Module | Contents |
|---|---|
| `types.py` | `DataType` singletons + `promote(a, b)` |
| `schema.py` | `Column`, `Schema`, `Stats` (ndv, nulls, avg_len) |
| `expr.py` | `Expr`, `ColumnRef`, `Literal`, `BinaryOp`, `UnaryOp` with operator overloads |
| `logical.py` | `LogicalScan`, `LogicalFilter`, `LogicalAggregate`, `LogicalJoin` with row-estimate propagation |
| `physical.py` | `PhysicalNode`, `PhysicalProperties`, `PhysicalPlan` |

### Frontend (`src/ppc/frontend/`)

`sqlglot`-based SQL → Logical IR. Supports `SELECT`, `WHERE`, `INNER JOIN`,
`GROUP BY`, `HAVING`, scalar aggregates (`COUNT/SUM/AVG/MIN/MAX`), arithmetic,
comparisons, AND/OR/NOT, parenthesised sub-expressions. Outer joins, window
functions, sub-queries, and `UNION` raise a clean `SqlParseError`.

### Cascades core (`src/ppc/cascades/`)

| Module | Role |
|---|---|
| `memo.py` | `Memo`, `Group`, `GroupExpression` — dedup'd DAG of equivalent exprs |
| `rules.py` | `TransformationRule` (logical→logical), `ImplementationRule` (logical→physical) |
| `optimizer.py` | Top-down memoized search with dominance pruning |
| `properties.py` | `PhysicalProperties` (engine, partitioning, sort_order) + `satisfies` |

Implemented rules:

- **PredicatePushdownThroughJoin** — push `Filter(Join(L,R), p)` into the
  side where `p`'s columns live.
- **JoinCommutativity** — `Join(A, B)` ↔ `Join(B, A)`.
- **ScanImpl / FilterImpl / AggregateImpl / HashJoinImpl** per engine.

### Engines (`src/ppc/engines/`)

| Engine | setup | per_byte_scan | memory cap | spill ×    |
|--------|------:|--------------:|-----------:|-----------:|
| spark  |  30.0 |       1.0e-9  |    200 GB  |       1.5  |
| dbt    |  60.0 |       0.5e-9  |      1 TB  |       1.2  |
| duckdb |   1.0 |       0.8e-9  |      8 GB  |    **20.0**|
| flink  |  60.0 |       1.5e-9  |    100 GB  |       1.5  |

These knobs are calibrated against community-known performance properties.
They live in `engines/base.py:ENGINE_PROFILES` — swap them in for your
cluster's measured numbers.

### Cross-engine conversions (`engines/conversions.py`)

12 directed edges modelling realistic transfer paths:
`spark → dbt` (S3 + external table), `duckdb → spark` (export parquet), etc.
Each carries `(setup, per_byte)`. The optimizer inserts a `PhysicalConversion`
op whenever a parent op's engine differs from its child's delivered engine.

### Codegen (`src/ppc/codegen/`)

| Emitter | Output |
|---|---|
| `emit_duckdb` | DuckDB Python script with embedded SQL |
| `emit_spark`  | PySpark DataFrame code |
| `emit_dbt`    | dbt model `.sql` with `{{ config() }}` |
| `emit_flink`  | Flink SQL `INSERT INTO ... SELECT ...` |
| `emit_dagster`| Orchestration manifest (one asset per engine region) |

## Cost model

Each `PhysicalNode.cost` derives its number from the engine profile:

```
cost = setup + bytes_in * per_byte * spill_penalty
```

The optimizer's total cost is the sum across the plan tree plus
conversion costs.

**Selectivity** for filter row-counts (`engines/physical_ops.py:estimate_selectivity`):
- `col = literal` → `1 / ndv(col)`
- `col {<,<=,>,>=} literal` → 0.33
- `col != literal` → `1 - 1/ndv(col)`
- AND, OR combine independently

To plug in a learned cost model, subclass `CalibratedCostModel` in
`src/ppc/cost/` and pass it to the `Optimizer`.

## Benchmarks

```bash
python -m benchmarks.tpch_planner_bench
```

Sample output:

```
Query   Scale Engine             Cost       Rows   Plan (µs)
----------------------------------------------------------------------
Q1          1 duckdb             4.37          6       374.6
Q1         10 duckdb            16.73          6       348.0
Q1        100 spark            277.20          6       348.6
Q1       1000 dbt            1,178.40          6       344.9
Q3          1 duckdb            13.05  1,500,000       856.9
Q3         10 spark            288.94 15,000,000       883.2
Q3        100 dbt              783.24 150,000,000       842.8
Q3       1000 dbt            4,348.08 1,500,000,000       846.1
Q6          1 duckdb             4.37          1       321.2
Q6        100 spark            277.20          1       315.9
Q6       1000 dbt            1,178.40          1       318.5
```

The compiler routes small workloads to DuckDB, medium workloads to Spark,
huge workloads to dbt (warehouse) — exactly as a human DBA would. Plan
times are sub-millisecond across all scales.

## Development

```bash
pip install -e ".[dev]"
make test         # pytest (64 tests)
make lint         # ruff
make typecheck    # mypy strict
make bench        # TPC-H benchmark
docker compose run --rm ppc make test    # in container
```

## Project status

**v0.1** — working core. The next milestones:

- [ ] Join reorder transformation (currently only commutativity)
- [ ] Property-driven enforcers (Sort, Exchange)
- [ ] Histogram-aware selectivity estimator
- [ ] Bushy join planner via dynamic programming for ≤12-way joins
- [ ] Learned cost-model adapter

## License

MIT. See `LICENSE`.

## References

- Graefe, "The Cascades Framework for Query Optimization" (Data Eng. Bulletin 1995)
- Apache Calcite — open-source Cascades planner
- Marcus et al., "Bao: Making Learned Query Optimization Practical" (SIGMOD 2021)
