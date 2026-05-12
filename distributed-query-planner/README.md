# Distributed Query Planner

A federated query optimizer that rewrites query plans per-engine, pushing WHERE predicates natively into MongoDB, Parquet (via PyArrow), and PostgreSQL — selecting the cheapest configuration using a calibrated cost model.

## Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                    FederatedOptimizer                       │
│   optimize(table, predicates, columns) -> PushedScanNode    │
│   optimize_join(left, right, join_pred, ...) -> PlanNode    │
└───────────────────┬─────────────────────────────────────────┘
                    │ uses
        ┌───────────┴───────────┐
        ▼                       ▼
  ┌──────────┐           ┌────────────┐
  │ Catalog  │           │ CostModel  │
  │          │           │            │
  │ TableSchema          │ StatsRegistry
  │ ColumnSchema         │ TableStats │
  └──────────┘           │ Histogram  │
                         └────────────┘
                                │ feeds
              ┌─────────────────┼──────────────────┐
              ▼                 ▼                  ▼
     ┌──────────────┐  ┌──────────────┐  ┌──────────────┐
     │ MongoDBEngine│  │ ParquetEngine│  │ PostgresEngine│
     │              │  │              │  │              │
     │ $match dict  │  │ ds.field()   │  │ SQL fragment │
     │ $aggregate   │  │ PyArrow expr │  │ SELECT ...   │
     └──────────────┘  └──────────────┘  └──────────────┘
              │                 │                  │
              └─────────────────┴──────────────────┘
                                │
                         Predicate IR
                    ┌────────────────────┐
                    │  ComparisonPred    │
                    │  InPredicate       │
                    │  BetweenPredicate  │
                    │  LikePredicate     │
                    │  IsNullPredicate   │
                    │  AndPredicate      │
                    │  OrPredicate       │
                    │  NotPredicate      │
                    └────────────────────┘
```

## Installation

```bash
pip install -e ".[dev]"
```

For engine-specific drivers (install only what you need):

```bash
pip install pyarrow>=14.0        # Parquet support
pip install pymongo>=4.6         # MongoDB support
pip install psycopg2-binary>=2.9 # PostgreSQL support
```

The library gracefully raises `ImportError` only when an engine is actually used, so you can install only the drivers you need.

## Quick Start

```python
from dqp import (
    Catalog, ColumnSchema, TableSchema,
    FederatedOptimizer, CostModel, StatsRegistry,
    ColumnStats, TableStats,
    ColumnRef, Literal, ComparisonOp,
    ComparisonPredicate, BetweenPredicate, InPredicate,
)
from dqp.engines.postgres_engine import PostgresEngine

# 1. Build the catalog
catalog = Catalog()
catalog.register_table(TableSchema(
    name="users",
    engine_name="postgres",
    columns=[
        ColumnSchema("user_id", "int", nullable=False, primary_key=True),
        ColumnSchema("age", "int", nullable=True),
        ColumnSchema("status", "str", nullable=True),
    ],
))

# 2. Register statistics (or use StatsBuilder to sample them)
registry = StatsRegistry()
registry.set_table_stats(TableStats(
    table_name="users",
    row_count=500_000,
    column_stats={
        "age": ColumnStats("age", 0.02, 80, 18.0, 99.0, None),
        "status": ColumnStats("status", 0.0, 4, None, None, None),
    },
))

# 3. Configure engines
engines = {"postgres": PostgresEngine(conn_string="postgresql://...")}

# 4. Build optimizer
optimizer = FederatedOptimizer(
    catalog=catalog,
    cost_model=CostModel(registry),
    engines=engines,
)

# 5. Build predicates
age_col = ColumnRef(column="age")
status_col = ColumnRef(column="status")
pred = (
    BetweenPredicate(age_col, Literal(25, "int"), Literal(50, "int"))
    & InPredicate(status_col, [Literal("active", "str"), Literal("trial", "str")])
)

# 6. Optimize and explain
plan = optimizer.optimize("users", [pred], columns=["user_id", "age", "status"])
print(optimizer.explain(plan))
```

### Output

```
=== Query Plan ===
PushedScanNode(table='users', engine='postgres', pushed=2, residual=0, columns=['user_id', 'age', 'status'])

=== Cost Breakdown ===
PushedScan: 'users' via 'postgres'
  Pushed predicates (2):
    (age BETWEEN 25 AND 50)
    (status IN ('active', 'trial'))
  Residual predicates (0):
  Estimated cost: PlanCost(cpu=60000.00, io=87500.00, rows=43750, total=147500.00)
```

## Predicate IR

All predicates are composable using Python operators:

```python
from dqp import ColumnRef, Literal, ComparisonOp, ComparisonPredicate

col = ColumnRef(column="age")
val = Literal(18, "int")

p1 = ComparisonPredicate(col, ComparisonOp.GT, val)  # age > 18
p2 = ComparisonPredicate(col, ComparisonOp.LT, Literal(65, "int"))

combined = p1 & p2   # AND
either   = p1 | p2   # OR
not_p1   = ~p1       # NOT

from dqp import negate, conjuncts, columns_referenced

conjuncts(p1 & p2 & combined)      # → [p1, p2, p1, p2] (flattened)
columns_referenced(combined)        # → {ColumnRef(column='age')}
negate(p1 & p2)                    # De Morgan → (NOT p1) OR (NOT p2)
```

## Engine Capabilities

| Predicate Type | MongoDB | Parquet | PostgreSQL |
|---------------|---------|---------|------------|
| COMPARISON    | Yes     | Yes     | Yes        |
| IN            | Yes     | Yes     | Yes        |
| BETWEEN       | Yes     | Yes     | Yes        |
| LIKE          | Yes (regex) | No  | Yes        |
| IS NULL       | Yes     | Yes     | Yes        |
| AND           | Yes     | Yes     | Yes        |
| OR            | Yes     | No*     | Yes        |
| NOT           | Yes     | No*     | Yes        |

*Parquet OR/NOT support is excluded to keep row-group skipping conservative.

## Cost Model

The cost model estimates selectivity per predicate type:

| Predicate | Selectivity |
|-----------|-------------|
| EQ        | 1 / NDV (or 0.05 fallback) |
| Range     | Histogram interpolation or (val-min)/(max-min) |
| BETWEEN   | Histogram range fraction |
| IN(k)     | min(k/NDV, 0.5) |
| LIKE prefix | 0.1 |
| LIKE wildcard | 0.3 |
| IS NULL   | null_fraction |
| AND       | product of selectivities |
| OR        | 1 - product(1 - sel_i) |

IO costs per row (relative units): Parquet = 0.5, MongoDB = 1.0, PostgreSQL = 1.0.

## Stats Collection

Use `StatsBuilder` to calibrate the cost model from live data:

```python
from dqp.cost.sampler import PostgresSampler, StatsBuilder, SamplingConfig
from dqp.cost.statistics import StatsRegistry

registry = StatsRegistry()
sampler = PostgresSampler(conn_string="postgresql://...")
builder = StatsBuilder(
    sampler=sampler,
    registry=registry,
    config=SamplingConfig(sample_fraction=0.01, min_sample_rows=1000),
)
stats = builder.build_stats("users", columns=["age", "status", "created_at"])
```

## Running Tests

```bash
# All tests
pytest

# With coverage
pytest --cov=dqp --cov-report=term-missing

# Specific module
pytest tests/test_predicate.py -v
pytest tests/test_engines/test_postgres.py -v
```

## Running Examples

```bash
# Basic predicate translation
python examples/basic_query.py

# Full federated optimizer demo
python examples/federated_query.py
```

## API Reference

### `FederatedOptimizer`

```python
optimizer.optimize(
    table_name: str,
    predicates: List[Predicate],
    columns: List[str],
) -> PushedScanNode
```

Returns the lowest-cost `PushedScanNode`. Tries all subsets of conjuncts (up to 10 predicates) to find the optimal pushed/residual split.

```python
optimizer.optimize_join(
    left_table: str,
    right_table: str,
    join_pred: Predicate,
    filter_preds: List[Predicate],
    columns: List[str],
) -> PlanNode
```

Pushes per-table filters before joining. Returns a `JoinNode` with `PushedScanNode` children.

```python
optimizer.explain(plan_node: PlanNode) -> str
```

Returns a human-readable plan tree with cost estimates.

### `CostModel`

```python
model.cost_scan(table_name, engine_name) -> PlanCost
model.cost_filter(scan_cost, predicate, table_stats) -> PlanCost
model.cost_pushed_scan(table_name, engine_name, pushed, residual, table_stats) -> PlanCost
model.cost_join(left, right, condition) -> PlanCost
```

### Engine translation

```python
engine.translate_predicate(pred: Predicate) -> Any   # engine-specific
engine.pushdown_predicates(preds: List[Predicate]) -> PushdownResult
engine.can_push(pred: Predicate) -> bool
```
