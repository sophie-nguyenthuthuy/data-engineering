# ivm-nested-aggregates

**Incremental View Maintenance for hard cases:** nested subqueries, window functions, correlated aggregates — the queries where naive delta propagation either produces incorrect results or triggers cascading recomputation. Implements the SIGMOD 2023 "Optimizing Incremental Queries" result: per-window, automatically pick between delta propagation and full recompute.

> **Status:** Design / spec phase. Extends [`ivm-engine`](../ivm-engine/) (which handles flat differential dataflow) into the hard SQL cases.

## Why

Differential dataflow handles `SELECT ... GROUP BY ... JOIN ...` cleanly. But IVM literature has long struggled with:

1. **Window functions** (`ROW_NUMBER`, `LAG`, `SUM() OVER`) — a single new row can change many output rows.
2. **Correlated subqueries** (`SELECT … WHERE x > (SELECT AVG(y) FROM …)`) — the inner aggregate's delta propagates differently for each outer row.
3. **Nested aggregates** (`SELECT MAX(SUM(...))`) — collapse / re-fan-out under delta.

For some shapes, computing the delta is asymptotically *more* expensive than just recomputing the view. This project detects those cases and switches strategies per window.

## Architecture

```
SQL → Logical plan → IVM analyzer ──┬─▶ Delta plan compiler
                                     │
                                     ├─▶ Cost estimator
                                     │       │
                                     │       └─▶ Per-window strategy decision:
                                     │             {delta, full_recompute, hybrid}
                                     │
                                     └─▶ Operator library:
                                           - Δ-aggregate
                                           - Δ-window-function (boundary tracking)
                                           - Δ-correlated (lift to materialized join)
```

## Components

| Module | Role |
|---|---|
| `src/analyzer/` | Walks plan, flags operators with non-trivial delta semantics |
| `src/operators/window.py` | Δ-window: tracks partition + frame boundary as auxiliary state |
| `src/operators/correlated.py` | Rewrites correlated subqueries to lateral joins on materialized inner |
| `src/operators/nested_agg.py` | Two-level aggregate with cascading delta |
| `src/cost/` | Estimator for delta cost vs. recompute cost per window |
| `src/strategy/` | Switch policy + hysteresis (avoid thrashing) |
| `src/eval/tpch_streaming/` | Streaming TPC-H queries with window & subquery shapes |

## Strategy decision

For each query operator at each maintenance window:

```
delta_cost ≈ |Δinput| × (per-tuple delta work) + state read/write
full_cost  ≈ |full input| × (per-tuple full work)

switch to full_recompute if delta_cost > α × full_cost   (α ≈ 0.5)
switch back to delta     if delta_cost < β × full_cost   (β ≈ 0.3)
```

`α > β` is the hysteresis — prevents oscillation when costs are close.

## Hard cases

### 1. Window functions

`ROW_NUMBER() OVER (PARTITION BY user ORDER BY ts)` — inserting a row with median timestamp shifts ranks of all rows after it in the partition.

Solution: maintain partition state as an order-statistic tree (B-tree augmented with subtree sizes). New row → `rank = order_statistic_rank(ts)` → emit delta for affected suffix.

### 2. Correlated subqueries

`SELECT * FROM orders o WHERE o.amount > (SELECT AVG(amount) FROM orders WHERE customer = o.customer)`

Rewrite to:
```sql
WITH per_customer_avg AS (SELECT customer, AVG(amount) avg FROM orders GROUP BY customer)
SELECT o.* FROM orders o JOIN per_customer_avg c ON o.customer = c.customer WHERE o.amount > c.avg
```

The CTE is a flat IVM-friendly shape. The original correlated form is provably equivalent.

### 3. Nested aggregates

`SELECT MAX(daily_total) FROM (SELECT date, SUM(amount) daily_total FROM tx GROUP BY date)`

Δ at the inner `SUM` is a {date, +Δsum} pair. The outer `MAX` only needs recomputation if Δsum increases `daily_total` past current MAX or decreases the row that *is* current MAX. Otherwise: no-op delta. Track which inner row currently holds the MAX as auxiliary state.

## Benchmarks

Compare against:
- **DBToaster** (academic IVM compiler, gold-standard for flat queries; weak on the cases above)
- **Materialize** (production differential dataflow)
- **PostgreSQL refresh materialized view** (full recompute baseline)

Targets: at least 5× speedup over PG full-refresh for window queries; within 2× of Materialize on shared flat queries; **correct results** on all cases (validated against PG ground truth).

## References

- Koch, "Incremental Query Evaluation in a Ring of Databases" (PODS 2010)
- DBToaster: Ahmad et al. (VLDB 2012)
- Materialize / Differential Dataflow: McSherry et al. (CIDR 2013, 2018)
- SIGMOD 2023, "Optimizing Incremental Queries"

## Roadmap

- [ ] Operator library: Δ-window, Δ-correlated, Δ-nested-agg
- [ ] Cost estimator
- [ ] Strategy switch with hysteresis
- [ ] Correctness oracle (validate against PG ground truth)
- [ ] Benchmark vs. DBToaster + Materialize + PG
