# IVM Engine — Incremental View Maintenance

A from-scratch implementation of **Incremental View Maintenance (IVM)** — the core idea behind [Materialize](https://materialize.com/) and [Differential Dataflow](https://github.com/TimelyDataflow/differential-dataflow).

Instead of recomputing a SQL view from scratch every time new data arrives, the engine computes only the **delta** (the minimal change to the output) and applies it to the materialized result. When a previously emitted result must be corrected, the engine handles **retractions** — negative-multiplicity updates — that propagate through the entire pipeline automatically.

---

## What's implemented

| Operator | Incremental? | Retractions? |
|---|---|---|
| **Filter** | ✅ | ✅ |
| **Project** (column select / transform) | ✅ | ✅ |
| **GROUP BY** (SUM, COUNT, AVG, MIN, MAX, COUNT DISTINCT) | ✅ | ✅ |
| **Tumbling window** (time-based, non-overlapping) | ✅ | ✅ |
| **Sliding window** (time-based, overlapping) | ✅ | ✅ |
| **Partition window** (ROW_NUMBER, RANK, LAG, LEAD) | ✅ | ✅ |
| **INNER JOIN** (multi-table, hash join) | ✅ | ✅ |
| **LEFT JOIN** (with unmatched-row tracking) | ✅ | ✅ |
| Operator composition (multi-hop pipelines) | ✅ | ✅ |

---

## Core concept: the Update triple

Every piece of data is an `(record, timestamp, diff)` triple:

```
diff = +1   →  this record was inserted
diff = -1   →  this record was retracted (corrected)
```

This follows the differential dataflow model.  The key invariant: the **sum of diffs for any record** equals its current multiplicity in the collection (0 = absent, 1 = present once, etc.).

---

## Quick start

```python
from ivm import IVMEngine
import ivm.aggregates as agg

engine = IVMEngine()
orders = engine.source("orders")

# Define a view as a composable dataflow pipeline
revenue = (
    orders
    .filter(lambda r: r["status"] == "completed")
    .group_by(
        key_columns=["category"],
        aggregates={
            "total_revenue": agg.Sum("amount"),
            "order_count":   agg.Count(),
            "avg_order":     agg.Avg("amount"),
        },
    )
)
engine.register_view("revenue_by_category", revenue)

# Ingest events — view is updated incrementally
engine.ingest("orders", {"id": 1, "category": "books", "amount": 25, "status": "completed"})
engine.ingest("orders", {"id": 2, "category": "books", "amount": 18, "status": "completed"})

# Query the current state (O(1) — no recomputation)
print(engine.query("revenue_by_category"))
# [{'category': 'books', 'total_revenue': 43, 'order_count': 2, 'avg_order': 21.5}]

# Retract a record (e.g. order cancellation)
engine.retract("orders", {"id": 2, "category": "books", "amount": 18, "status": "completed"})

print(engine.query("revenue_by_category"))
# [{'category': 'books', 'total_revenue': 25, 'order_count': 1, 'avg_order': 25.0}]
```

---

## Window functions

```python
from ivm import IVMEngine, TumblingWindow, SlidingWindow, PartitionWindow
import ivm.aggregates as agg

engine = IVMEngine()
events = engine.source("events")

# Tumbling 10-second windows
tumbling = events.window(
    TumblingWindow(size_ms=10_000),
    aggregates={"count": agg.Count(), "avg_latency": agg.Avg("latency_ms")},
)
engine.register_view("tumbling", tumbling)

# Sliding 30s window with 10s step
sliding = events.window(
    SlidingWindow(size_ms=30_000, step_ms=10_000),
    aggregates={"count": agg.Count()},
)
engine.register_view("sliding", sliding)

# ROW_NUMBER per user, ordered by score descending
ranked = events.window(
    PartitionWindow(partition_by=["user_id"], order_by=[("score", "desc")]),
    rank_fns={"rank": "row_number"},
)
engine.register_view("ranked", ranked)
```

---

## Multi-table joins

```python
orders   = engine.source("orders")
products = engine.source("products")

# INNER JOIN
enriched = orders.join(products, left_key="product_id", right_key="product_id")

# LEFT JOIN — orders without a matching product still appear
enriched = orders.join(products, left_key="product_id", right_key="product_id",
                       join_type="left")

# Compose join with GROUP BY
revenue = enriched.group_by(["category"], {"total": agg.Sum("amount")})
engine.register_view("revenue", revenue)
```

---

## Value corrections

An "UPDATE" in SQL is modelled as **retract old + insert new**:

```python
# Record was ingested with the wrong amount
engine.retract("orders", {"id": 42, "amount": 100, ...})
engine.ingest("orders",  {"id": 42, "amount": 150, ...})
# All downstream views self-correct automatically.
```

---

## Aggregates reference

| Class | Usage |
|---|---|
| `agg.Count()` | `COUNT(*)` |
| `agg.Sum("col")` | `SUM(col)` |
| `agg.Avg("col")` | `AVG(col)` |
| `agg.Min("col")` | `MIN(col)` — retraction-safe via value counter |
| `agg.Max("col")` | `MAX(col)` — retraction-safe via value counter |
| `agg.CountDistinct("col")` | `COUNT(DISTINCT col)` |

---

## Running examples

```bash
python examples/01_group_by.py
python examples/02_window_functions.py
python examples/03_joins.py
python examples/04_retractions.py
```

## Running tests

```bash
pip install pytest
pytest tests/ -v
```

---

## Architecture

```
IVMEngine
├── sources: {name → SourceOperator}   # input stream roots
└── views:   {name → ViewState}        # materialised output snapshots

Dataflow graph (DAG of Operators):

  SourceOperator
      │
  FilterOperator          ← drops records by predicate
      │
  ProjectOperator         ← reshapes columns
      │
  ┌───┴──────────────────┐
  │                      │
GroupByOperator      WindowOperator
  │                      │
  └──────────┬───────────┘
             │
         JoinOperator    ← receives updates from two input streams
             │
         GroupByOperator ← can layer additional aggregation

Each operator receives List[Update] and emits List[Update].
Updates flow forward; retractions (diff < 0) flow forward identically.
```

### Why MIN/MAX need value counters

Most aggregates (SUM, COUNT) track only a running total, which is trivially
updated by `state += diff * value`. MIN and MAX cannot work this way: if the
current minimum is retracted, we need to know the *next* smallest value.

The solution (used in real systems): maintain a `Counter[value → multiplicity]`.
`MIN = min(counter.keys())` after each update. This is O(log n) per update
using a sorted structure, O(n) here with Python's built-in `min`.

### ROW_NUMBER incremental maintenance

When a row is inserted at position `p` in a sorted partition, every row at
position ≥ p has its `ROW_NUMBER` incremented by 1. The engine retracts the
old ranked records and emits new ones. This is O(n) in the worst case —
production systems (Materialize) use hierarchical data structures to achieve
O(log² n).

---

## References

- [Differential Dataflow (Frank McSherry)](https://github.com/TimelyDataflow/differential-dataflow)
- [Materialize: Incremental View Maintenance](https://materialize.com/docs/overview/architecture/)
- [DBToaster: Higher-order Delta Processing](https://dbtoaster.github.io/)
- [Naiad: A Timely Dataflow System (SOSP 2013)](https://dl.acm.org/doi/10.1145/2517349.2522738)
