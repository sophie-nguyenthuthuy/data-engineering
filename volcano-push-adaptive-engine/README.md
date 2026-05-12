# Volcano-to-Push Adaptive Query Engine

A query engine that begins execution in pull-based (Volcano/iterator) mode, profiles actual cardinality mid-query, and **dynamically switches hot paths to push-based pipelines**. Includes runtime re-optimization when cardinality estimates are off by more than 10×.

## Architecture

```
┌─────────────────────────────────────────────────────────┐
│                    AdaptiveEngine                        │
│                                                         │
│   Plan ──► Optimizer ──► VolcanoExecutor                │
│                              │                          │
│                         ProfilingIterator               │
│                         (count rows, measure time)      │
│                              │                          │
│               actual/estimated > 10× ?                  │
│                    ╱                  ╲                  │
│               yes ╱                    ╲ no             │
│                  ╱                      ╲               │
│        PushCompiler                  continue           │
│        (compile subtree              volcano            │
│         to pipeline,                                    │
│         materialise rows)                               │
│                  │                                      │
│           BufferNode ──► ReOptimizer                    │
│           (hot subtree   (swap join sides,              │
│            replaced)      merge filters,                │
│                           upgrade NLJ→HashJoin)         │
└─────────────────────────────────────────────────────────┘
```

### Execution modes

| Mode | Model | Best for |
|------|-------|----------|
| **Volcano** | Pull / iterator | Low-cardinality paths, complex tree shapes |
| **Push** | Pipeline / callback | High-cardinality hot paths, tight inner loops |

The engine starts every query in Volcano mode.  After every `check_interval` rows (default 100), the profiler compares actual vs estimated cardinality.  When `actual / estimated ≥ hot_threshold` (default 10×), the hot subtree is:

1. Compiled into a push pipeline and fully materialised into a `BufferNode`.
2. The remaining Volcano tree reads from that buffer.
3. The `ReOptimizer` rewrites the plan (join-side swap, filter merge, NLJ upgrade) before the next round.

---

## Components

| Module | Responsibility |
|--------|---------------|
| `expressions.py` | Expression / predicate DSL (`col`, `eq`, `gt`, `BinOp`, …) |
| `catalog.py` | In-memory table store + column statistics |
| `plan.py` | Logical plan node hierarchy (`ScanNode`, `FilterNode`, `HashJoinNode`, …) |
| `volcano.py` | Pull-based iterator executor |
| `push.py` | Push-based pipeline compiler + operator stages |
| `profiler.py` | Runtime cardinality instrumentation, `HotPathSignal` |
| `optimizer.py` | Initial cost-based annotation + `ReOptimizer` |
| `engine.py` | `AdaptiveEngine` — coordinates everything |

---

## Quick start

```python
from adaptive_engine import (
    AdaptiveEngine, Catalog,
    ScanNode, FilterNode, HashJoinNode, AggregateNode,
    gt,
)

catalog = Catalog()
catalog.create_table("orders", orders_data, estimated_rows=100)   # deliberate underestimate
catalog.create_table("products", products_data)

plan = AggregateNode(
    child=HashJoinNode(
        left=FilterNode(
            child=ScanNode(table="orders"),
            predicate=gt("amount", 50),
            selectivity=0.8,
        ),
        right=ScanNode(table="products"),
        left_key="product_id",
        right_key="product_id",
    ),
    group_by=["category"],
    aggregates=[("total", "sum", "amount"), ("cnt", "count", "order_id")],
)

engine = AdaptiveEngine(catalog, hot_threshold=10.0, check_interval=100)
rows, report = engine.execute(plan)

print(report)
# === ExecutionReport ===
#   rows=…  elapsed=…ms  reopt_rounds=1
#   Mode switches:
#     ModeSwitch(ScanNode_1: volcano→push after 100 rows, ratio=50.0x)
#   Re-optimizations:
#     • Swap HashJoin sides: …
```

---

## Installation

```bash
# From source
pip install -e ".[dev]"

# Run tests
pytest

# Run demo
python examples/demo.py
```

Requires **Python 3.11+**.  No external dependencies for the core engine.

---

## Re-optimization strategies

The `ReOptimizer` applies three rewrites when triggered:

1. **Join-side swap** — If the probe side turns out smaller than the build side, swap them so the smaller relation is hashed (reduces memory + probe cost).
2. **Filter merge** — Stacked `FilterNode`s are collapsed into a single `AndExpr` predicate, reducing iterator overhead.
3. **NLJ → HashJoin upgrade** — A `NestedLoopJoinNode` is upgraded to `HashJoinNode` when either input exceeds 1 000 rows, avoiding O(n²) cost.

---

## Running the demo

```
python examples/demo.py
```

Produces four scenarios:

| Demo | What it shows |
|------|---------------|
| 1 | Pure Volcano vs pure Push on a 10 k-row filter |
| 2 | Adaptive mode switch: estimated 50 rows, actual 10 000 |
| 3 | Runtime re-optimization on a 50 k-row join + sort |
| 4 | Small dataset stays in Volcano (no unnecessary switching) |

---

## License

MIT
