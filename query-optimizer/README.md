# Query Optimizer with Cost-Based Join Reordering

A pure-Python implementation of the **Cascades** query optimization framework featuring:

- **Memo-table memoization** — each logical sub-plan is costed exactly once
- **Histogram-based cardinality estimation** — column NDV, range predicates, equi-join selectivity
- **Three physical join algorithms** — Hash Join, Sort-Merge Join, Block Nested Loop
- **Cost model** — I/O pages + CPU work units, Grace hash spill penalty, external merge-sort passes
- **Subset-DP join enumeration** — all bushy trees over N relations (O(3ᴺ) states)
- **10-table star schema** demo with 10 million fact rows

---

## Architecture

```
query-optimizer/
├── main.py                     ← demo entry point
├── optimizer/
│   ├── expressions.py          ← logical & physical expression nodes
│   ├── histogram.py            ← ColumnStats, TableStats, StatsCatalog
│   ├── cost_model.py           ← CostModel (hash/merge/NL join + seq scan)
│   ├── memo.py                 ← Memo table, Group, Winner
│   ├── rules.py                ← transformation & implementation rules
│   ├── cascades.py             ← CascadesOptimizer (DP over subsets)
│   └── schema.py               ← 10-table star schema definition
└── tests/
    └── test_optimizer.py       ← 20 unit + integration tests
```

### Key Components

| Component | Description |
|-----------|-------------|
| `Memo` | Maps `frozenset[tables]` → `Group`; deduplicates equivalent sub-plans |
| `Group` | Equivalence class holding all logical/physical expressions for a sub-plan |
| `Winner` | Best `(PhysicalExpr, CostEstimate)` found for a group; recursive via `child_winners` |
| `CostEstimate` | `io_cost + cpu_cost`; I/O in pages, CPU weighted by `CPU_FACTOR` |
| `StatsCatalog` | Per-table `TableStats` with per-column `ColumnStats` (NDV, min/max, histogram) |

### Cascades Algorithm

```
for size in 2 .. N:
    for each subset S of size tables:
        for each binary split (S1, S2) of S:
            for each algorithm in {HashJoin, MergeJoin, NestedLoop}:
                cost = local_cost(algo, S1, S2) + winner(S1).cost + winner(S2).cost
                if cost < best so far: update winner(S)
```

### Cost Models

| Algorithm | Formula |
|-----------|---------|
| **Hash Join** | `pages(build) + pages(probe)` + Grace spill penalty when build > buffer pool |
| **Merge Join** | `sort_cost(left) + sort_cost(right) + pages(left) + pages(right)` |
| **Nested Loop** | `pages(outer) + (pages(outer)/B) × pages(inner)` |

Buffer pool = 1 024 pages (8 MB). Sort cost uses external merge-sort model.

### Cardinality Estimation

Join output rows = `left_rows × right_rows × ∏ sel(pᵢ)`

where selectivity of an equi-join predicate is `1 / max(NDV_left, NDV_right)`.

---

## Star Schema

```
fact_sales  (10 M rows)
    │
    ├── dim_customer   (500 K)
    ├── dim_product     (50 K)
    ├── dim_date         (3.6 K)
    ├── dim_store        (1 K)
    ├── dim_employee    (20 K)
    ├── dim_supplier     (5 K)
    ├── dim_region         (200)
    ├── dim_category       (500)
    └── dim_promotion    (2 K)
```

All joins are FK→PK equi-joins on surrogate integer keys.

---

## Quick Start

```bash
# Install test dependencies
pip install -r requirements.txt

# Run the optimizer demo
python main.py

# Run the test suite
pytest -v
```

### Example Output

```
========================================================================
  Cascades Cost-Based Query Optimizer – 10-Table Star Schema
========================================================================

Optimizing … done in ~400 ms  (57002 DP states explored)

────────────────────────────────────────────────────────────────────────
OPTIMAL JOIN ORDER
────────────────────────────────────────────────────────────────────────
dim_region ⋈ dim_category ⋈ dim_store ⋈ dim_date ⋈ dim_promotion ⋈ dim_supplier ⋈ dim_employee ⋈ dim_product ⋈ dim_customer ⋈ fact_sales

────────────────────────────────────────────────────────────────────────
PER-JOIN ALGORITHM SELECTION
────────────────────────────────────────────────────────────────────────
  Tables                                   Algorithm      Cost
  ['dim_region', 'fact_sales', ...]        HashJoin    ...
  ...
```

The optimizer prefers **Hash Join** for large build sides and **Merge Join** when inputs
are small enough that the sort cost amortises. Nested Loop appears only for very small
outer relations.

---

## Extending

- **Add a filter push-down rule**: subclass `Rule`, add to `TRANSFORMATION_RULES`
- **Add index scan**: create `IndexScan(PhysicalExpr)` and an `ImplementIndexScan` rule
- **Plug in real statistics**: replace `TableStats` entries in `StatsCatalog` with values from `pg_stats`
- **Required properties** (sort order, distribution): extend `Winner` to carry a `PhysicalProperties` key

---

## References

- Graefe, G. (1995). *The Cascades Framework for Query Optimization.* IEEE Data Eng. Bull.
- Selinger, P. et al. (1979). *Access Path Selection in a Relational Database Management System.* SIGMOD.
- Moerkotte, G. & Neumann, T. (2006). *Analysis of Two Existing and One New Dynamic Programming Algorithm.* VLDB.
