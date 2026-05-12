# ivm-nested-aggregates

[![CI](https://github.com/sophie-nguyenthuthuy/data-engineering/actions/workflows/ivm.yml/badge.svg)](https://github.com/sophie-nguyenthuthuy/data-engineering/actions)

Incremental view maintenance for **the hard SQL cases** that
differential-dataflow alone gets wrong or pays cascading recomputation:

- **Window functions** — `ROW_NUMBER`, `RANK`, `DENSE_RANK`, `LAG/LEAD`,
  `SUM/AVG OVER (sliding frame)`
- **Correlated subqueries** — `WHERE x > (SELECT AVG(x) FROM ... WHERE k = outer.k)`,
  via rewrite to lateral join
- **Nested aggregates** — `MAX(SUM(...))`, `SUM(MAX(...))` with held-max
  tracking
- **Strategy switching** — delta-propagation vs full recompute, picked
  per-window with hysteresis on a linear cost model

50 tests pass; ROW_NUMBER IVM is **32× faster** than full sort-recompute
on a 5k-event workload.

## Install

```bash
pip install -e ".[dev]"
```

## Quick start

```python
from ivm import RowNumberIVM, CorrelatedSubqueryIVM, MaxOfSum, StrategyController

# ROW_NUMBER OVER (PARTITION BY user ORDER BY ts)
rn = RowNumberIVM()
deltas = rn.insert("user1", t=10, row_id="c1")   # [("c1", 1)]
deltas = rn.insert("user1", t=30, row_id="c3")   # [("c3", 2)]
deltas = rn.insert("user1", t=20, row_id="c2")   # [("c2", 2), ("c3", 3)]  ← suffix only

# Correlated subquery: orders.amount > AVG(orders.amount per customer)
cq = CorrelatedSubqueryIVM()
cq.insert("c1", 100)
cq.insert("c1", 200)
print(cq.qualifying())              # [("c1", 200)]

# Nested aggregate: MAX(SUM(amount)) GROUP BY date
mos = MaxOfSum()
mos.insert("2024-01-01", 100)
mos.insert("2024-01-02", 50)
mos.insert("2024-01-01", 30)        # day1: 130
print(mos.max)                       # (130, "2024-01-01")

# Strategy switch: when delta cost exceeds α × full cost, recompute
sc = StrategyController(alpha=0.5, beta=0.3)
print(sc.decide(delta_size=10_000, state_size=1_000))   # "full"
print(sc.decide(delta_size=100,    state_size=1_000))   # "delta"
```

## CLI

```bash
ivmctl info
ivmctl bench delta-vs-full
```

## Architecture

### Window functions (`src/ivm/window/`)

| Module | What |
|---|---|
| `row_number.py`  | `ROW_NUMBER` with affected-suffix deltas (no need to re-rank rows before the insert) |
| `rank.py`        | `RANK`, `DENSE_RANK` — sort-key-based, tie-aware |
| `lag_lead.py`    | `LAG(k)`, `LEAD(k)` per partition |
| `sliding_sum.py` | `SUM/AVG OVER (ROWS BETWEEN n PRECEDING AND CURRENT)` with prefix-sum maintenance |

### Correlated subqueries (`src/ivm/correlated/`)

| Module | What |
|---|---|
| `per_key_agg.py` | `PerKeySum`, `PerKeyCount`, `PerKeyAvg`, `PerKeyMax`, `PerKeyMin` |
| `subquery.py`    | `CorrelatedSubqueryIVM` — rewrites `WHERE x > AVG(x)` style queries to a CTE + lateral join, with `qualifying()` membership tracking under insert/delete |

### Nested aggregates (`src/ivm/nested/`)

| Module | What |
|---|---|
| `max_of_sum.py` | `MAX(SUM(amount)) GROUP BY k` with held-max tracking |
| `sum_of_max.py` | `SUM(MAX(amount)) GROUP BY k` with per-key max-delta update |

### Strategy (`src/ivm/strategy/`)

| Module | What |
|---|---|
| `cost_model.py`  | Linear cost model: `delta_cost / full_cost` |
| `controller.py`  | Strategy switch with hysteresis (α > β) and bounded history |

## Algorithms

### ROW_NUMBER affected-suffix optimisation

For `ROW_NUMBER() OVER (PARTITION BY p ORDER BY t)`, inserting `(t, rid)`
at position `i` in the sorted partition shifts ranks of all rows at
positions `≥ i` by +1. Naïve recomputation is `O(n)`; we emit only the
affected suffix's `(rid, new_rank)` pairs — which is what a streaming
downstream actually needs.

### Correlated subquery rewrite

```sql
SELECT * FROM orders o
WHERE o.amount > (SELECT AVG(amount) FROM orders WHERE cust = o.cust)
```

→

```sql
WITH per_cust AS (
    SELECT cust, AVG(amount) AS avg_amt FROM orders GROUP BY cust
)
SELECT o.* FROM orders o
JOIN per_cust c ON o.cust = c.cust
WHERE o.amount > c.avg_amt
```

The CTE `per_cust` is flat-IVM-friendly. On each insert/delete, we
recompute that customer's AVG (`PerKeyAvg`, O(1)) and re-evaluate which
of that customer's rows now qualify (linear in the customer's rows).
Other customers are untouched.

### MAX(SUM) held-max tracking

For `MAX(SUM(amount)) GROUP BY date`:

- **Insert at non-MAX key**: O(1) — bump that key's sum; if it now beats
  current MAX, take over
- **Insert at MAX key**: O(1) — just bump the MAX value
- **Delete from MAX key**: O(K) — may need to scan all K keys to find the
  new MAX (rare in practice)

## Benchmarks

```
$ ivmctl bench delta-vs-full
op                n   delta (ms)    full (ms)  speedup
row_number     5000          3.7        119.1    31.91x
max_of_sum    10000          4.9          7.2     1.46x
sliding_sum    5000          5.3          1.6     0.30x
```

Honest readouts:

- **ROW_NUMBER**: 32× speedup. Full-recompute does `O(n log n)` sort per
  insert; the IVM does `O(log n)` bisect + `O(k)` for the affected suffix.
- **MAX(SUM)**: 1.5× speedup. With only 50 keys, `max(sums.values())`
  in CPython is so fast that IVM's bookkeeping adds visible overhead.
  IVM wins more decisively at high K (1000+ keys).
- **Sliding SUM**: 0.3× — the "full" path in the bench is `sum(slice)`
  which is pure-C. IVM's win shows up under **out-of-order inserts** or
  **deletes from the middle**, neither of which the simple benchmark
  exercises. The implementation is still O(1) for in-order appends.

## Correctness

**Hypothesis property tests** (60 random op sequences each) compare:

- `RowNumberIVM` against full sort of all inserted rows
- `MaxOfSum` against `max(naive_sums.values())`
- `CorrelatedSubqueryIVM` against full scan + per-key AVG recomputation

All pass on random workloads with mixed inserts and deletes.

## Strategy switcher

```python
from ivm import StrategyController

sc = StrategyController(alpha=0.5, beta=0.3)
# Small delta → stay incremental
print(sc.decide(delta_size=10, state_size=10_000))    # "delta"
# Huge delta → switch to full recompute
print(sc.decide(delta_size=10_000, state_size=1_000)) # "full"
# Back to small — hysteresis must be cleared
print(sc.decide(delta_size=100, state_size=1_000))    # "delta"
```

α > β prevents the controller from flapping when the cost ratio
hovers near the threshold.

## Limitations / roadmap

- [ ] **Window-function FRAME variants** — currently `ROWS BETWEEN n PRECEDING
      AND CURRENT`; need `RANGE` / `GROUPS` framing
- [ ] **First-value / last-value / nth-value** windowed functions
- [ ] **Order-statistic-tree backing for ROW_NUMBER** — pure Python list
      insert is O(n); a true BST would give O(log n)
- [ ] **Real SQL frontend** — currently programmatic API only; sqlglot
      integration would let users write SQL directly
- [ ] **MAX-of-MAX, SUM-of-SUM, etc** — N-level generalisation

## Development

```bash
make install
make test          # 50 tests
make lint
make typecheck
make bench
docker compose run --rm ivm make test
```

## References

- Koch, "Incremental Query Evaluation in a Ring of Databases" (PODS 2010)
- Ahmad et al., "DBToaster" (VLDB 2012)
- McSherry et al., "Differential Dataflow" (CIDR 2013, 2018)
- SIGMOD 2023, "Optimizing Incremental Queries"

## License

MIT.
