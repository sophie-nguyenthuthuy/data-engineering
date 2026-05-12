# b-epsilon-tree

A **write-optimized B^ε-tree** implementation. Internal nodes reserve a fraction `ε` of their space for an **update buffer**, so writes amortise into batched flushes down the tree — yielding provably better write I/O complexity than B+-trees. A cost model tunes ε online based on observed read/write ratio.

> **Status:** Design / spec phase.

## Why B^ε-trees

B+-tree write complexity is `O(log_B N)` random I/Os per insert. B^ε-trees achieve `O((log_B N) / B^(1-ε))` amortised I/Os — for write-heavy workloads, the difference is 10–100×.

The trade-off is `ε`: tuning ε close to 1 → B+-tree behaviour (read-fast, write-slow); ε close to 0 → all buffer (write-fast, read-slow). The right ε depends on workload.

## Architecture

```
            ┌──────────────────────────────────────────────┐
            │ Root: pivots (1-ε of space) + buffer (ε)     │
            └─────────────┬────────────────────────────────┘
                          │ flush when buffer full
            ┌─────────────▼────────────────────────────────┐
            │ Internal: pivots + buffer                    │
            └─────────────┬────────────────────────────────┘
                          │
            ┌─────────────▼────────────────────────────────┐
            │ Leaf: sorted key→value, no buffer            │
            └──────────────────────────────────────────────┘

Insert path:
  1. Drop message {op, key, value} into root buffer
  2. If root buffer >= ε*B → flush B/(log N) largest messages to children
  3. Recurse

Read path:
  1. Walk tree top→bottom
  2. At each level, check buffer for pending messages on the search key
  3. Merge pending message with leaf value (newer wins)
```

## Components

| Module | Role |
|---|---|
| `src/tree/node.py` | Internal & leaf node layouts with split buffer |
| `src/tree/flush.py` | Message flush down-tree, with batched I/O |
| `src/tree/read.py` | Top-down read with buffer-merge |
| `src/cost_model/` | Online estimator: tracks read/write ratio, recomputes optimal ε |
| `src/tuner/` | Background re-tuner: rewrites nodes with new ε when workload shifts |
| `src/io/page_cache.py` | Pinned-page I/O abstraction (testable in-memory or file-backed) |

## Cost model

Given observed read fraction `r` and write fraction `w` in a sliding window:

```
expected_cost(ε) = r * read_cost(ε) + w * write_cost(ε)
read_cost(ε) ∝ log_B(N) / (1-ε)             // wider buffer → fewer pivots → more levels
write_cost(ε) ∝ log_B(N) / B^(1-ε)          // wider buffer → more amortisation
```

Solve for ε minimising expected_cost; re-tune when |Δε| > 0.1 with hysteresis.

## Benchmarks (targets)

Workload: SOSD 1B-key uniform integer, 16 GB working set, NVMe.

| Workload mix | B+-tree (RocksDB) | This B^ε-tree |
|---|---|---|
| 100% writes | 300 K op/s | ≥ 1.5 M op/s (~5×) |
| 50/50 r/w | 250 K op/s | ≥ 500 K op/s |
| 100% reads | 800 K op/s | ≥ 600 K op/s (1.3× slower acceptable) |
| Write-amplification | ~30× | ≤ 5× |

## Information-theoretic check

For pure write workload, the ELT lower bound for external-memory sorting is `O((N/B) log_(M/B)(N/B))` I/Os. B^ε-tree (with ε → 0) achieves this within a constant factor. Empirically verify: measure I/Os vs. theoretical bound across N = 10⁶ … 10⁹.

## References

- Brodal & Fagerberg, "Lower Bounds for External Memory Dictionaries" (SODA 2003)
- Bender et al., "An Introduction to B^ε-trees and Write-Optimization" (login Usenix Magazine 2015)
- BetrFS: "BetrFS: A Right-Optimized Write-Optimized File System" (FAST 2015)

## Roadmap

- [ ] Page-cached node layout (pivots + buffer)
- [ ] Insert path with cascading flushes
- [ ] Read path with buffer merge
- [ ] Crash-consistent flush (atomic page write or WAL)
- [ ] Cost model + online ε tuner
- [ ] Workload generator + benchmarks vs. RocksDB
- [ ] Information-theoretic validation
