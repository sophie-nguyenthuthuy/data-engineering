# Learned Index Structures for Time-Series

Replaces B-tree and Bloom filter lookups in an LSM storage engine with a
**Recursive Model Index (RMI)** — a two-level learned approximation of the
empirical CDF of sorted keys.  Includes a distribution-drift detector that
automatically falls back to classic indexes when the key distribution shifts.

## Key ideas

| Component | Classic | Learned |
|---|---|---|
| Point lookup | B-tree O(log n) | RMI → short binary search |
| Membership | Bloom filter | Bloom filter (unchanged) |
| Drift | n/a | ADWIN + KS-test detectors |
| Fallback | always | on drift, then retrain |

### Recursive Model Index (RMI)

```
key ──► stage-1 linear model ──► model index m
                                        │
                                        ▼
                            stage-2[m] linear model
                                        │
                                        ▼
                              predicted_position ± error_bound
                                        │
                                        ▼
                              binary search in O(error_range)
```

Stage-1 learns a coarse mapping (key → which stage-2 model to use).  Each
stage-2 model learns a fine-grained mapping (key → array position) within its
partition.  Prediction errors are bounded per model; the final lookup is a
binary search over a typically tiny range.

### Drift detection

- **ADWIN** (Bifet & Gavalda, 2007) — O(log n) sliding window that detects
  mean shifts in the stream of RMI prediction errors.
- **KS-window** — two-sample Kolmogorov-Smirnov test over a reference and a
  recent error window, sensitive to shape changes as well as mean shifts.

When drift is detected the `AdaptiveIndexManager` switches to the B-tree
fallback and attempts to retrain the RMI once errors stabilise.

## Project layout

```
src/lsm_learned/
  indexes/
    rmi.py          — Recursive Model Index
    bloom.py        — Bloom filter (double-hashing)
    btree.py        — B-tree index (SortedList)
  lsm/
    memtable.py     — In-memory write buffer
    sstable.py      — Immutable binary SSTable with mmap
    engine.py       — LSM-tree orchestration + compaction
  drift/
    detector.py     — ADWIN + KS-window drift detectors
  adaptive/
    index_manager.py — Adaptive RMI ↔ B-tree switching

benchmarks/
  workload.py       — Uniform, Zipfian, time-series generators
  runner.py         — Benchmark harness (prints table + JSON)
  plot.py           — Matplotlib result plots

tests/
  test_rmi.py
  test_bloom.py
  test_drift.py
  test_lsm.py
  test_adaptive.py
```

## Quick start

```bash
pip install -e ".[dev]"
pytest -q                          # run tests
python -m benchmarks.runner        # run full benchmark suite
python -m benchmarks.plot          # generate plots → plots/
```

## Benchmark results (example, 500k keys / 50k queries)

| Workload | Index | Build(ms) | Mean(ns) | P99(ns) | Mem(KB) |
|---|---|---|---|---|---|
| uniform | RMI | ~45 | ~180 | ~600 | ~4000 |
| uniform | BTree | ~320 | ~350 | ~900 | ~13700 |
| zipfian_α1.2 | RMI | ~47 | ~185 | ~620 | ~4000 |
| time_series | RMI | ~43 | ~165 | ~580 | ~4000 |

RMI reduces mean lookup latency ~40–50% vs. B-tree on skewed distributions
because the learned model narrows the binary search range from O(log n) ≈ 19
to typically 5–30 entries.

## References

- Kraska, T. et al. (2018). *The Case for Learned Index Structures*. SIGMOD.
- Bifet, A. & Gavalda, R. (2007). *Learning from Time-Changing Data with
  Adaptive Windowing*. SDM.
- Kirsch, A. & Mitzenmacher, M. (2008). *Less Hashing, Same Performance*.
  ESA.
