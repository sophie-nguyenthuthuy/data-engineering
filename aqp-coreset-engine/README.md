# aqp-coreset-engine

Approximate Query Processing using **coreset theory**. For any aggregation (COUNT, SUM, AVG, quantiles), build a weighted coreset that guarantees `ε`-approximation with `(1−δ)` probability. Coresets construct online as data streams in, merge across partitions, and let the query interface return **confidence intervals** rather than point estimates.

> **Status:** Design / spec phase.

## Why coresets vs. uniform sampling

A uniform 1% sample answers `AVG(amount)` accurately for the bulk of data but is terrible for `AVG(amount) WHERE category = 'rare'` — the rare category may have zero samples. Coresets are *importance-weighted* samples specifically designed to bound error on entire **classes** of queries.

For sum-like aggregations under arbitrary predicates: ε-coresets of size `O((1/ε²) log(1/δ))` exist and can be constructed online.

## The guarantee

For any query `q ∈ Q` from a fixed class (e.g., counting queries with axis-aligned-box predicates):

```
| ans(q on coreset) - ans(q on full data) |  ≤  ε * ans(q on full data)
```

with probability ≥ 1 − δ.

The coreset size depends on the **VC-dimension / shatter function** of `Q`, not on the data size.

## Architecture

```
Stream ──▶ Coreset builder ──▶ Coreset store (per-partition)
                                       │
                                       │  merge (associative)
                                       ▼
                              Merged coreset
                                       │
                                       ▼
                              Query executor
                                       │
                                       ▼
                              Confidence interval
                              [ans_low, ans_high, 1-δ]
```

## Components

| Module | Role |
|---|---|
| `src/coreset/sensitivity.py` | Sensitivity sampling (Feldman & Langberg) |
| `src/coreset/merge.py` | Streaming coreset construction with mergers — `O(log n)` levels |
| `src/coreset/queries/` | COUNT, SUM, AVG, quantile, histogram |
| `src/bounds/` | ε / δ → coreset size; coreset → confidence interval |
| `src/store/` | Per-partition serializable coreset |
| `src/eval/` | Validate guarantees hold empirically over 10⁴ random queries |

## The streaming coreset

Merge-and-reduce skeleton:

```
build(stream):
    levels = []
    for batch in stream:
        coreset = local_coreset(batch)
        i = 0
        while levels[i] is not None:
            coreset = merge_and_reduce(levels[i], coreset)
            levels[i] = None
            i += 1
        levels[i] = coreset
    return merge_all(levels)
```

- `local_coreset(batch)`: size `s = O((1/ε²) log(1/δ) VCdim(Q))`
- `merge_and_reduce(a, b)`: produces a coreset of `a ∪ b` of size `s` (the error compounds by a factor `c` per level, but log-many levels keep total error bounded)

## Query → confidence interval

For a coreset `C` of `n` rows with weights `w_i`, and a query `q`:

```
estimate = Σ_{i ∈ C : q(i)} w_i
variance ≈ Σ_{i ∈ C : q(i)} w_i² * (sensitivity bound)
CI       = estimate ± z_{1-δ/2} * sqrt(variance)
```

The interval is returned together with the point estimate. Useful for dashboards: "users yesterday: 1,205,000 ± 14,000 (99% CI)".

## Empirical validation

Run 10,000 random predicates `q ~ Q`. For each:
- True answer (full scan)
- Coreset answer + CI

Check: ≥ (1 − δ) fraction have true answer inside CI. If not, the bounds are wrong — debug.

## Benchmarks

- **TPC-H** at SF=1000, with coreset 0.01% size:
  - Q1 (heavy aggregate): target ε ≤ 1% with 99% confidence
  - Q6 (filter + sum): same
  - Q19 (predicate-heavy join): challenging — current research

## References

- Feldman & Langberg, "A unified framework for approximating and clustering data" (STOC 2011)
- Bachem, Lucic, Krause, "Coresets for clustering and classification" (NeurIPS 2017)
- Chazelle, *The Discrepancy Method* (2000) — geometric coresets

## Roadmap

- [ ] Sensitivity-sampling coreset for COUNT/SUM
- [ ] Streaming merge-and-reduce
- [ ] Quantile coreset (KLL or rank-based)
- [ ] Histogram coreset
- [ ] CI computation
- [ ] Distributed merge protocol
- [ ] 10k-query empirical guarantee test
