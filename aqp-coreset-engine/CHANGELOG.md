# Changelog

All notable changes to **aqp-coreset-engine** are documented in this file.
Format: [Keep a Changelog](https://keepachangelog.com/en/1.1.0/).

## [0.1.0] — 2026-05-13

### Added

- **Coreset core** (`aqp.coreset.core`)
  - Immutable `Coreset`, `WeightedRow` (rejects negative weight),
    `ConfidenceInterval` with `contains` / `half_width`.
  - SUM / COUNT / AVG queries (predicate-aware) with Horvitz–Thompson
    estimators.
  - Two-sided Gaussian CIs: cached z-scores for `level ∈
    {0.90, 0.95, 0.99}`, fallback to a Beasley–Springer–Moro
    inverse-Φ approximation tested against the known
    1.96 and 2.5758 quantiles.
- **Predicates** (`aqp.queries.predicates`)
  - `eq_pred`, `range_pred`, `box_pred`, `and_`, `always_true`.
  - Input validation at construction time (no negative cols,
    no inverted ranges, no empty boxes).
- **Samplers**
  - `SensitivityCoreset` — offline Feldman–Langberg sensitivity
    sampler with input validation on (ε, δ, vc) and a fast-path that
    keeps every row when the population size ≤ target.
  - `UniformCoreset` — Algorithm-R reservoir; reweights survivors to
    the population size for unbiased SUM.
  - `StreamingSumCoreset` — merge-and-reduce streaming SUM coreset
    with binary level stack; `n_levels` and `n_rows` observables.
- **KLL quantile sketch** (`aqp.coreset.kll`)
  - `KLLSketch(k, seed)` and `KLLSketch.for_epsilon(eps)`; `add`,
    `quantile`, `rank`, associative `merge`.
- **Bounds** (`aqp.bounds.size`)
  - `coreset_size_sum(eps, delta, vc)` (Feldman–Langberg).
  - `hoeffding_count_size(eps, delta)` (Hoeffding bound).
- **Empirical validator** (`aqp.eval`)
  - `validate_coverage` runs N random 1-D range queries, reports
    coverage / mean / max relative error.
- **CLI** (`aqpctl`)
  - `info`, `size`, `validate`, `quantile` subcommands.
- **Quality**
  - 72 pytest tests (~70 deterministic + 2 Hypothesis property tests:
    `query_count` matches `total_weight` under random row sets, and
    KLL quantile is monotone in `q`).
  - mypy `--strict` clean over 13 source files.
  - Multi-stage slim Dockerfile, non-root `aqp` user.
  - GitHub Actions matrix (Python 3.10 / 3.11 / 3.12) + Docker build
    smoke step.

### Notes

- Confidence intervals use ``Σ contribᵢ²`` as a per-row variance upper
  bound — conservative but does not require second-order inclusion
  probabilities. Empirical coverage on rare-stratum range queries is
  well above the nominal level.
- `Coreset` is a frozen dataclass with a `tuple` of rows — coresets
  are safely shared across threads / queries.
- Merge-and-reduce uses uniform downsampling as a degenerate fallback
  when the reducer's contribution-sum is exactly zero, which would
  otherwise produce ``0/0`` probabilities.
