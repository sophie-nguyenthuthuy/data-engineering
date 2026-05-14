# Changelog

## [0.1.0] — 2026-05-13

### Added
- Lightweight SQL extractor (`parse_query`) covering `WHERE` (=, <>,
  <, ≤, >, ≥, IN, BETWEEN, IS NULL), `JOIN ... ON`, `GROUP BY`. No
  external SQL-parser dependency.
- `Profiler` aggregates per-column usage across a query log; emits a
  `QueryProfile` with filter / join / group counts.
- `estimate_cardinality` — Chao1-style distinct-count estimator with
  a sample-size cap so all-singleton inputs don't blow up.
- `detect_skew` — coefficient of variation + top-3 share; both used
  by `is_skewed(cv≥1 or top3≥0.5)`.
- `recommend(profile, cardinalities, skews)` returns a typed
  partition + bucket recommendation with explicit reasons.
  Bucket count is rounded to a power-of-two ≈ √estimated-distinct,
  floor 8, cap 1024.
- CLI `psactl info | profile | recommend`.
- 35 tests including 1 Hypothesis property (parser is deterministic
  on the same input).
- mypy `--strict` clean over 7 source files; ruff clean.
- Multi-stage slim Docker image, non-root `psa` user.
- Zero runtime dependencies.

### Notes
- The original uniform-distribution skew test used 4 distinct values
  × 100 each, which made top-3 share = 0.75 by definition. Rewrote
  with 20 distinct values × 50 each so the test exercises true
  uniformity rather than tripping on small cardinality.
