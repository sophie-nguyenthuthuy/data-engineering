# Changelog

## [0.1.0] — 2026-05-13

### Added
- Column + Schema with int64/float64/string/bool typing, null-aware,
  duplicate-name rejection, strict bool/int separation.
- Encodings (each round-trip tested + isolated):
  * plain (length-prefixed baseline, supports NULL)
  * RLE (run-length, handles NULL as distinct run)
  * dictionary (low-cardinality, NULL via index -1)
  * delta (monotone int64, rejects NULL)
- `ColumnStats.from_values` + `Predicate` + `can_skip_row_group`
  pushdown logic (=, <, ≤, >, ≥, ≠, IS_NULL, NOT_NULL).
- Three mini formats:
  * **Parquet-like** — row groups, per-chunk encoding heuristic,
    stats footer (JSON).
  * **ORC-like** — stripes with *leading* stats index so a
    reader skips stripes without seeking to the file end.
  * **Avro-like** — row-oriented, one-shot schema header, body
    gzip-compressed.
- `run_benchmark(schema, columns)` → `BenchmarkResult` with
  `best_compression`, `fastest_read`, `fastest_write` helpers.
- CLI `povactl info | bench`.
- 54 tests including 2 Hypothesis properties (plain int round-trip,
  dictionary low-cardinality round-trip).
- mypy `--strict` clean over 18 source files; ruff clean.
- Multi-stage slim Docker image, non-root `pova` user.
- Zero runtime dependencies (gzip from stdlib).
