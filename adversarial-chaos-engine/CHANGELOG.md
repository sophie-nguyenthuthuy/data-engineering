# Changelog

All notable changes to **adversarial-chaos-engine** are documented in this file.
Format: [Keep a Changelog](https://keepachangelog.com/en/1.1.0/).

## [0.1.0] — 2026-05-13

### Added

- **Edge-case libraries** (`ace.edges`)
  - `INT_EDGES` (int8/16/32/64 overflow boundaries, off-by-one neighbours)
  - `FLOAT_EDGES` (±inf, NaN, ±0.0, subnormals, ULP-of-1)
  - `STRING_EDGES` (null byte, Unicode oddities, NFC vs NFD, ZWJ, RTL
    override, SQL/log4j/path-traversal/null-byte injection prefixes,
    length-bomb, lone surrogate)
  - `TIMESTAMP_EDGES` (Unix epoch, DST jumps, Y2K38, leap-second
    boundary, 9999-12-31)
- **Invariant DSL** (`ace.invariants`)
  - Per-`Catalog` registry — no module-level global state (the original
    prototype's biggest correctness issue).
  - `default_catalog()` for convenience in scripts.
  - Builtin checks: `row_count_preserved`, `sum_invariant`,
    `column_no_nulls`, `column_value_range`, `monotone_increasing`,
    `distinct_count_preserved`.
  - `sum_invariant` propagates NaN so NaN-laden inputs are rejected
    instead of silently summing to 0.
  - Each spec exposes the columns it references; the generator uses
    that set to bias edge-case sampling.
- **Adversarial generator** (`ace.generator`)
  - `AdversarialGenerator(edge_fraction, max_rows, rng)` — edge-biased
    sampling on targeted columns.
  - `generate_random()` — pure-random baseline for benchmark comparison.
  - Column-kind detection (`id`, `timestamp`, `string`, `numeric`)
    picks the right edge library.
- **Shrinker** (`ace.shrinker`)
  - `shrink_rows(fn, rows, check)` — Zeller-style delta debugging that
    repeatedly removes a row while the check still fails.
- **Runner** (`ace.runner`)
  - `Runner(catalog, generator, seed).run(trials)` →
    `Report(n_trials, n_pipelines, violations)`.
  - Per-`(function, invariant)` deduplication — a single violation per
    pair is reported (not one per trial).
  - Exceptions are recorded as `Violation(is_exception=True)`.
- **Regression emitter** (`ace.regression`)
  - `emit_pytest(violation, module=...)` → Python source verified to
    parse with `ast.parse`. Picks the right assertion per invariant
    family.
- **Bug-zoo benchmark** (`ace.bench`)
  - `run_benchmark(trials, seed)` compares targeted vs. random fuzzing
    on three seeded buggy pipelines and reports counts and a
    `speedup` ratio.
- **CLI** (`acectl`)
  - `info`, `edges`, `run`, `bench` subcommands.
- **Quality**
  - **49 pytest tests** including 1 Hypothesis property test (generator
    always returns `list[dict]` on any seed).
  - mypy `--strict` clean over 14 source files.
  - Multi-stage slim Dockerfile, non-root `ace` user.
  - GitHub Actions matrix (Python 3.10 / 3.11 / 3.12) + Docker build
    smoke step.
  - **Zero runtime dependencies** — stdlib only.

### Notes

- The original prototype's regression emitter used `textwrap.dedent`
  over an f-string that interpolated a multi-line `rows_repr` block.
  `dedent` only strips the COMMON leading whitespace, and after
  interpolation the common prefix was zero — producing invalid Python.
  Rebuilt as a line-by-line `"\n".join(...)` with explicit indentation,
  and added an `ast.parse(source)` sanity check inside `emit_pytest`
  so the unit tests can rely on the parser instead of regex matching.
- The runner deduplicates violations per `(fn_name, invariant)` to
  avoid drowning the user in a stream of identical reports — the
  shrinker would otherwise re-shrink to the same minimal frame many
  times in a row.
- The bench result is intentionally honest: at 100 trials per pipeline
  the random and targeted fuzzers both find 2/3 seeded bugs on the
  current bug zoo. The third bug (`buggy_default_zero`) needs a
  literal `None` in the `name` column which neither library currently
  produces; that's flagged as a future expansion in this changelog.
