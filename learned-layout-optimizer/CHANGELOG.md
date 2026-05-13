# Changelog

All notable changes to **learned-layout-optimizer** are documented in this file.
Format: [Keep a Changelog](https://keepachangelog.com/en/1.1.0/).

## [0.1.0] — 2026-05-13

### Added

- **Curves** (`llo.curves.spacefill`)
  - `z_order_index` — N-D Morton interleave with input validation
    (rejects negative, oversized, wrong shape, bad bit count).
  - `hilbert_index` — 2-D Hilbert curve (iterative rotate/reflect).
  - `hilbert_index_nd` — N-D Hilbert via Skilling's transposed-axes
    algorithm; `d * bits ≤ 63`.
- **Workload** (`llo.workload`)
  - `Query` (frozen) + `WorkloadProfile` tracking per-column equality
    vs range counts, mean range selectivity, and pairwise
    co-occurrence.
  - `DriftDetector` using total-variation distance over normalised
    column-frequency vectors; `calibrate / score / has_drifted`.
- **Policies** (`llo.policy.bandit`)
  - `Action` with strict arity validation per kind.
  - `HeuristicPolicy` (workload-aware rule).
  - `UCBPolicy` (UCB1, configurable exploration coefficient).
  - `EpsilonGreedyPolicy` (injectable RNG).
  - `ThompsonPolicy` (Gaussian Thompson sampling with known variance).
- **Replay** (`llo.replay.pages`)
  - Page-model shadow replay: `apply_layout`, `pages_scanned`,
    `expected_pages`, `reward` (amortised pages-saved-vs-noop minus
    per-query I/O cost).
- **Agent** (`llo.agent.loop`)
  - `LayoutAgent` closed loop: observe → choose → replay → update,
    with sliding-window recent queries and drift re-baselining.
- **Benchmark** (`llo.bench`)
  - Synthetic table + shifted-workload generator (pair-of-columns
    rotates every `shift_every` queries).
  - `evaluate_static` for static-layout comparison.
- **CLI** (`llo`)
  - `info`, `bench`, `simulate` subcommands.
- **Quality**
  - 73 pytest tests (~70 deterministic + Hypothesis property tests
    over Z-order determinism and uniqueness on unique coords).
  - mypy `--strict`, ruff lint + format clean.
  - Multi-stage slim Dockerfile, non-root `llo` user.
  - GitHub Actions matrix (Python 3.10 / 3.11 / 3.12) + Docker build
    smoke step.

### Notes

- Hilbert 2-D consecutive-keys-are-neighbours is exercised as a
  property test: on a full 8×8 grid the max Manhattan distance between
  successive curve points is exactly 1.
- `Action` rejects ill-shaped instances at construction time:
  `noop` must be column-less, `sortkey` exactly one column,
  `zorder`/`hilbert` ≥ 2 columns.
