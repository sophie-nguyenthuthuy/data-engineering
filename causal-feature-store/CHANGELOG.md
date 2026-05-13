# Changelog

All notable changes to **causal-feature-store** are documented in this file.
Format: [Keep a Changelog](https://keepachangelog.com/en/1.1.0/).

## [0.1.0] — 2026-05-13

### Added

- **Vector-clock primitives** (`cfs.clock.vector_clock`)
  - `dominates`, `equal`, `lt`, `concurrent`, `pointwise_max`, `bump`.
  - Construction-time validation: rejects non-string keys, negative
    counters, and empty component names in `bump`.
- **Versioned stores** (`cfs.store`)
  - Frozen `Version(value, clock, wall)` record (`wall ≥ 0` enforced).
  - `HotStore(k)` — thread-safe, bounded-history online tier with
    `entity_clock` (pointwise max so far) and `versions` lookup;
    keeps the most recent `k` versions per `(entity, feature)`.
  - `ColdStore` — thread-safe, append-only history tier.
  - Every public method protected by `threading.RLock` so readers can
    chain `entity_clock` → `versions(...)` without race.
- **Writer** (`cfs.writer`)
  - `Writer.write` bumps the per-entity counter for the producing
    component and fans the record out to both tiers atomically under
    the writer's own `RLock`.
- **Resolver** (`cfs.serving.resolver`)
  - `Resolver.get` assembles a single causally consistent snapshot
    (`features`, `chosen_clock`, `missing`).
  - `Resolver.verify` re-reads each returned value and asserts
    `chosen_clock` dominates the matching `Version.clock`.
- **Partition simulator** (`cfs.partition`)
  - `PartitionScenario` with two sides and pre-heal foreign-component
    isolation; `heal()` lifts the restriction so cross-component
    writes can proceed.
- **CLI** (`cfsctl`)
  - `info`, `demo`, `partition` subcommands.
- **Quality**
  - **52 pytest tests** including:
    - 6 Hypothesis lattice properties (`dominates` is reflexive,
      antisymmetric, transitive; `pointwise_max` is the join and is
      commutative + associative).
    - 2 threaded tests (`HotStore` and `ColdStore` each survive two
      concurrent producers writing 500 records each).
    - End-to-end resolver invariant: `Resolver.verify` returns `True`
      after randomised writer bursts.
  - mypy `--strict` clean over 12 source files.
  - Multi-stage slim Dockerfile, non-root `cfs` user.
  - GitHub Actions matrix (Python 3.10 / 3.11 / 3.12) + Docker build
    smoke step.

### Notes

- **Zero runtime dependencies.** The package uses stdlib only — no
  numpy, no third-party data structures.
- `RLock` (re-entrant lock) is mandatory: the resolver calls
  `hot.entity_clock(...)` and then `hot.versions(...)` for each feature
  from the same thread. With a plain `Lock` that would self-deadlock on
  any code path that recursed into the store.
- `Resolver` returns a feature in `missing` rather than raising when no
  stored version is dominated by `target`; the caller decides whether
  to retry (in case a writer just produced one) or fall back.
- During a `PartitionScenario` the "foreign" components are rejected
  at write time, so the test suite cannot accidentally smuggle
  cross-partition state into the pre-heal snapshot.
