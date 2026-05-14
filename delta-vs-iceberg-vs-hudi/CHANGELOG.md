# Changelog

## [0.1.0] — 2026-05-13

### Added
- **Delta**: `DeltaTable` with optimistic-concurrency `commit`,
  log-replay state (`files_at`, `current_schema_id`), explicit
  `compact` operation, raised `DeltaConflict` on version race.
  Filenames are 20-digit zero-padded.
- **Iceberg**: `IcebergTable` with snapshot tree, `append/delete/
  overwrite/rollback/files_at` and parent-chain time travel that
  keeps abandoned forward snapshots reachable.
- **Hudi**: `HudiCoWTable` (rewrites base on every upsert) and
  `HudiMoRTable` (base + appended log files + explicit
  `compact`). Both expose a timeline of `(action, files_added,
  files_removed)`.
- `CDCEvent` + `Workload` shared across all four format drivers.
- `run_workload(wl)` produces a `CompareReport` with per-format
  commit count, write amplification, and read-time file count —
  the three numbers that distinguish the formats in practice.
- CLI `tflctl info | compare`.
- 41 tests including a threaded concurrent-commit race for the
  Delta optimistic-concurrency contract.
- mypy `--strict` clean over 12 source files; ruff clean.
- Multi-stage slim Docker image, non-root `tfl` user.
- Zero runtime dependencies.
