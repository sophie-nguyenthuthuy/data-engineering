# Changelog

## [0.1.0] — 2026-05-13

### Added
- `Schema` with stable field ids + safe evolution
  (`add_column / drop_column / rename_column / promote_type`).
  Lossy promotions (e.g. double → int) rejected.
- `DataFile` record (path, record count, size, partition, column
  stats) with construction-time validation.
- `Manifest` + `ManifestEntry` + `FileStatus` (ADDED / EXISTING /
  DELETED).
- `Snapshot` + `SnapshotOp` (APPEND / OVERWRITE / DELETE).
- `TableMetadata` — schema history + snapshot history + current
  pointers, immutable; helpers `with_schema`, `with_snapshot`,
  `rollback_to`, `current_snapshot`, `schema`, `snapshot`.
- `Storage` ABC + `CASMismatch` + two backends:
  * `InMemoryStorage` — RLock-guarded dict.
  * `LocalFSStorage` — POSIX with atomic rename + CAS by etag
    comparison.
- `Catalog` — Hive-like `(namespace, name) → metadata_path` with
  duplicate-registration rejection.
- `Table.create / append / delete / overwrite / files_at / rollback /
  evolve_schema`. Every commit serialises a new manifest then swaps
  the metadata pointer atomically via `Storage.atomic_put`.
- CLI `lakectl info | demo`.
- **46 tests** covering schema evolution paths (incl. error cases),
  storage CAS correctness, catalog scoping, snapshot replay through
  append→delete→overwrite, rollback non-destructiveness, evolve
  schema metadata version bump.
- mypy `--strict` clean over 13 source files; ruff clean.
- Multi-stage slim Docker image, non-root `lake` user.
- Zero runtime dependencies.

### Notes
- Time-travel works by walking the parent chain from a leaf snapshot
  to the root and applying each manifest's ADDED/DELETED entries in
  chronological order. Rollback only updates `current_snapshot_id`
  — abandoned forward snapshots stay reachable so you can roll back
  *and* roll forward.
