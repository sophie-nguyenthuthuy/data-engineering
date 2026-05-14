# Changelog

## [0.1.0] — 2026-05-13

### Added
- `Tenant` + `TenantRegistry` with validated id pattern.
- `SourceSpec` + `SourceRegistry` — per-tenant source scoping.
- `ResourceQuota` (cpu/storage/qps) + `ResourceUsage` (RLock-guarded).
- `StorageNamespace` — rejects absolute paths + `..` traversal.
- `ComputeSlots` — bounded tenant-tagged pool.
- `AdmissionController` → `Decision` (admit / over-cpu / over-storage /
  over-qps / unknown-tenant).
- `FairScheduler` — Weighted Deficit Round Robin (Shreedhar &
  Varghese 1995).
- `Platform` facade tying everything together; `submit_job` /
  `release`.
- CLI `mtipctl info | demo`.
- 41 tests including 1 Hypothesis property (heavier weights serve at
  least as many jobs as lighter ones).
- mypy `--strict` clean over 12 source files; ruff clean.
- Multi-stage slim Docker image, non-root `mtip` user; GHA matrix.
- Zero runtime dependencies.
