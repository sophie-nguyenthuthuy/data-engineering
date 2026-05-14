# Changelog

## [0.1.0] — 2026-05-13

### Added
- `FileEvent` + `EventKind` with construction-time validation.
- `Manifest` / `ManifestEntry` — append-only JSONL with RLock.
- `Deduplicator` (`from_manifest` rehydration).
- `LateArrivalDetector(watermark_ms, grace_ms)`.
- Backends: `InMemoryBackend`, `PollingBackend` (injectable lister),
  `S3SqsBackend` + `parse_s3_event` (handles `Z`-suffixed
  `eventTime`, `ObjectCreated/Removed` mapping).
- `Runner.run_once → RunReport(processed, duplicates, late, failures)`
  with rehydration of dedupe + watermark from the manifest on
  construction.
- CLI `ifwctl info | demo | manifest`.
- 34 tests including a Hypothesis property (replaying the same
  backend twice never re-processes an event).
- mypy `--strict` clean over 12 source files; ruff clean.
- Multi-stage slim Docker image, non-root `ifw` user; GHA matrix.
- Zero runtime dependencies.
