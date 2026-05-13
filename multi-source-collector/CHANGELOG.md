# Changelog

## [0.1.0] — 2026-05-13

### Added
- **Naming convention** (`msc.naming`) — slug-normalised
  `<source>/<dataset>/<YYYY>/<MM>/<DD>/<run_id>.<ext>` with
  round-trippable `StagedKey.path()` / `StagedKey.parse()`.
- **Idempotency manifest** (`msc.manifest`) — append-only JSONL,
  `RLock`-guarded for parallel writers; `Manifest.has`, `latest`,
  `record`.
- **Source adapters** — `CSVSource`, `ExcelSource` (opt extra),
  `HTTPAPISource` (injectable fetcher), `FTPSource` (injectable
  connect), `GoogleSheetSource` (keyless GViz CSV endpoint), all
  with construction-time validation.
- **Staging zone** (`msc.staging.zone`) — atomic JSONL writes via
  `tmp + os.replace`; `WriteReport(staged_path, bytes_written,
  row_count, sha256)`.
- **Runner** — drives `Source → StagingZone → Manifest`; honours
  manifest for idempotency (re-run returns `skipped=True`).
- **CLI** — `mscctl info | naming | ingest-csv | list-staging |
  manifest`.
- **Quality** — 56 pytest tests (incl. 1 Hypothesis property on
  the naming slug), mypy `--strict` clean over 14 source files,
  multi-stage Docker image, GitHub Actions matrix
  (Python 3.10 / 3.11 / 3.12) + Docker smoke step.
- **Zero required runtime dependencies** — stdlib only; openpyxl
  is opt-in via `[excel]`.

### Notes
- Sources use injectable transports (fetcher / connect) so tests
  drive every adapter against fakes without touching the network.
- The original Hypothesis property failed on `²` (superscript 2 is
  Unicode-alnum but not ASCII-alnum); the strategy was tightened
  to draw from `[a-zA-Z0-9]` only, and the assertion strengthened
  to require `c.isascii()`.
- mypy `--strict` flagged `urlopen(...).read()` as `Any`; pinned
  the result with an explicit `: bytes` annotation in both HTTP
  and GSheet default fetchers.
