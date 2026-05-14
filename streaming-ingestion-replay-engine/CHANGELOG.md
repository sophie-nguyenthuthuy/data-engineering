# Changelog

## [0.1.0] — 2026-05-13

### Added
- Append-only log primitives:
  * `Record` + 24-byte big-endian `RecordHeader`
    (`offset, ts, klen, vlen`).
  * `Segment` — append-only byte buffer with index, persist to/from
    bytes; truncation errors raised as `SegmentError`.
  * `Topic` — single-partition, rolls segments at capacity;
    `seek_offset`, `seek_timestamp` lookups; RLock-guarded
    appends.
  * `Cursor` + `EndOfLog` sentinel for stateful consumption.
- Transform pipeline:
  * `Transform` ABC + `SKIP` sentinel.
  * `Mapper`, `Filter`, `ComposedTransform` (short-circuits on SKIP).
- Sinks: `Sink` ABC, `CollectingSink`, `JsonlFileSink`
  (base64-encoded key/value for safe text storage).
- `OffsetStore` — JSONL-persisted (group, topic) → next offset
  with atomic-rename flush.
- `ReplayEngine` with four entry points:
  `from_beginning / from_offset / from_timestamp / from_committed`,
  optional `max_records` cap, no auto-commit (transform-crash safe).
- CLI `sirectl info | demo`.
- 52 tests including 1 Hypothesis property
  (segment persist round-trip).
- mypy `--strict` clean over 18 source files; ruff clean.
- Multi-stage slim Docker image, non-root `sire` user.
- Zero runtime dependencies.

### Notes
- `from_committed` is intentionally read-only — the engine never
  auto-commits, so the caller can decide whether to advance the
  group's watermark after verifying the sink wrote successfully.
