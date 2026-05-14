# Changelog

## [0.1.0] — 2026-05-13

### Added
- `Quota(capacity, refill_per_second)` + per-second/minute/hour helpers.
- `StorageBackend` ABC + `BucketState` record.
- `InMemoryStorage` — RLock-guarded reference backend.
- Redis Lua script (`REDIS_TOKEN_BUCKET_LUA`) implementing the same
  atomic refill+take semantics; emitted by `render_redis_lua()` —
  callers pass the source to their Redis client's `register_script`.
- `TokenBucket` + `AcquireResult(took, tokens_remaining, suggested_wait)`.
- `Orchestrator.wait_and_acquire(tokens)` — bounded-wait polling loop
  with `AcquireTimeout`.
- CLI `arlctl info | lua | sim`.
- 32 tests, 1 Hypothesis property (max-tokens-at-t0 ≤ capacity).
- Deterministic-clock concurrency test: 8 threads × 50 iterations
  share one bucket; total acquired ≤ capacity + elapsed · refill.
- mypy `--strict` clean; ruff clean; Docker + GHA matrix.
- Zero runtime dependencies.

### Notes
- The first concurrency test used wallclock-bounded acquisitions and
  was flaky on macOS due to GIL scheduling — the rewrite uses a
  fake clock so the upper bound is exact.
