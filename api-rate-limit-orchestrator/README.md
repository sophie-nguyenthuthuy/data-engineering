# api-rate-limit-orchestrator

A distributed **token-bucket** rate limiter. N workers share one
upstream quota via a pluggable storage backend; ships an in-memory
backend (`threading.RLock`-guarded, deterministic) for tests and a
ready-to-paste Redis Lua script for production.

[![Python](https://img.shields.io/badge/python-3.10%20%7C%203.11%20%7C%203.12-blue.svg)](#)
[![License: MIT](https://img.shields.io/badge/license-MIT-green.svg)](LICENSE)

## Why

Five ingestion workers behind one API key with a 1000 req/min quota
will burn through the budget in 6 s if they don't coordinate. A
distributed token bucket lets each worker check out tokens from a
single bucket — the bucket lives outside any single worker, in Redis
or another atomic store — so concurrent acquisition is correct by
construction.

## Architecture

```
   Worker 1 ┐
   Worker 2 ├──► TokenBucket(key, Quota) ──► StorageBackend.atomic_take(...)
   ...      ┘                                  │
                                               ├── InMemoryStorage  (tests)
                                               └── Redis + Lua       (prod)
```

The single atomic primitive every backend implements:

```
atomic_take(key, capacity, refill_per_second, requested, now) -> (took, new_state)
```

`InMemoryStorage` holds an `RLock` for the read-modify-write; Redis
runs the bundled Lua script inside `EVAL`.

## Install

```bash
pip install -e ".[dev]"
```

Python 3.10+. **Zero runtime dependencies.**

## CLI

```bash
arlctl info                            # version
arlctl lua                             # print the Redis Lua script
arlctl sim --workers 4 --rps 10 --duration 2.0
```

## Library

```python
from arlo.quota             import Quota
from arlo.bucket            import TokenBucket
from arlo.orchestrator      import Orchestrator
from arlo.storage.inmemory  import InMemoryStorage

storage = InMemoryStorage()                       # or your Redis adapter
bucket  = TokenBucket(key="github-api",
                       quota=Quota.per_minute(1_000),
                       storage=storage)

# Try once; if not enough tokens, get back the suggested wait.
r = bucket.acquire(1.0)
if not r.took:
    time.sleep(r.suggested_wait)

# Or block with bounded backoff:
Orchestrator(bucket=bucket, max_wait=30.0).wait_and_acquire()
```

For Redis, paste the script from `arlo.storage.redis_lua.REDIS_TOKEN_BUCKET_LUA`
into your Redis client's `register_script(...)` and use the returned
sha-cached callable.

## Components

| Module                          | Role                                                              |
| ------------------------------- | ----------------------------------------------------------------- |
| `arlo.quota`                    | `Quota(capacity, refill_per_second)` + `per_second/minute/hour`   |
| `arlo.storage.base`             | `StorageBackend` ABC, `BucketState`                               |
| `arlo.storage.inmemory`         | `InMemoryStorage` (RLock-guarded)                                 |
| `arlo.storage.redis_lua`        | `REDIS_TOKEN_BUCKET_LUA` script, `render_redis_lua()`             |
| `arlo.bucket`                   | `TokenBucket` + `AcquireResult`                                   |
| `arlo.orchestrator`             | `Orchestrator.wait_and_acquire`, `AcquireTimeout`                 |
| `arlo.cli`                      | `arlctl info | lua | sim`                                        |

## Token-bucket math

```
elapsed       = now − last_refill_ts                  (clamped to 0)
tokens'       = min(capacity, tokens + elapsed · refill_per_second)
if requested ≤ tokens'    →  take, new_tokens = tokens' − requested
else                       →  refuse, new_tokens = tokens'
last_refill_ts := now in both branches
```

A failed `acquire` still refreshes the timestamp; the returned
`suggested_wait` = `(requested − tokens') / refill_per_second` is the
soonest the caller could succeed.

## Quality

```bash
make lint       # ruff
make format
make type       # mypy --strict
make test       # 32 tests
```

- **32 tests**, 0 failing; includes 1 Hypothesis property test (at
  t=0 the bucket cannot serve more than its capacity).
- A deterministic-clock concurrency test runs 8 threads × 50 iterations
  against one bucket and verifies the total tokens served never
  exceeds `capacity + elapsed × refill`.
- mypy `--strict` clean over 9 source files.
- Multi-stage slim Docker image, non-root `arlo` user.
- Python 3.10 / 3.11 / 3.12 CI matrix + Docker build smoke step.

## License

MIT — see [LICENSE](LICENSE).
