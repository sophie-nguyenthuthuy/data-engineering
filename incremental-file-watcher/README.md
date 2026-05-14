# incremental-file-watcher

Watch an S3 / MinIO prefix for new objects, dedupe processed files
through a JSONL manifest, and label late-arriving files so downstream
can route them to a slower pipeline. Event-driven (SQS) is the
preferred path; a polling backend is bundled as the fallback when
event notifications aren't available.

[![Python](https://img.shields.io/badge/python-3.10%20%7C%203.11%20%7C%203.12-blue.svg)](#)
[![License: MIT](https://img.shields.io/badge/license-MIT-green.svg)](LICENSE)

## Why

Two questions every incremental pipeline must answer:

1. **"Did I already process this file?"** — usually solved with
   client-side state; the manifest here is its on-disk JSONL form.
2. **"Is this file 'old data' I'm only seeing now?"** — answered by
   comparing the event's `last_modified` to a high-water mark
   (manifest watermark + grace window).

This package gives you both, plus pluggable backends so the same code
runs against either an SQS-triggered S3 event stream or a polling diff
of `ListObjectsV2`.

## Architecture

```
   ┌───────────────────────┐
   │ S3SqsBackend          │   parses S3-→SQS notifications
   │ PollingBackend        │   diffs ListObjectsV2 by ETag
   │ InMemoryBackend       │   tests
   └────────────┬──────────┘
                │ FileEvent
                ▼
   ┌───────────────────────┐
   │   Deduplicator        │  ← Manifest.keys()  (rehydration)
   └────────────┬──────────┘
                ▼
   ┌───────────────────────┐
   │ LateArrivalDetector   │   late = lm + grace < watermark
   └────────────┬──────────┘
                ▼
   ┌───────────────────────┐
   │   Runner.processor    │
   └────────────┬──────────┘
                ▼
       Manifest.record(ManifestEntry)
```

## Install

```bash
pip install -e ".[dev]"
```

Python 3.10+. **Zero runtime dependencies.**

## CLI

```bash
ifwctl info
ifwctl demo --manifest /tmp/ifw.jsonl --events 5
ifwctl manifest --manifest /tmp/ifw.jsonl --tail 10
```

## Library

```python
from pathlib import Path
from ifw.backends.s3_sqs   import S3SqsBackend
from ifw.backends.polling  import PollingBackend
from ifw.runner            import Runner
from ifw.manifest          import Manifest
from ifw.late              import LateArrivalDetector

# Either driver works.
sqs_backend = S3SqsBackend(client=(my_receive_fn, my_delete_fn))
polling     = PollingBackend(bucket="my-bucket", lister=my_list_objects_fn)

runner = Runner(
    backend=sqs_backend,
    manifest=Manifest(path=Path("/var/lib/ifw/manifest.jsonl")),
    processor=process_file,
    late=LateArrivalDetector(grace_ms=60_000),
)
while True:
    report = runner.run_once()
    print(report)
```

The runner rehydrates its dedupe set + watermark from the manifest at
construction time, so the second invocation in a new process sees the
same state as the first.

## Components

| Module                       | Role                                                            |
| ---------------------------- | --------------------------------------------------------------- |
| `ifw.events`                 | `FileEvent`, `EventKind` (validated)                            |
| `ifw.manifest`               | `Manifest`, `ManifestEntry` (JSONL, RLock, watermark)           |
| `ifw.dedupe`                 | `Deduplicator(.from_manifest)`                                  |
| `ifw.late`                   | `LateArrivalDetector(watermark_ms, grace_ms)`                   |
| `ifw.backends.base`          | `Backend` ABC                                                   |
| `ifw.backends.inmemory`      | `InMemoryBackend` — test double                                 |
| `ifw.backends.polling`       | `PollingBackend` (injectable `lister`)                          |
| `ifw.backends.s3_sqs`        | `S3SqsBackend`, `parse_s3_event`                                |
| `ifw.runner`                 | `Runner.run_once → RunReport`                                  |
| `ifw.cli`                    | `ifwctl info | demo | manifest`                                |

## Backend choice

- **S3SqsBackend** — production path. Hook your S3 event-notification
  → SQS queue into the backend's injectable `(receive, delete)` pair.
- **PollingBackend** — diff `ListObjectsV2` results by `ETag`; emits
  CREATED on first sight, MODIFIED when ETag changes for the same key.
- **InMemoryBackend** — used in the test suite.

## Late-arrival semantics

```
high_water = max(manifest.last_modified_ms)
late_event  ↔  event.last_modified_ms + grace_ms < high_water
```

Late events are *still processed* (and recorded) — they just get
counted separately in `RunReport.late`, so the caller can wire that
to a Prometheus counter or alert.

## Quality

```bash
make test       # 36 tests, 1 Hypothesis property
make type       # mypy --strict
make lint
```

- **36 tests**, 0 failing; Hypothesis property: running the same
  backend twice never re-processes an event.
- mypy `--strict` clean over 11 source files; ruff clean.
- Multi-stage slim Docker image, non-root `ifw` user.
- Python 3.10 / 3.11 / 3.12 CI matrix + Docker smoke step.

## License

MIT — see [LICENSE](LICENSE).
