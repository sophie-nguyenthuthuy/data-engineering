# streaming-ingestion-replay-engine

Kafka-style replay engine. An append-only log of records (segments +
offsets + timestamps) with seek-by-offset and seek-by-timestamp
primitives, an on-the-fly `Transform` pipeline, pluggable sinks, and a
persistent `OffsetStore` so consumer groups can resume.

[![Python](https://img.shields.io/badge/python-3.10%20%7C%203.11%20%7C%203.12-blue.svg)](#)
[![License: MIT](https://img.shields.io/badge/license-MIT-green.svg)](LICENSE)

## Why

You don't always need a Kafka cluster. For tests, replays, or
mid-pipeline reprocessing of a captured stream, a small local
append-only log with the same surface (offset / timestamp seek,
consumer-group offsets, transforms during read) is plenty — and
testable byte-for-byte.

## Components

| Module                           | Role                                                         |
| -------------------------------- | ------------------------------------------------------------ |
| `sire.log.record`                | `Record` + 24-byte big-endian header (`offset, ts, klen, vlen`) |
| `sire.log.segment`               | `Segment` — append-only byte buffer; persistable             |
| `sire.log.topic`                 | `Topic` (single-partition) — rolls segments at capacity      |
| `sire.log.cursor`                | `Cursor` + `EndOfLog` sentinel                               |
| `sire.transforms`                | `Mapper`, `Filter`, `ComposedTransform`, `SKIP`              |
| `sire.sinks`                     | `Sink` ABC, `CollectingSink`, `JsonlFileSink`                |
| `sire.offsets`                   | `OffsetStore` — JSONL-persisted (group, topic) → next offset |
| `sire.replay`                    | `ReplayEngine.from_beginning / from_offset / from_timestamp / from_committed` |
| `sire.cli`                       | `sirectl info | demo`                                        |

## Install

```bash
pip install -e ".[dev]"
```

Python 3.10+. **Zero runtime dependencies.**

## Library

```python
from sire.log.topic       import Topic
from sire.replay          import ReplayEngine
from sire.sinks.collect   import CollectingSink
from sire.transforms.filter import Filter
from sire.transforms.mapper import Mapper
from sire.transforms.composed import ComposedTransform
from sire.offsets         import OffsetStore

topic = Topic(name="orders", segment_size_records=10_000)
topic.append(key=b"order-1", value=b"...payload...", timestamp=1_700_000_000_000)

upper = Mapper(fn=lambda r: r.__class__(
    offset=r.offset, timestamp=r.timestamp, key=r.key, value=r.value.upper()
))
keep = Filter(predicate=lambda r: r.value != b"DROP")

store = OffsetStore(path="/var/lib/sire/offsets.jsonl")
ReplayEngine(
    topic=topic,
    sink=CollectingSink(),
    transform=ComposedTransform(transforms=[upper, keep]),
    offsets=store,
).from_committed("my-consumer-group")
```

Re-running `from_committed` after a `store.commit(...)` resumes
exactly where the previous run left off — the watermark is stored
durably on disk via atomic rename.

## CLI

```bash
sirectl info
sirectl demo --records 10 --segment-size 4 --from-offset 4
```

## Record wire format

```
+--------+-----------+--------+--------+----------+----------+
| offset | timestamp | klen   | vlen   |   key    |  value   |
| Q (8B) |   q (8B)  | I (4B) | I (4B) | (klen B) | (vlen B) |
+--------+-----------+--------+--------+----------+----------+
```

Big-endian network order so an `xxd` dump is human-readable across
architectures. The format is unit-tested with a Hypothesis property
that exercises arbitrary `(timestamp, key, value)` triples across
random batch sizes.

## Replay positions

```python
engine.from_beginning()                # offset 0
engine.from_offset(N)                  # absolute offset
engine.from_timestamp(t)               # earliest record with ts ≥ t
engine.from_committed(group)           # next-offset stored for group
```

All four respect an optional `max_records=` cap. The engine emits
`engine.sink.flush()` after each pass; no auto-commit — so a
transform crash mid-batch can't accidentally advance the watermark.

## Quality

```bash
make test       # 50 tests
make type       # mypy --strict
make lint
```

- **50 tests**, 0 failing; includes 1 Hypothesis property
  (segment persist round-trip).
- mypy `--strict` clean over 14 source files; ruff clean.
- Multi-stage slim Docker image, non-root `sire` user.
- Python 3.10 / 3.11 / 3.12 CI matrix.
- **Zero runtime dependencies.**

## License

MIT — see [LICENSE](LICENSE).
