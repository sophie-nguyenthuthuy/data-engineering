# stream-state-backend

A persistent, queryable state backend for stream processors — Flink-style managed
operator state built from scratch in Python.  It provides the five Flink state
primitives (ValueState, ListState, MapState, ReducingState, AggregatingState) backed
by RocksDB (with a dict-backed in-memory fallback for testing), an asynchronous
topology migration system, background TTL compaction, and a FastAPI read API so
external systems can inspect live operator state.

---

## Architecture

```
┌─────────────────────────────────────────────────────────┐
│                   Operator / User Code                  │
│  ctx = mgr.get_state_context("word_count", record_key)  │
│  state = ctx.get_value_state("count", default=0)        │
└───────────────────────┬─────────────────────────────────┘
                        │  StateContext
┌───────────────────────▼─────────────────────────────────┐
│              StateBackendManager                        │
│  ┌─────────────┐  ┌─────────────┐  ┌────────────────┐  │
│  │ TTLCompactor│  │TopologyMig. │  │   FastAPI API  │  │
│  │ (daemon thr)│  │(asyncio task│  │  /operators/…  │  │
│  └──────┬──────┘  └──────┬──────┘  └───────┬────────┘  │
│         │                │                 │            │
│  ┌──────▼────────────────▼─────────────────▼──────────┐ │
│  │              StorageBackend (abstract)              │ │
│  │   RocksDBBackend          MemoryBackend             │ │
│  │   (column families)       (SortedDict CFs)          │ │
│  └─────────────────────────────────────────────────────┘ │
└─────────────────────────────────────────────────────────┘

Key encoding (per CF = operator_id::state_name):
  record key  → msgpack bytes
  value       → 8-byte big-endian ms timestamp | msgpack payload
  MapState    → per-entry key: record_key_bytes + 0xff + msgpack(map_key)
  tombstone   → 0x00 single byte (written on clear(), cleaned by compactor)
```

---

## Quick Start

```bash
pip install -e ".[dev]"          # in-memory backend (tests)
pip install -e ".[dev,rocksdb]"  # with RocksDB support
```

```python
from ssb import StateBackendManager, TopologyDescriptor, OperatorDescriptor

# 1. Create and start manager
mgr = StateBackendManager("/tmp/mydb", backend="rocksdb")  # or "memory"
mgr.start(run_api=True)  # starts compactor + FastAPI on :8765

# 2. Register topology (optional — auto-created on first use)
topo = TopologyDescriptor(
    version=1,
    operators={
        "word_count": OperatorDescriptor("word_count", state_names=["count"])
    },
)
mgr.set_topology(topo)

# 3. Use state handles
ctx = mgr.get_state_context("word_count", record_key="hello")
count = ctx.get_value_state("count", default=0)
count.set(count.get() + 1)
print(count.get())  # 1

# 4. Topology migration (async)
import asyncio
new_topo = TopologyDescriptor(version=2, operators={...})
task = asyncio.run(mgr.update_topology(new_topo))
# task.progress → (migrated_keys, total_keys)

mgr.stop()
```

---

## State Handle API

| Method | ValueState | ListState | MapState | ReducingState | AggregatingState |
|--------|-----------|-----------|----------|---------------|-----------------|
| `get()` | `T\|None` | `list[T]` | — | `T\|None` | `OUT` |
| `set(v)` | ✓ | — | — | — | — |
| `add(v)` | — | ✓ | — | ✓ reduce_fn | ✓ add_fn |
| `update(lst)` | — | ✓ | — | — | — |
| `get(key)` | — | — | `V\|None` | — | — |
| `put(k,v)` | — | — | ✓ | — | — |
| `remove(k)` | — | — | ✓ | — | — |
| `keys()` | — | — | ✓ | — | — |
| `values()` | — | — | ✓ | — | — |
| `items()` | — | — | ✓ | — | — |
| `contains(k)` | — | — | ✓ | — | — |
| `clear()` | ✓ | ✓ | ✓ | ✓ | ✓ |

---

## TTL Configuration

```python
from ssb import TTLConfig

ttl = TTLConfig(ttl_ms=60_000, update_on_read=True)  # 1-minute idle TTL
state = ctx.get_value_state("session", ttl=ttl)
```

- `update_on_read=False` (default): entry expires `ttl_ms` after it was **written**.
- `update_on_read=True`: expiry resets on every `get()` call.
- The `TTLCompactor` background thread scans CFs and physically deletes expired
  entries every `compaction_interval_s` seconds (default 5 s).
- `clear()` writes a tombstone (`0x00`) so downstream replicas can detect
  deletions; tombstones are removed by the compactor.

---

## Read API Reference

Start the API server: `mgr.start(run_api=True)` or run standalone:

```bash
uvicorn "ssb.api.server:create_app" --factory --host 0.0.0.0 --port 8765
```

| Method | Path | Description |
|--------|------|-------------|
| GET | `/health` | `{"status":"ok","version":<topo_version>}` |
| GET | `/topology` | Current `TopologyDescriptor` as JSON |
| GET | `/topology/migrations` | Active + completed migration tasks |
| GET | `/operators` | List all operator IDs |
| GET | `/operators/{op_id}/state-names` | State names for operator |
| GET | `/operators/{op_id}/{state_name}/keys` | Paginated record keys (`limit`, `cursor`) |
| GET | `/operators/{op_id}/{state_name}` | Paginated entries with decoded values |
| GET | `/operators/{op_id}/{state_name}/{key}` | Single decoded value or 404 |

Pagination: responses include `next_cursor` (base64-encoded); pass as `?cursor=`
on the next request.  Keys in path params are JSON-encoded (e.g. `"hello"`, `42`).

---

## Running Tests

```bash
make install       # pip install -e ".[dev]"
make test          # pytest tests/ -v
```

Individual suites:

```bash
pytest tests/test_state_types.py -v    # CRUD for all 5 state types
pytest tests/test_ttl.py -v            # TTL expiry + tombstone compaction
pytest tests/test_migration.py -v      # Topology migration
pytest tests/test_api.py -v            # FastAPI endpoints
```

---

## Running the Examples

```bash
make example-word-count      # python examples/word_count.py
make example-windowed-join   # python examples/windowed_join.py
```

`word_count.py` — simulates 1 000 events from a sentence corpus, maintains per-word
counts using `ValueState`, prints the top-10, then demonstrates a live API query.

`windowed_join.py` — simulates a windowed stream join between click events and
purchase events using `ListState` (click buffer) + `MapState` (purchase index),
both with configurable TTL windows.  Demonstrates TTL expiry and compaction.

---

## Project Layout

```
src/ssb/
  manager.py              StateBackendManager + StateContext
  backend/
    base.py               Abstract StorageBackend interface
    rocksdb_backend.py    RocksDB column-family implementation
    memory_backend.py     SortedDict-backed in-memory backend
  state/
    descriptor.py         StateDescriptor, TTLConfig
    handle.py             ValueState, ListState, MapState, ReducingState, AggregatingState
    serializer.py         msgpack + timestamp encoding helpers
  ttl/
    compactor.py          Background TTL compaction daemon
  topology/
    descriptor.py         OperatorDescriptor, TopologyDescriptor
    migrator.py           MigrationTask + TopologyMigrator (asyncio)
  api/
    server.py             FastAPI read API
```
