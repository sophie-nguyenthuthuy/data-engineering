# LSM-Remote-Compaction

A **Log-Structured Merge Tree** extended with a **tiered remote compaction worker** that communicates over **gRPC**.  The local node continues serving reads and writes uninterrupted during remote compaction.  A **quorum-based commit protocol** ensures the new SSTable is acknowledged by a majority of peers before it is applied.

## Architecture

```
┌──────────────────────────────────────────────────────────┐
│  Local LSM Node                                          │
│                                                          │
│  ┌──────────┐  WAL  ┌──────────┐                        │
│  │  Client  │──────▶│ MemTable │  (skip list, RW-locked) │
│  │  Writes  │       └─────┬────┘                        │
│  └──────────┘             │ flush (background)           │
│                           ▼                              │
│                     ┌──────────┐                        │
│  Client ──Get()──▶  │  L0 SSTs │ (overlapping ranges)   │
│                     ├──────────┤                        │
│                     │  L1 SSTs │ (non-overlapping)       │
│                     ├──────────┤                        │
│                     │  …  LN  │                        │
│                     └────┬─────┘                        │
│                          │ trigger: L0 ≥ 4 files        │
│                          │   or level exceeds budget     │
└──────────────────────────┼──────────────────────────────┘
                           │ gRPC: SubmitCompaction
                           ▼
┌──────────────────────────────────────────────────────────┐
│  Remote Compaction Worker                                │
│                                                          │
│  1. Receive SSTable bytes inline                         │
│  2. Merge + apply tombstones (k-way merge)               │
│  3. Solicit AcknowledgeCompaction from peer nodes        │
│  4. Quorum reached → mark job "done"                     │
│  5. Return merged SSTable bytes to leader node           │
└──────────────────────────────────────────────────────────┘
```

## Key components

| Path | Description |
|------|-------------|
| `internal/lsm/bloom.go` | Bloom filter (Kirsch-Mitzenmacher, serialisable) |
| `internal/lsm/skiplist.go` | Probabilistic skip list used as MemTable backing store |
| `internal/lsm/memtable.go` | Concurrent MemTable with RW lock |
| `internal/lsm/wal.go` | Append-only Write-Ahead Log with CRC32 per record |
| `internal/lsm/sstable.go` | Immutable SSTable: data blocks + index + bloom + footer |
| `internal/lsm/compaction.go` | k-way merge, tombstone elision at bottom level |
| `internal/lsm/lsm.go` | Main engine: flush loop, compact loop, read path |
| `internal/remote/client.go` | `RemoteCompactor` — ships files, polls, drives commit |
| `internal/remote/worker.go` | gRPC server: runs compaction, calls quorum |
| `internal/remote/quorum.go` | Quorum manager: collects acks, signals commit |
| `pkg/rpc/` | gRPC service (JSON codec, no protoc needed) |
| `cmd/node/` | LSM node binary with HTTP key-value API |
| `cmd/worker/` | Compaction worker binary |

## SSTable format

```
[Data block 0] … [Data block N]
[Index block]      ← (firstKey, offset, length) per block
[Bloom filter]     ← serialised bit array
[Footer: 40 B]     ← index_offset, index_len, bloom_offset, bloom_len, magic
```

Data blocks are 4 KiB targets, each CRC32-checked.  The bloom filter uses Kirsch-Mitzenmacher (two base FNV-1a hashes, k derived positions) targeting 1 % FPR.

## Compaction triggers

- **L0 → L1**: when L0 has ≥ 4 files (configurable)
- **L1 → L2 …**: when a level exceeds its byte budget (10× per level, starting at 10 MiB for L1)
- Bottom level tombstones are elided (no older data exists below)

## Quorum commit

```
Leader (LSM node)           Worker              Peer nodes
    |──SubmitCompaction──────▶|                    |
    |                         |──AcknowledgeComp──▶|
    |                         |◀─ack ──────────────|
    |                         | (self-vote + peers) |
    |                         | quorum reached      |
    |◀─ poll GetStatus ───────| status = "done"     |
    |──CommitCompaction───────▶|                    |
    | apply new SSTable        |                    |
```

The worker casts a self-vote; each peer it contacts via `AcknowledgeCompaction` adds one vote.  A configurable quorum size (default N/2 + 1) must be reached before the job is marked done.

## Build & run

```bash
make build          # produces bin/node and bin/worker

# Terminal 1 — start the remote compaction worker
./bin/worker --addr :9090 --id worker-1 --workdir /tmp/lsm-worker

# Terminal 2 — start an LSM node backed by the worker
./bin/node \
  --dir /tmp/lsm-data \
  --http :8080 \
  --worker-addr :9090 \
  --id node-1 \
  --quorum 1

# Write and read
curl -X PUT http://localhost:8080/key/hello -d "world"
curl http://localhost:8080/key/hello        # → world
curl -X DELETE http://localhost:8080/key/hello
curl http://localhost:8080/key/hello        # → 404

make demo           # automated smoke test
```

### Without a remote worker (local compaction)

```bash
./bin/node --dir /tmp/lsm-local --http :8080
```

## Tests

```bash
make test           # go test ./... -race -timeout 60s
```

Covers: bloom filter correctness and FPR, SSTable round-trips and tombstones, full LSM put/get/delete, compaction data preservation across restarts, remote worker submit+poll, quorum ack.

## gRPC transport

The project uses `google.golang.org/grpc` with a custom JSON codec registered via `encoding.RegisterCodec`.  This means no `protoc` binary is needed to build — the `.proto` in `proto/` documents the contract.  To switch to binary protobuf, run `make proto` (requires `protoc` + `protoc-gen-go` + `protoc-gen-go-grpc`) and replace the JSON codec registration.
