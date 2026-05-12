# lsm-ts — LSM-Tree Time-Series Storage Engine

A from-scratch implementation of a Log-Structured Merge-Tree (LSM-tree) storage engine, tuned for append-heavy time-series workloads (IoT ingestion, metrics, sensor data).

This is the same internal architecture used by **ClickHouse**, **Cassandra**, **LevelDB**, and **RocksDB** — built here from first principles so every design decision is visible.

---

## Architecture

```
Write path
──────────
  put(key, value)
       │
       ▼
  WAL (wal.log)          ← crash recovery
       │
       ▼
  Memtable               ← sorted in-memory buffer (SortedDict)
       │  (when full, ~64 MB)
       ▼
  L0 SSTable             ← immutable, may overlap in key range
       │  (when ≥4 L0 files)
       ▼
  L1 SSTable             ← non-overlapping, leveled compaction
       │  (when L1 > 10 MB)
       ▼
  L2 … L6 SSTables       ← each level 10× larger than previous

Read path
─────────
  get(key)
    → Memtable  →  Immutable memtables  →  L0..L6 SSTables (bloom filter first)
```

### Key Components

| Component | File | What it does |
|-----------|------|--------------|
| **TSKey** | `lsm/types.py` | Encodes `metric + tags + timestamp_ns` into a lexicographically sortable byte string |
| **BloomFilter** | `lsm/bloom.py` | Probabilistic set membership; avoids disk reads for absent keys |
| **WAL** | `lsm/wal.py` | Append-only write-ahead log with CRC32 per record |
| **Memtable** | `lsm/memtable.py` | In-memory sorted buffer backed by `SortedDict` |
| **SSTable** | `lsm/sstable.py` | Immutable on-disk sorted file with 64KB data blocks, bloom filter, and binary-search index |
| **Compaction** | `lsm/compaction.py` | Leveled compaction; k-way merge of sorted iterators |
| **LSMEngine** | `lsm/engine.py` | Orchestrates all components; public API |

### SSTable Binary Format

```
[Data Blocks]          64 KB target, lz4-compressed, CRC32 per block
[Bloom Filter Block]   serialized bit array
[Index Block]          (first_key, offset, size) per data block
[Footer 40B]           pointers to index + bloom + magic "LSMTFOOT"
```

### Key Encoding

```
{metric}\x00{tag_k1=v1,...}\x00{timestamp_ns as 8-byte big-endian}
```

Big-endian timestamp ensures timestamps sort correctly as raw bytes, enabling efficient range scans.

---

## Quick Start

```bash
# Install
pip install -e ".[dev]"

# Run tests
pytest

# Run benchmark (100k IoT points)
python -m benchmarks.bench_lsm --points 100000

# Larger benchmark
python -m benchmarks.bench_lsm --points 1000000 --no-wal
```

### Basic usage

```python
from lsm import LSMEngine, TSKey, TSValue, DataPoint

with LSMEngine("./data") as eng:
    # Write a single point
    key = TSKey.make("temperature", {"facility": "plant-A", "machine": "m001"}, ts_ns)
    eng.put(key, TSValue(value=23.5))

    # Bulk write (faster)
    points = [DataPoint(key=..., value=...) for ...]
    eng.write_batch(points)

    # Point lookup
    val = eng.get(key)

    # Range scan: all temperature readings from machine m001 in a 1-hour window
    for point in eng.scan("temperature", {"facility": "plant-A", "machine": "m001"},
                           start_ns, end_ns):
        print(point.key.timestamp_ns, point.value.value)
```

---

## Benchmark Results

Typical numbers on an M-series MacBook Pro (100k points, lz4 on):

| Metric | Value |
|--------|-------|
| Write throughput | ~200k–400k pts/sec |
| Point lookup p50 | ~50–200 µs |
| Point lookup p99 | ~500 µs–2 ms |
| Range scan (1h, 1 device) | ~1–5 ms |
| Storage (vs raw bytes) | ~1.2–2× write amplification |

---

## Comparison with Production Databases

| Feature | lsm-ts | InfluxDB OSS | TimescaleDB |
|---------|--------|-------------|-------------|
| Storage model | LSM-tree | TSM (variant of LSM) | B-tree (PostgreSQL) |
| Compression | lz4 per block | Gorilla + zstd | PostgreSQL TOAST |
| Bloom filters | Yes | Yes | No (index scan) |
| Compaction | Leveled | Time-partitioned | Chunk-based |
| WAL | Yes (CRC32) | Yes | Yes (PostgreSQL WAL) |
| Query language | Python API | Flux / InfluxQL | SQL |
| Cluster support | No | Enterprise | Yes |

**When to use what:**
- **lsm-ts**: Learning, embedding, custom pipelines, understanding internals
- **InfluxDB**: Simple metrics/monitoring, Grafana integration
- **TimescaleDB**: Need SQL, complex joins, existing Postgres infrastructure

---

## Design Decisions & Trade-offs

**Why big-endian timestamps?** Byte-order matters for lexicographic sorting. Big-endian stores the most-significant byte first, so raw `memcmp` on the encoded key gives correct temporal ordering — no special comparator needed.

**Why Leveled compaction (not Size-Tiered)?** Leveled gives better read performance and lower space amplification. Size-tiered is better for pure write throughput (Cassandra's default). We chose leveled because time-series workloads read recent windows frequently.

**Why lz4 over zstd/snappy?** lz4 has the best compression/decompression speed trade-off for hot data. zstd gives ~30% better ratios but 2× slower decompression — worth it for cold tiers.

**Why not mmap?** `mmap` simplifies random reads but makes eviction policy invisible. Explicit `pread` calls give clearer control over I/O and are easier to reason about in this educational context.

**Bloom filter placement:** Stored per-SSTable (not per-level) so we can skip individual files without reading their index. False-positive rate target is 1% — tunable via `BloomFilter(capacity, fpr=...)`.

---

## Project Structure

```
lsm-ts/
├── lsm/
│   ├── __init__.py
│   ├── types.py        # TSKey, TSValue, DataPoint + binary encoding
│   ├── bloom.py        # Bloom filter (double hashing)
│   ├── wal.py          # Write-ahead log (CRC32, crash recovery)
│   ├── memtable.py     # In-memory sorted buffer
│   ├── sstable.py      # SSTable writer + reader (blocks, index, bloom)
│   ├── compaction.py   # Leveled compaction + k-way merge
│   └── engine.py       # Public LSMEngine API
├── benchmarks/
│   ├── workload.py     # IoT data generator (60 sensors × 6 metrics)
│   └── bench_lsm.py    # Throughput, latency, compaction benchmarks
├── tests/
│   ├── test_bloom.py
│   ├── test_wal.py
│   ├── test_memtable.py
│   ├── test_sstable.py
│   └── test_engine.py
├── pyproject.toml
└── README.md
```

---

## Further Reading

- [LevelDB implementation notes](https://github.com/google/leveldb/blob/main/doc/impl.md) — the blueprint for modern LSM-trees
- [The Log-Structured Merge-Tree (O'Neil et al., 1996)](https://www.cs.umb.edu/~poneil/lsmtree.pdf) — original paper
- [RocksDB tuning guide](https://github.com/facebook/rocksdb/wiki/RocksDB-Tuning-Guide) — production-level knobs
- [Designing Data-Intensive Applications](https://dataintensive.net/) ch. 3 — best accessible explanation of LSM vs B-tree trade-offs
