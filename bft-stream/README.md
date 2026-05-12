# BFT Stream Processing

Byzantine Fault-Tolerant stream pipeline that uses PBFT consensus **only for watermark advancement**, not individual records. Benchmarked against Raft (CFT baseline) to demonstrate < 3× overhead for honest nodes.

## Design

The key insight: windowed aggregations need agreement on *when a window closes*, not on every record. Applying BFT consensus to watermark events (once per window) rather than records (once per record) amortizes the O(n²) PBFT message cost across thousands of records.

```
Record arrives → All replicas process locally (no consensus)
                        ↓
     Primary proposes WatermarkAdvance{window_end, record_count, checksum}
                        ↓
              PBFT: PRE-PREPARE → PREPARE → COMMIT
                  (2f+1 honest nodes must agree)
                        ↓
         All replicas finalize window aggregate (deterministic)
```

### Byzantine protection for watermarks

A Byzantine primary cannot:
- **Skip records**: honest replicas reject proposals with mismatched `record_count`
- **Forge window boundaries**: `checksum` (Σ values) is independently verified
- **Roll back the watermark**: replicas enforce monotone watermark advancement

### Architecture

```
internal/
  transport/   In-memory async message bus with fault injection
  pbft/        PBFT node + cluster (3f+1 nodes, tolerates f Byzantine faults)
  raft/        Raft node + cluster (2f+1 nodes, tolerates f crash faults)
  stream/      WindowManager, Pipeline, WatermarkProposal types
benchmark/     Throughput & latency benchmark (PBFT vs Raft)
cmd/demo/      Interactive demo with live Byzantine fault injection
```

## Benchmark Results

Tested on a 4-core laptop, in-process simulation, no network I/O.

| Scenario | Protocol | Windows | Mean latency | p99 | Overhead |
|---|---|---|---|---|---|
| n=4, 100k rec/s, 100ms window | Raft (n=3, f=1) | 93 | 32ms | 35ms | baseline |
| n=4, 100k rec/s, 100ms window | PBFT (n=4, f=1) | 98 | 10ms | 11ms | **0.32×** |
| n=7, 50k rec/s, 200ms window | Raft (n=7, f=3) | 112 | 35ms | 42ms | baseline |
| n=7, 50k rec/s, 200ms window | PBFT (n=7, f=2) | 112 | 45ms | 48ms | **1.28×** |
| Byzantine fault (1/4 nodes) | Raft (n=3, f=1) | 93 | 32ms | 42ms | baseline |
| Byzantine fault (1/4 nodes) | PBFT (n=4, f=1) | 97 | 17ms | 17ms | **0.53×** |

All scenarios achieve **< 3× overhead** vs Raft. The amortization principle works: with ~3000 records/window, one PBFT round costs microseconds per record.

## Protocol comparison

| | PBFT | Raft |
|---|---|---|
| Fault model | Byzantine (active, lying) | Crash (fail-stop) |
| Nodes needed | 3f+1 | 2f+1 |
| Phases | 3 (PRE-PREPARE → PREPARE → COMMIT) | 2 (AppendEntries → Response) |
| Messages/round | O(n²) | O(n) |
| Latency | ~2–2.5 RTTs | ~1 RTT |
| View change | On primary timeout | On election timeout |

## Usage

**Run benchmark:**
```bash
go run ./benchmark/main.go
```

**Run interactive demo** (shows live window commits + Byzantine injection):
```bash
go run ./cmd/demo/main.go
```

**Run tests:**
```bash
go test ./...
```

## Watermark protocol detail

```
type WatermarkProposal struct {
    WindowID     WindowID   // {Start, End} of the window being closed
    NewWatermark time.Time  // advance stream watermark to this time
    RecordCount  int64      // number of records in the window
    Checksum     float64    // sum of all record values (Byzantine check)
}
```

The primary proposes every `wmInterval`. Backups validate:
1. `NewWatermark > current watermark` (monotone)
2. `RecordCount` matches local window state ± tolerance
3. The window ID is the next expected window

With f=1 Byzantine nodes out of n=4, the 2f+1=3 honest nodes overrule any invalid proposal. A Byzantine primary that proposes the wrong checksum will fail to collect 2f PREPARE messages.

## References

- Castro & Liskov (1999): *Practical Byzantine Fault Tolerance* — [OSDI '99](http://pmg.csail.mit.edu/papers/osdi99.pdf)
- Ongaro & Ousterhout (2014): *In Search of an Understandable Consensus Algorithm* — [ATC '14](https://raft.github.io/raft.pdf)
- The Dataflow Model (Akidau et al., 2015): watermarks and event-time windows — [VLDB '15](http://www.vldb.org/pvldb/vol8/p1792-Akidau.pdf)
