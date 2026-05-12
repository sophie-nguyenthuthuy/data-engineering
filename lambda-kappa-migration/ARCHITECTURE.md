# Architecture Reference

## 1. Lambda Architecture

Lambda architecture separates concerns into three independent layers that run concurrently.

```
  Raw Events
  (Kafka / S3 / files)
        │
        ├──────────────────────────────────────────────────────┐
        │                                                      │
        ▼                                                      ▼
┌───────────────────┐                              ┌──────────────────────┐
│   BATCH LAYER     │                              │    SPEED LAYER       │
│                   │                              │                      │
│  Periodic full    │                              │  Kafka consumer;     │
│  recompute from   │                              │  incremental update  │
│  historical store │                              │  of real-time view   │
│  (hours/days old) │                              │  (seconds old)       │
└────────┬──────────┘                              └──────────┬───────────┘
         │                                                    │
         │  BatchView                                         │  RealTimeView
         │  (immutable snapshot)                              │  (mutable, in-memory)
         │                                                    │
         └──────────────────┬─────────────────────────────────┘
                            │
                            ▼
                 ┌─────────────────────┐
                 │   SERVING LAYER     │
                 │                     │
                 │  query(user_totals) │
                 │  = batch_view       │
                 │  + realtime_view    │
                 │                     │
                 │  Merges on read;    │
                 │  always fresh       │
                 └─────────────────────┘
```

### Data Flow

1. Events are written to both S3 (for batch) and Kafka (for speed layer)
2. The **batch layer** periodically reads all historical data and recomputes views from scratch
3. The **speed layer** consumes Kafka and maintains a delta view covering the gap since the last batch run
4. The **serving layer** merges both views: `result = batch_view + realtime_delta`

---

## 2. Kappa Architecture

Kappa eliminates the batch layer. Everything flows through a single stream processor.

```
  Historical Data         Live Events
  (local files / S3)      (Kafka topic: events-live)
        │                        │
        │                        │
        ▼                        │
┌──────────────────┐             │
│  REPLAY MANAGER  │             │
│                  │             │
│  Reads files →   │             │
│  publishes to    │             │
│  events-replay   │             │
└────────┬─────────┘             │
         │                       │
         ▼                       ▼
         └──────────┬────────────┘
                    │  (unified topic)
                    ▼
         ┌─────────────────────────┐
         │   KAPPA STREAM          │
         │   PROCESSOR             │
         │                         │
         │  mode: REPLAY → LIVE    │
         │                         │
         │  Applies every event    │
         │  to the same            │
         │  aggregation code path  │
         └───────────┬─────────────┘
                     │
                     ▼
          ┌──────────────────────┐
          │     STATE STORE      │
          │                      │
          │  hourly_event_counts │
          │  user_totals         │
          │  event_type_summary  │
          │                      │
          │  (in-memory dict;    │
          │   optional Redis)    │
          └──────────────────────┘
```

### Data Flow

1. On startup the **Replay Manager** reads all historical files and publishes them (in chronological order) to the replay Kafka topic
2. The **stream processor** consumes the replay topic in `REPLAY` mode, building state from scratch
3. Once replay is complete the processor switches to `LIVE` mode and consumes the live events topic
4. There is **no merge step** — the state store is the single source of truth at all times

---

## 3. Lambda vs Kappa Trade-off Table

| Dimension | Lambda | Kappa |
|---|---|---|
| **Operational complexity** | High — two separate code paths (batch + stream) to maintain, test, and deploy | Low — single unified streaming pipeline |
| **End-to-end latency** | Batch results are hours-to-days stale; real-time view fills the gap | Seconds to minutes; historical replay achieves parity quickly |
| **Consistency** | Batch view and real-time view can diverge; merge logic required | Single consistent view; no divergence possible |
| **Replay / backfill** | Requires re-running the full batch job (expensive, slow) | Replay is native: re-publish historical events and re-consume |
| **Code duplication** | Same aggregations implemented twice (batch + stream) | One implementation driven by the same event stream |
| **Fault tolerance** | Batch layer provides high-accuracy recovery; speed layer can be restarted | Stream processor must be fault-tolerant; state must be checkpointed |
| **Infrastructure cost** | High — batch cluster (Spark/EMR) + streaming cluster (Kafka) | Lower — streaming only; batch cluster eliminated |
| **Query simplicity** | Serving layer must merge two views on every query | State store is the answer; no merge required |
| **Historical accuracy** | Batch layer recomputes from raw data; very high accuracy | Replay must reproduce exact event ordering; accuracy depends on replay fidelity |
| **Technology lock-in** | Separate tools per layer (Spark, Flink/Kafka Streams) | Single streaming framework throughout |
| **Debugging** | Bugs must be traced across both layers | Simpler: single code path to trace |

---

## 4. When to Choose Lambda vs Kappa

### Choose Lambda when:

- You have **existing batch infrastructure** (Spark, Hadoop) that is deeply integrated
- Your **batch layer must produce ML training datasets** that require global joins impossible in streaming
- Data arrives with **very late (hours-to-days) arrival patterns** that streaming windows cannot accommodate
- Your team has **separate batch and streaming specialists** and the organisational split maps to the architecture
- You need **SQL-on-history** queries where ad-hoc Spark SQL is more practical than stream SQL

### Choose Kappa when:

- You want a **single codebase** for both historical processing and real-time serving
- Your **event stream is the system of record** and you can replay it at will (Kafka retention ≥ your lookback window)
- **Operational simplicity** is a priority — fewer moving parts, fewer failure modes
- Your aggregations are **incremental** (sums, counts, averages) without global joins that require a full scan
- You are **building from scratch** and have no legacy batch infrastructure to preserve

---

## 5. Migration Strategy

The migration from Lambda to Kappa is a five-phase process:

```
Phase 1: Lambda is sole primary system
         ↓
Phase 2: Kappa is stood up alongside Lambda (dual-write)
         Both systems ingest live events; neither is primary for queries
         ↓
Phase 3: Backfill — replay all historical events through Kappa
         Kappa state now covers the full historical window
         ↓
Phase 4: Correctness validation
         Run both systems on identical data; compare aggregations within tolerance
         If validation fails → rollback (see MIGRATION_GUIDE.md)
         ↓
Phase 5: Cutover
         Lambda batch layer disabled; Lambda serving layer decommissioned
         Kappa is now sole primary system
```

The **correctness validator** is the gate between Phase 4 and Phase 5. It compares:

- Count fields: exact match required (zero tolerance)
- Amount / average fields: ≤ 0.01% relative difference allowed

See [MIGRATION_GUIDE.md](MIGRATION_GUIDE.md) for the detailed step-by-step playbook.
