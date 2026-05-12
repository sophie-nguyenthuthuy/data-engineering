# tla-verified-pipeline

A **TLA+ verified data pipeline** — full spec of a CDC → Kafka → aggregation → warehouse → reverse-ETL flow, TLC-model-checked for safety (no data loss) and liveness (every record eventually delivered). A **runtime monitor** instantiates the same state machine and flags violations on live traffic. Runtime verification applied to data engineering.

> **Status:** Design / spec phase.

## Why

We know our pipelines have bugs we can't reproduce. We know the design has properties we believe but can't prove. TLA+ lets us specify both, model-check the design once, then *use the same spec* as a runtime oracle.

## Architecture

```
            ┌──────────────────────────────────────────────────┐
            │              Production pipeline                 │
            │                                                  │
            │  Postgres ─▶ Debezium ─▶ Kafka ─▶ Flink ─▶ DW    │
            │                                       │          │
            │                                       ▼          │
            │                                  Reverse ETL     │
            └──────────────────────────────────────────────────┘
                                  │
                ┌─────────────────┼─────────────────┐
                │                 │                 │
            event log         event log         event log
                │                 │                 │
                ▼                 ▼                 ▼
            ┌──────────────────────────────────────────────┐
            │           Runtime monitor                    │
            │     (re-plays events through TLA+ state)     │
            │                                              │
            │  - same Init / Next as design spec           │
            │  - checks invariants on every step           │
            │  - alerts on violation                       │
            └──────────────────────────────────────────────┘
```

## Components

| Path | Role |
|---|---|
| `spec/pipeline.tla` | Full TLA+ spec of the pipeline |
| `spec/properties.tla` | Safety + liveness properties |
| `spec/MCPipeline.cfg` | TLC config for model checking |
| `src/monitor/replay.py` | Reads event logs, drives TLA+ state machine |
| `src/monitor/invariants.py` | Python mirror of TLA+ properties — runs on every step |
| `src/monitor/alerts.py` | Violation → incident |
| `src/connectors/` | Adapters: read Debezium, Kafka, warehouse change logs |

## TLA+ spec (sketch)

```tla
---------------------------- MODULE pipeline ----------------------------
EXTENDS Naturals, Sequences, FiniteSets

CONSTANTS Records, Kafka_MaxLag

VARIABLES pg, kafka, flink_state, warehouse, rev_etl_target

Init ==
  /\ pg = {}
  /\ kafka = <<>>
  /\ flink_state = [k \in {} |-> 0]
  /\ warehouse = {}
  /\ rev_etl_target = {}

PgInsert(r) ==
  /\ r \notin pg
  /\ pg' = pg \cup {r}
  /\ UNCHANGED <<kafka, flink_state, warehouse, rev_etl_target>>

DebeziumPublish ==
  /\ \E r \in pg : r \notin Range(kafka)
       /\ kafka' = Append(kafka, r)
  /\ UNCHANGED <<pg, flink_state, warehouse, rev_etl_target>>

FlinkConsume ==
  /\ Len(kafka) > 0
  /\ LET head == Head(kafka)
     IN  flink_state' = [flink_state EXCEPT ![head.key] = @ + head.delta]
  /\ kafka' = Tail(kafka)
  /\ UNCHANGED <<pg, warehouse, rev_etl_target>>

WarehouseLoad == ...
ReverseETL    == ...

Next == PgInsert(...) \/ DebeziumPublish \/ FlinkConsume \/ ...

\* ---- Safety ----
NoDataLoss == \A r \in pg : eventually(r \in warehouse)
ExactlyOnceInAgg == \A k : flink_state[k] = SUM(records_for_key_in_pg(k))

\* ---- Liveness ----
EventualDelivery == \A r \in pg : <>(r \in rev_etl_target)
=========================================================================
```

## Runtime monitor

The same `Init` / `Next` actions are mirrored in Python. The monitor:

1. Subscribes to the change logs of every stage (PG WAL, Kafka topic, Flink checkpoint, DW transaction log, rev-ETL ack log).
2. Reconstructs the global state after every event.
3. Checks `NoDataLoss` and `ExactlyOnceInAgg` after every state transition.
4. Fires an incident with the offending state snapshot when an invariant fails.

Throughput: the monitor lags real traffic by seconds but doesn't gate it. Pure observer.

## Properties checked

**Safety (every state must satisfy):**
- `NoDataLoss` — every PG record eventually in warehouse
- `ExactlyOnceInAgg` — Flink aggregate equals replayed sum from PG
- `MonotoneOffsets` — Kafka offsets never go backward
- `IdempotentRevETL` — same record delivered ≤ 1 time

**Liveness (eventually):**
- `EventualDelivery` — every PG record reaches reverse-ETL target
- `BoundedLag` — Kafka lag stays below `Kafka_MaxLag` infinitely often

## References

- Lamport, *Specifying Systems* (2002)
- Newcombe et al., "How Amazon Web Services Uses Formal Methods" (CACM 2015)
- Havelund et al., "Runtime Verification: Past Experiences and Future Projections" (2018)

## Roadmap

- [ ] TLA+ spec for each stage
- [ ] Composite spec for full pipeline
- [ ] TLC model check with bounded record count
- [ ] Python state-machine mirror
- [ ] Change-log connectors for each stage
- [ ] Invariant checker + alerter
- [ ] Replay harness against historical logs
