# Exactly-Once Cross-System Transaction Pipeline

A production-grade demonstration of how to write a payment event atomically to **Kafka**, a **Postgres ledger**, a **data warehouse**, and a **notification queue** — with full failure recovery.

## Architecture

```
                     ┌──────────────────────────────────────────────────────┐
                     │                 PAYMENT SERVICE                       │
                     │  Single Postgres transaction writes:                  │
                     │    1. ledger row     (source of truth)                │
                     │    2. outbox row     (relay trigger)                  │
                     │    3. coordinator row (saga state)                    │
                     └────────────────────┬─────────────────────────────────┘
                                          │ committed atomically
                            ┌─────────────▼──────────────┐
                            │       OUTBOX TABLE          │
                            │  (Postgres, unpublished)    │
                            └─────────────┬───────────────┘
                                          │ polled by
                            ┌─────────────▼───────────────┐
                            │       OUTBOX RELAY           │
                            │  Kafka transactions          │
                            │  (transactional.id)          │
                            └──┬──────────────────────┬───┘
                               │ exactly-once publish  │
                 ┌─────────────▼──────┐    ┌──────────▼──────────────┐
                 │  WAREHOUSE         │    │  NOTIFICATION            │
                 │  CONSUMER          │    │  CONSUMER                │
                 │  idempotency guard │    │  idempotency guard       │
                 │  → warehouse table │    │  → notification_log      │
                 └──────────┬─────────┘    │  → Redis queue           │
                            │              └───────────┬──────────────┘
                            │                          │
                 ┌──────────▼──────────────────────────▼──────────────┐
                 │          DISTRIBUTED TRANSACTION COORDINATOR        │
                 │  Tracks: kafka_published / warehouse_ack /          │
                 │          notification_ack  →  COMPLETED             │
                 │  On failure: retry (transient) or compensate        │
                 └────────────────────────────────────────────────────┘
```

## Patterns Implemented

| Pattern | Where | What it guarantees |
|---|---|---|
| **Outbox Pattern** | `payment_service.py` + `outbox_poller.py` | Ledger write and Kafka publish are atomic |
| **Idempotency Keys** | `idempotency_log` table | No event processed more than once per consumer |
| **Kafka Transactions** | `outbox_poller.py` | Exactly-once publish (no duplicate messages on broker restart) |
| **`read_committed` consumers** | `base_consumer.py` | Consumers skip aborted Kafka transactions |
| **Saga / Coordinator** | `coordinator/transaction_coordinator.py` | Distributed step tracking + compensation |
| **Dead Letter Queue** | `base_consumer.py` | Permanently failed messages captured for analysis |
| **Recovery Agent** | `coordinator/recovery_agent.py` | Detects stuck sagas, resets outbox, triggers compensation |

## Failure Recovery

Each failure scenario is handled differently:

```
Step fails               How recovery works
─────────────────────    ───────────────────────────────────────────────────
Outbox poller crash      outbox.published_at stays NULL → relay retries on restart
                         Kafka's transactional.id deduplicates the re-publish

Kafka publish error      abort_transaction() → outbox entry increments retry_count
                         Next poll cycle republishes; consumers never see partial batch

Warehouse consumer crash Consumer offset not committed → Kafka re-delivers message
                         idempotency_log check skips duplicates transparently

Notification crash       Same as warehouse: re-delivery + idempotency guard

All retries exhausted    RecoveryAgent calls coordinator.compensate()
                         Ledger row marked COMPENSATED; saga enters FAILED
```

## Quick Start

```bash
# 1. Start infrastructure
make up

# 2. Install dependencies
make install

# 3. Run the happy path demo (5 payments)
make demo

# 4. Inject failures and watch recovery
make fail-kafka        # Kafka publish fails ~50% → retries to completion
make fail-warehouse    # Warehouse consumer fails ~50% → retries
make fail-notification # Notification fails ~50% → retries

# 5. Run all failure scenarios sequentially
make simulate

# 6. Run integration tests (requires stack to be up)
make test
```

## Project Structure

```
exactly-once-pipeline/
├── docker-compose.yml              Kafka, ZooKeeper, Postgres, Redis, Kafka UI
├── sql/001_schema.sql              All table definitions
├── src/
│   ├── config.py                   Settings (env-driven)
│   ├── models.py                   Pydantic data models
│   ├── db.py                       Postgres connection pool + transaction helper
│   ├── payment_service.py          Atomic ledger + outbox + coordinator write
│   ├── outbox_poller.py            Transactional Kafka relay
│   ├── consumers/
│   │   ├── base_consumer.py        Idempotency guard, DLQ, retry logic
│   │   ├── warehouse_consumer.py   Writes to warehouse_payments
│   │   └── notification_consumer.py Writes to notification_log + Redis
│   ├── coordinator/
│   │   ├── transaction_coordinator.py Saga state machine
│   │   └── recovery_agent.py       Detects and recovers stuck sagas
│   └── recovery/
│       └── failure_injector.py     Deterministic/random failure injection
├── scripts/
│   ├── run_demo.py                 Interactive demo with status table
│   └── simulate_failures.py       Automated failure scenario runner
└── tests/
    ├── conftest.py
    ├── test_exactly_once.py        Atomicity, idempotency, coordinator tests
    └── test_recovery.py            Failure injection + recovery tests
```

## Kafka UI

After `make up`, visit [http://localhost:8080](http://localhost:8080) to browse topics, consumer groups, and message offsets in real time.

## Key Design Decisions

**Why Outbox instead of dual-write?**  
Dual-write (write to DB then immediately publish to Kafka) has a window where the process can crash between the two writes, leaving them out of sync. The outbox collapses both into one atomic Postgres transaction, then relays asynchronously.

**Why Kafka transactions on the poller?**  
Without `transactional.id`, a poller crash after `flush()` but before the `UPDATE outbox SET published_at` commit would republish the same batch. Kafka transactions make the publish idempotent at the broker level; the consumer's `read_committed` isolation level filters out aborted batches.

**Why idempotency keys in every consumer?**  
Kafka guarantees at-least-once delivery within a consumer group. Idempotency keys in `idempotency_log` give the final line of defence: even if a message is re-delivered the consumer silently skips it.

**Why a Saga instead of 2PC?**  
Two-phase commit requires all participants to hold locks simultaneously, which creates availability problems across heterogeneous systems (Kafka, Postgres, Redis). The Saga pattern keeps each step autonomous and uses compensating transactions to undo partial work on failure.
