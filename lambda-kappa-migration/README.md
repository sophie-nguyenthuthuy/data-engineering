# lambda-kappa-migration

A data engineering showcase project demonstrating the migration from **Lambda architecture** (batch + speed layers) to **Kappa architecture** (unified stream processing), including correctness validation tooling to prove equivalence.

---

## Project Goals

1. Illustrate the practical differences between Lambda and Kappa architectures on a real aggregation workload
2. Provide production-ready migration tooling (backfill, dual-write, cutover)
3. Include a correctness validator that proves Kappa results match Lambda results on the same dataset
4. Make everything runnable locally without Docker (using a local-file Kafka mock)

---

## Quick Start (no Kafka, no Docker — 5 commands)

```bash
# 1. Clone and enter the project
git clone https://github.com/<your-org>/lambda-kappa-migration.git
cd lambda-kappa-migration

# 2. Install dependencies
pip install -r requirements.txt

# 3. Generate 10,000 synthetic events across 7 days
make seed

# 4. Run the Lambda architecture demo (batch + serving layer)
make lambda-demo

# 5. Run the Kappa architecture demo (unified stream replay)
make kappa-demo
```

All demos run in `LOCAL_MODE=true`, which substitutes the local filesystem for Kafka and S3. No external services required.

To run the correctness validator (proves Lambda == Kappa):

```bash
make validate
```

---

## Full Demo with Kafka + LocalStack (Docker)

```bash
# Start the infrastructure
make docker-up

# Seed and run both architectures against live Kafka
make seed
python scripts/run_lambda_demo.py
python scripts/run_kappa_demo.py

# Trigger the historical backfill into Kafka
make backfill

# Run validation against Kafka results
make validate-kafka

# Tear down
make docker-down
```

---

## Architecture Overview

### Lambda Architecture

Three layers process data independently:

| Layer | Role |
|---|---|
| **Batch** | Periodically recomputes aggregates from all historical data (high latency, high accuracy) |
| **Speed** | Kafka consumer applies each new event incrementally (low latency, eventual consistency) |
| **Serving** | Merges batch view + real-time view to answer queries |

### Kappa Architecture

A single stream processor handles everything:

- Historical data is **replayed** through Kafka to bootstrap state
- Live events flow through the same consumer code path
- No dual code paths, no merge logic, no batch jobs

See [ARCHITECTURE.md](ARCHITECTURE.md) for detailed diagrams and trade-off analysis.

---

## Aggregations Computed

Both architectures compute the same three views:

| View | Description |
|---|---|
| `hourly_event_counts` | `{hour_bucket: {event_type: count}}` |
| `user_totals` | `{user_id: {total_amount: float, event_count: int}}` |
| `event_type_summary` | `{event_type: {count: int, total_amount: float, avg_amount: float}}` |

---

## Project Structure

```
src/
  lambda_arch/        # Batch layer, speed layer, serving layer
  kappa_arch/         # Unified stream processor, replay manager, state store
  migration/          # Backfill job and migration runner
  validator/          # Correctness validator, tolerance checker, report
scripts/              # Runnable entry points
tests/                # pytest test suite (self-contained, no Kafka needed)
data/historical/      # Seeded JSON event files
```

---

## Makefile Targets

| Target | Description |
|---|---|
| `make setup` | Install Python dependencies |
| `make seed` | Generate synthetic historical events |
| `make lambda-demo` | Seed + run Lambda demo (local mode) |
| `make kappa-demo` | Seed + run Kappa demo (local mode) |
| `make backfill` | Run backfill to Kafka (requires Docker stack) |
| `make validate` | Run correctness validator (local mode) |
| `make docker-up` | Start Kafka + Zookeeper + LocalStack |
| `make docker-down` | Stop and remove Docker containers |
| `make test` | Run the full pytest suite |

---

## Environment Variables

| Variable | Default | Description |
|---|---|---|
| `LOCAL_MODE` | `false` | Use local JSONL file instead of Kafka |
| `LOG_LEVEL` | `INFO` | Python logging level |
| `BACKFILL_RATE` | `1000` | Events per second during backfill (0 = unlimited) |

---

## Running the Tests

```bash
# All tests are self-contained — no Kafka or Docker required
make test

# With coverage
make test-cov
```

---

## Further Reading

- [ARCHITECTURE.md](ARCHITECTURE.md) — detailed ASCII diagrams, trade-off table, and architecture decision rationale
- [MIGRATION_GUIDE.md](MIGRATION_GUIDE.md) — step-by-step migration playbook including rollback plan
