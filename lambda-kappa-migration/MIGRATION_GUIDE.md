# Migration Playbook: Lambda → Kappa

This guide walks through a safe, validated, zero-downtime migration from Lambda architecture to Kappa architecture.

---

## Prerequisites

Before starting the migration:

- [ ] Python 3.11+ installed
- [ ] Docker and docker-compose installed (for Kafka + LocalStack)
- [ ] `requirements.txt` installed: `pip install -r requirements.txt`
- [ ] Historical data available in `data/historical/` (run `make seed` if not)
- [ ] Kafka is running and healthy (run `make docker-up` and wait for health checks)
- [ ] Sufficient Kafka log retention to cover your full historical window (set `log.retention.bytes=-1` for demo)
- [ ] You have read [ARCHITECTURE.md](ARCHITECTURE.md) and understand the trade-offs

---

## Phase 1: Validate Lambda Architecture

**Goal**: Confirm the existing Lambda system is working correctly before any changes.

```bash
# Start infrastructure
make docker-up

# Generate historical data
make seed

# Run the Lambda demo end to end
python scripts/run_lambda_demo.py

# Verify output: event-type summary, user totals, hourly counts should all be non-zero
```

Expected output: A table showing event counts per type, top users by spend, and hourly buckets.

**Checkpoint**: Lambda batch layer must process all events without errors. Stop here if you see errors.

---

## Phase 2: Stand Up Kappa Alongside Lambda (Dual-Write Period)

**Goal**: Kappa is live and consuming events, but Lambda remains the primary system for query results.

```bash
# Start the Kappa stream processor in the background
# (both systems now receive live events from Kafka)
LOCAL_MODE=false python -c "
from src.kappa_arch.stream_processor import KappaProcessor
p = KappaProcessor(local_mode=False)
p.start_live()
import time; time.sleep(60)  # run for 1 minute to collect live events
p.stop()
print(p.get_results())
"
```

During this phase:
- Lambda batch + speed layer are still primary
- Kappa consumes the same live Kafka topic (`events-live`)
- Do **not** route user-facing queries to Kappa yet

**Duration**: Run dual-write for at least one full batch cycle (e.g., 24 hours in production) to build confidence.

---

## Phase 3: Run Backfill — Replay Historical Data into Kappa

**Goal**: Populate Kappa state with the complete historical dataset so it covers the same window as Lambda's batch view.

```bash
# Trigger backfill: reads data/historical/ → publishes to events-replay Kafka topic
make backfill

# Monitor progress (in a separate terminal)
docker-compose exec kafka kafka-consumer-groups.sh \
  --bootstrap-server localhost:9092 \
  --describe --group kappa-processor
```

The backfill rate is controlled by `BACKFILL_RATE` (default: 1000 events/sec).
For large datasets, set `BACKFILL_RATE=0` to publish as fast as possible.

```bash
BACKFILL_RATE=0 make backfill
```

**Checkpoint**: The backfill script reports `events_published: N` where N matches your historical event count.
Verify with: `python scripts/run_backfill.py --dry-run`

---

## Phase 4: Run Correctness Validator

**Goal**: Prove that Kappa produces results within acceptable tolerance of Lambda's batch results.

```bash
# Run validator — compares Lambda batch results vs Kappa replay results
make validate

# Or with a custom tolerance (1% for amounts instead of default 0.01%)
python scripts/run_validation.py --tolerance 0.01

# Save a JSON report for auditing
python scripts/run_validation.py --output reports/validation_$(date +%Y%m%d).json
```

The validator output shows:
- Per-field comparison: exact match / within tolerance / mismatch
- Delta percentages for amount fields
- Overall PASSED / FAILED verdict

**Checkpoint**: The report must show `Overall: PASSED` before proceeding to cutover.

If the report shows mismatches:
1. Check for data loss during backfill (event count mismatch)
2. Check for floating-point accumulation differences (increase tolerance slightly)
3. Check for missing hour buckets (events arriving out of order during replay)
4. **Do not cut over — see Rollback Plan below**

---

## Phase 5: Cutover — Kappa Becomes Primary

**Goal**: Disable the Lambda batch layer and serving layer. Kappa is now the sole source of truth.

**Step 5.1**: Switch query routing to Kappa

Update your query service / API to read from the Kappa state store instead of the Lambda serving layer.

```python
# Before (Lambda serving layer)
serving = ServingLayer(batch_view, realtime_view)
result = serving.get_user_totals()

# After (Kappa state store)
processor = KappaProcessor()  # already running with full state
result = processor.state.get_user_totals()
```

**Step 5.2**: Stop the Lambda speed layer

```python
speed_layer.stop()
```

**Step 5.3**: Disable the Lambda batch job

Remove or comment out the batch job scheduler (cron, Airflow DAG, etc.). The batch layer is no longer needed.

**Step 5.4**: Decommission the Lambda serving layer

Remove the `ServingLayer` instance and the merge logic.

**Step 5.5**: Verify Kappa is serving queries correctly

```bash
python -c "
from src.kappa_arch.stream_processor import KappaProcessor
p = KappaProcessor()
p.run_replay()  # if starting fresh; skip if state is already warm
results = p.get_results()
print('Users tracked:', len(results['user_totals']))
print('Event types:', list(results['event_type_summary'].keys()))
"
```

**Cutover complete.** Lambda architecture is decommissioned.

---

## Rollback Plan

If validation fails or Kappa produces incorrect results in production:

### Immediate rollback (< 5 minutes)

1. **Restore Lambda as primary** — re-route all queries to the Lambda serving layer
2. Stop the Kappa processor: `processor.stop()`
3. Keep the Lambda speed layer running (it has been consuming live events throughout)
4. Re-run the Lambda serving layer merge to confirm it still produces correct results

```bash
python scripts/run_lambda_demo.py
```

### Root cause analysis

After routing traffic back to Lambda:

1. Compare Kafka consumer group offsets for Lambda and Kappa
2. Re-run the correctness validator with verbose output
3. Check for events that arrived during the cutover window that may have been processed twice or not at all
4. Fix the root cause, re-run Phase 3 (backfill), and repeat Phase 4 (validation)

### Prevention

- Always run Phase 4 validation with a full dataset before cutover
- Keep Lambda speed layer running for at least 24 hours after cutover as a safety net
- Do not delete historical files from `data/historical/` until confidence is established

---

## Monitoring Checklist

After cutover, monitor these signals:

### Kafka

- [ ] Consumer group `kappa-processor` lag stays near zero
- [ ] No consumer rebalances (stable partition assignment)
- [ ] `events-live` topic produce rate matches expected ingest volume
- [ ] No consumer group offset resets (would cause duplicate processing)

### Kappa State Store

- [ ] `state_store.event_count` increments at the expected rate
- [ ] `user_totals` key count matches known user count
- [ ] `hourly_event_counts` has entries for all expected hours
- [ ] Memory usage of state store stays within bounds (add eviction if needed)

### Application

- [ ] Query latency for `get_user_totals()` is acceptable (should be O(1) dict lookup)
- [ ] No `KeyError` or `None` values returned from state store queries
- [ ] Correctness validator continues to pass if re-run against fresh data

### Alerting Recommendations

| Alert | Condition | Severity |
|---|---|---|
| Consumer lag spike | Lag > 10,000 messages for > 5 minutes | Warning |
| Consumer group offline | No heartbeat for > 30 seconds | Critical |
| State store empty | `event_count == 0` after startup | Critical |
| Validation mismatch | Any field mismatch in scheduled validator run | Warning |

---

## Full Automated Migration

The `MigrationRunner` class orchestrates all five phases programmatically:

```python
from src.migration.runner import MigrationRunner

runner = MigrationRunner(local_mode=True)
state = runner.run_full_migration()

print(f"Final phase: {state.phase}")
print(f"Events backfilled: {state.events_backfilled}")
print(f"Validation passed: {state.validation_passed}")
for entry in state.phase_log:
    print(f"  {entry['at']}  {entry['from']} → {entry['to']}  {entry['note']}")
```

Set `skip_validation=True` only in development/testing environments — **never in production**.
