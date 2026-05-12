# Tiered Storage Orchestrator

An intelligent data lifecycle system that automatically moves data across **hot → warm → cold** storage tiers based on access patterns, with transparent read routing, a monthly cost model, and rehydration SLA guarantees.

```
┌─────────────────────────────────────────────────────────────────────┐
│                     TieredStorageOrchestrator                       │
│                                                                     │
│  ┌──────────────┐    ┌──────────────┐    ┌──────────────────────┐  │
│  │   Hot Tier   │    │  Warm Tier   │    │     Cold Tier        │  │
│  │              │    │              │    │                      │  │
│  │ Redis (L1)   │───▶│  Parquet on  │───▶│  gzip archives on    │  │
│  │ Postgres (L2)│    │     S3       │    │  S3 / local fs       │  │
│  │              │    │              │    │                      │  │
│  │ < 10 ms      │    │ 50–200 ms    │    │ SLA-backed restore   │  │
│  └──────────────┘    └──────────────┘    └──────────────────────┘  │
│         ▲                   ▲                        │              │
│         │    promotion      │      rehydration       │              │
│         └───────────────────┴────────────────────────┘              │
│                                                                     │
│  AccessPatternTracker ── LifecycleEngine ── RehydrationManager      │
│  ReadRouter ── CostModel                                            │
└─────────────────────────────────────────────────────────────────────┘
```

## Features

| Feature | Detail |
|---|---|
| **Hot tier** | Redis L1 cache + PostgreSQL L2 persistent store |
| **Warm tier** | Snappy-compressed Parquet objects on S3 |
| **Cold tier** | gzip-compressed JSON archives (S3 Glacier or local) |
| **Access tracking** | Exponential moving average (EMA) of daily access frequency per key |
| **Lifecycle engine** | Background task demotes stale/infrequent keys; enforces size caps |
| **Read routing** | Waterfall: hot → warm → cold; read-through promotion back to hot |
| **Rehydration SLA** | Expedited (5 min) / Standard (5 h) / Bulk (12 h) restore jobs |
| **Cost model** | Monthly USD projections with per-tier breakdowns and savings reports |
| **CLI** | Full `put / get / locate / delete / metrics / cost / lifecycle / rehydrate` |

---

## Quick Start

### 1. Clone & install

```bash
git clone https://github.com/YOUR_USERNAME/tiered-storage-orchestrator.git
cd tiered-storage-orchestrator
python -m venv .venv && source .venv/bin/activate
pip install -e ".[dev]"
```

### 2. Start local services (Redis + Postgres + LocalStack S3)

```bash
docker compose up -d
```

### 3. Copy config

```bash
cp .env.example .env   # edit as needed
```

### 4. Run the demo (no real services required)

```bash
python examples/demo.py
```

### 5. Run tests

```bash
pytest -v
```

---

## Architecture

### Tier definitions

```
Hot   →  Redis (TTL cache) + PostgreSQL (durable)
Warm  →  Parquet files on S3 Standard
Cold  →  gzip JSON on S3 Glacier Flexible Retrieval (or local FS)
```

### Read path (waterfall)

```
GET(key)
  │
  ├─ Hot tier hit?  ──yes──▶  return record  (sub-ms)
  │
  ├─ Warm tier hit? ──yes──▶  return record  (50–200 ms)
  │                            if freq ≥ threshold → promote to hot
  │
  ├─ Cold tier hit? ──yes──▶  enqueue RehydrationJob
  │                            block=True  → wait for SLA window, return record
  │                            block=False → return job handle immediately
  │
  └─ Not found      ──────▶  return None
```

### Lifecycle demotion rules

```
Every N seconds (default: 3 600):

  Hot → Warm  if  idle > hot_to_warm_idle_days (default 7)
              OR  ema_freq < hot_min_access_freq (default 1/day)
              OR  hot_size > hot_max_size_gb (default 10 GB)

  Warm → Cold if  idle > warm_to_cold_idle_days (default 30)
              OR  ema_freq < warm_min_access_freq (default 0.01/day)
              OR  warm_size > warm_max_size_gb (default 500 GB)
```

### Rehydration SLA windows

| Priority | SLA | AWS Glacier equivalent |
|---|---|---|
| `expedited` | 5 minutes | Glacier Expedited |
| `standard` | 5 hours | Glacier Standard |
| `bulk` | 12 hours | Glacier Bulk |

---

## Usage (Python API)

```python
import asyncio
from tiered_storage import TieredStorageOrchestrator, StorageConfig, RehydrationPriority

async def main():
    cfg  = StorageConfig()
    orch = TieredStorageOrchestrator(cfg)
    await orch.start()

    # Write
    await orch.put("user:42", {"name": "Alice", "score": 99})

    # Read (transparent — works regardless of which tier holds the key)
    result = await orch.get("user:42")
    print(result.record.value)   # {"name": "Alice", "score": 99}
    print(result.tier_hit)       # Tier.HOT

    # Trigger lifecycle scan manually
    report = await orch.run_lifecycle_cycle()
    print(report.summary())

    # Monthly cost estimate
    cost = await orch.cost_report(hot_reads_per_day=5000)
    print(cost.summary())

    # Savings report
    print(await orch.savings_report())

    # Rehydrate a cold key
    job = await orch.rehydrate("old_key", priority=RehydrationPriority.EXPEDITED)
    print(f"SLA deadline in {job.eta_seconds:.0f}s")

    await orch.stop()

asyncio.run(main())
```

---

## CLI

```bash
# Write
python cli.py put user:1 '{"name":"Alice"}'

# Read (returns from whichever tier holds it)
python cli.py get user:1

# Show which tier holds a key
python cli.py locate user:1

# Delete from all tiers
python cli.py delete user:1

# Tier metrics
python cli.py metrics

# Monthly cost estimate
python cli.py cost --hot-reads 5000 --warm-reads 500 --cold-reads 20

# Savings from current tier distribution
python cli.py savings

# Manual lifecycle scan
python cli.py lifecycle

# Trigger cold restore
python cli.py rehydrate user:1 --priority expedited --block

# SLA compliance report
python cli.py sla-report
```

---

## Cost Model

Pricing based on AWS `us-east-1` (May 2025, configurable via `CostConfig`):

| Tier | Storage | Notes |
|---|---|---|
| Redis (ElastiCache r7g.large) | ~$10.83/GB/month | Instance-based pricing |
| PostgreSQL (RDS db.t4g.medium + gp3) | $0.115/GB/month | + instance overhead |
| S3 Standard (warm) | $0.023/GB/month | + $0.005/1k PUTs, $0.0004/1k GETs |
| S3 Glacier Flexible (cold) | $0.004/GB/month | + restore fees per GB |

Restore costs per GB:

| Priority | Cost/GB |
|---|---|
| Expedited | $0.03 |
| Standard | $0.01 |
| Bulk | $0.0025 |

```python
from tiered_storage.cost_model import CostModel, CostConfig, TierUsage

model = CostModel(CostConfig())
usage = TierUsage(
    redis_used_gb=5,
    postgres_used_gb=5,
    warm_used_gb=200,
    cold_used_gb=1000,
    warm_reads_per_day=500,
    cold_reads_per_day=10,
)
breakdown = model.monthly_cost(usage)
print(breakdown.summary())
# ── Monthly Cost Estimate ─────────────────────────
#   Hot  (Redis):     $  130.00
#   Hot  (Postgres):  $   61.29
#   Warm (S3 Parquet):$    4.80
#   Cold (Archive):   $    4.00
#   Rehydration:      $    0.03
#   ──────────────────────────
#   TOTAL/month:      $  200.12
```

---

## Configuration

All settings are read from environment variables (see `.env.example`):

| Variable | Default | Description |
|---|---|---|
| `REDIS_URL` | `redis://localhost:6379` | Redis connection URL |
| `POSTGRES_DSN` | `postgresql://postgres:postgres@localhost:5432/tiered_storage` | Postgres DSN |
| `S3_BUCKET` | `tiered-storage-warm` | S3 bucket for warm + cold |
| `S3_ENDPOINT_URL` | _(none)_ | Override for LocalStack / MinIO |
| `COLD_LOCAL_PATH` | _(none)_ | Use local FS for cold tier (dev/test) |
| `LIFECYCLE_INTERVAL_SECONDS` | `3600` | How often lifecycle engine runs |
| `HOT_TO_WARM_IDLE_DAYS` | `7` | Days idle before hot→warm |
| `WARM_TO_COLD_IDLE_DAYS` | `30` | Days idle before warm→cold |
| `HOT_MAX_SIZE_GB` | `10` | Force-demote when hot exceeds this |
| `REHYDRATION_PRIORITY` | `standard` | Default restore priority |
| `PROMOTE_FREQ_THRESHOLD` | `5.0` | Accesses/day to trigger warm→hot promotion |
| `BLOCK_ON_COLD` | `false` | Block GET until cold restore completes |

---

## Project Structure

```
tiered-storage-orchestrator/
├── tiered_storage/
│   ├── orchestrator.py        # Main façade — start here
│   ├── config.py              # Environment-driven config
│   ├── schemas.py             # Dataclasses (DataRecord, CostBreakdown, …)
│   ├── router.py              # Transparent read routing
│   ├── lifecycle.py           # Hot→warm→cold demotion engine
│   ├── rehydration.py         # Cold restore jobs + SLA tracking
│   ├── cost_model.py          # Monthly cost projections
│   ├── tiers/
│   │   ├── hot.py             # Redis + PostgreSQL
│   │   ├── warm.py            # Parquet on S3
│   │   └── cold.py            # gzip archives (S3 / local)
│   └── tracking/
│       └── access_patterns.py # EMA-based per-key frequency tracker
├── tests/
│   ├── conftest.py            # Fake in-process tiers
│   ├── test_orchestrator.py
│   ├── test_routing.py
│   ├── test_lifecycle.py
│   ├── test_rehydration.py
│   ├── test_cost_model.py
│   └── test_access_tracker.py
├── examples/
│   └── demo.py                # Full end-to-end walkthrough (no services needed)
├── cli.py                     # Click CLI
├── docker-compose.yml         # Redis + Postgres + LocalStack
├── pyproject.toml
└── .env.example
```

---

## License

MIT
