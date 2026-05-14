# multi-tenant-ingestion-platform

A small, single-process self-service ingestion platform. Each team
registers as a tenant with a resource quota; each tenant registers
their own data sources; admission control rejects over-quota jobs and
a Deficit-Round-Robin scheduler shares compute fairly across tenants.

[![Python](https://img.shields.io/badge/python-3.10%20%7C%203.11%20%7C%203.12-blue.svg)](#)
[![License: MIT](https://img.shields.io/badge/license-MIT-green.svg)](LICENSE)

## Components

| Module                        | Role                                                    |
| ----------------------------- | ------------------------------------------------------- |
| `mtip.registry.tenant`        | `Tenant`, `TenantRegistry` (id pattern enforced)        |
| `mtip.registry.source`        | `SourceSpec`, `SourceRegistry` (per-tenant scoping)     |
| `mtip.quota`                  | `ResourceQuota`, `ResourceUsage` (RLock-guarded)        |
| `mtip.isolation.storage`      | `StorageNamespace` (rejects abs paths + traversal)      |
| `mtip.isolation.compute`      | `ComputeSlots` (tenant-tagged bounded pool)             |
| `mtip.admission`              | `AdmissionController` → `Decision`                      |
| `mtip.scheduler`              | `FairScheduler` — Deficit Round Robin (Shreedhar 1995)  |
| `mtip.platform`               | One-stop `Platform` facade                              |
| `mtip.cli`                    | `mtipctl info | demo`                                  |

## Install

```bash
pip install -e ".[dev]"
```

Python 3.10+. **Zero runtime dependencies.**

## Library

```python
from mtip.platform        import Platform
from mtip.quota           import ResourceQuota
from mtip.registry.tenant import Tenant

plat = Platform()

plat.register_tenant(Tenant(
    id="team-orders",
    display_name="Orders Team",
    quota=ResourceQuota(cpu_cores=4.0, storage_gb=500.0, ingestion_qps=200.0),
))
plat.register_source("team-orders", "shopify-orders", "http_api",
                     {"url": "https://api.shopify.com/orders"})

decision = plat.submit_job("team-orders", "ingest-2026-05-13",
                            cpu=1.0, storage_gb=10.0, qps=20.0)
# decision is one of: ADMIT / REJECT_UNKNOWN_TENANT / REJECT_OVER_CPU /
#                     REJECT_OVER_STORAGE / REJECT_OVER_QPS

dispatched = plat.scheduler.schedule(n=10)
```

## CLI

```bash
mtipctl info
mtipctl demo --tenants 3 --jobs 4
```

## Isolation rules

- **Storage**: `StorageNamespace(tenant_id).resolve("a/b/c")` returns
  `tenants/<tenant_id>/a/b/c`; the resolver refuses absolute paths and
  `..` traversal segments, so no tenant can address another's
  namespace.
- **Compute**: `ComputeSlots` is one bounded pool tagged with the
  acquiring tenant. The scheduler reports per-tenant usage so the
  admission controller can reject before the pool is exhausted.

## Fair scheduling

`FairScheduler` implements weighted **Deficit Round Robin** (Shreedhar
& Varghese, SIGCOMM 1995). Each tenant has a deficit counter; per
quantum the counter is incremented by `weight × quantum`. Jobs serve
while their cost ≤ the deficit. Heavier-weighted tenants serve more
work per round without starving lighter ones.

## Quality

- **44 tests** including 1 Hypothesis property: under random weights
  the heavier tenant serves at least as many jobs as the lighter one
  (within a ±1 quantisation slack).
- mypy `--strict` clean over 11 source files; ruff clean.
- Multi-stage slim Docker image, non-root `mtip` user.
- Python 3.10 / 3.11 / 3.12 CI matrix + Docker smoke step.

## License

MIT — see [LICENSE](LICENSE).
