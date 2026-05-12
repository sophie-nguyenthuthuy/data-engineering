# Multi-Tenant Data Platform

A production-grade platform where teams/clients share infrastructure but have isolated storage, per-tenant compute quotas, and row-level security enforced at the database layer.

## Architecture Overview

```
┌─────────────────────────────────────────────────────────────────┐
│                        API Gateway (FastAPI)                    │
│          JWT auth → tenant resolution → quota check             │
└──────────────────────────┬──────────────────────────────────────┘
                           │
           ┌───────────────┼───────────────┐
           ▼               ▼               ▼
    ┌─────────────┐ ┌─────────────┐ ┌─────────────┐
    │  Tenant A   │ │  Tenant B   │ │  Tenant C   │
    │  Schema     │ │  Schema     │ │  Schema     │
    │  (isolated) │ │  (isolated) │ │  (isolated) │
    └─────────────┘ └─────────────┘ └─────────────┘
           │               │               │
           └───────────────┼───────────────┘
                           ▼
              ┌────────────────────────┐
              │   PostgreSQL + RLS     │  ← Row-level security policies
              │   (shared cluster)     │     enforce tenant isolation
              └────────────────────────┘
                           │
              ┌────────────┴────────────┐
              ▼                         ▼
       ┌─────────────┐          ┌─────────────┐
       │    Redis    │          │ Object Store │
       │  (quotas +  │          │  (MinIO/S3)  │
       │   caching)  │          │  per-tenant  │
       └─────────────┘          │   buckets    │
                                └─────────────┘
```

## Key Design Principles

| Concern | Approach |
|---------|----------|
| **Storage isolation** | Per-tenant PostgreSQL schemas + dedicated S3/MinIO prefixes |
| **Compute quotas** | Redis token-bucket rate limiting per tenant tier |
| **Row-level security** | PostgreSQL RLS policies gated on `current_setting('app.tenant_id')` |
| **Auth** | JWT with `tenant_id` + `role` claims; API keys for service accounts |
| **Query isolation** | Every query sets `SET LOCAL app.tenant_id = ?` before execution |
| **Audit trail** | Append-only audit log table shared across tenants (tenant-partitioned) |

## Tenant Tiers

| Tier | Storage | Query Rate | Concurrent Jobs | Max Row Count |
|------|---------|------------|-----------------|---------------|
| `free` | 1 GB | 10 req/min | 1 | 100K |
| `starter` | 10 GB | 60 req/min | 3 | 5M |
| `pro` | 100 GB | 300 req/min | 10 | 50M |
| `enterprise` | unlimited | custom | custom | unlimited |

## Quick Start

```bash
# 1. Start infrastructure
docker compose up -d

# 2. Run migrations and seed
./scripts/bootstrap.sh

# 3. Create a tenant
curl -X POST http://localhost:8000/admin/tenants \
  -H "Authorization: Bearer $ADMIN_TOKEN" \
  -d '{"name": "acme", "tier": "starter"}'

# 4. Start the API
uvicorn api.main:app --reload
```

## Project Structure

```
multi-tenant-platform/
├── api/                  # FastAPI application
│   ├── main.py           # App factory, middleware wiring
│   ├── routers/          # Route handlers per domain
│   ├── middleware/        # Tenant resolution, quota enforcement
│   └── schemas/          # Pydantic request/response models
├── core/                 # Business logic (framework-agnostic)
│   ├── auth/             # JWT + API key handling
│   ├── quotas/           # Token-bucket quota engine
│   ├── storage/          # Object storage abstraction
│   └── security/         # RLS helpers, encryption
├── db/                   # Database layer
│   ├── models/           # SQLAlchemy models
│   ├── migrations/       # Alembic migration scripts
│   └── session.py        # Async session factory with tenant context
├── workers/              # Background job workers (Celery)
├── infra/                # Infrastructure as code
│   ├── docker/           # Dockerfiles
│   ├── terraform/        # Cloud provisioning
│   └── k8s/              # Kubernetes manifests
├── tests/                # Test suite
└── scripts/              # Operational scripts
```
