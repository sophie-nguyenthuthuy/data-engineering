# Quota System

## Token Bucket Algorithm

Request rate limiting uses a **token bucket** implemented as an atomic Lua script in Redis. This guarantees no TOCTOU races — the check-and-decrement is a single atomic operation.

```
Bucket capacity  = burst_requests   (e.g. 120 for starter)
Refill rate      = requests_per_minute / 60 tokens/second
```

Each API request consumes 1 token. If the bucket is empty, the request gets a `429 Too Many Requests` with `Retry-After: 60`.

## Concurrent Job Quota

Background jobs (exports, bulk operations) use a simple Redis counter:

```
INCR quota:{tenant_id}:jobs   # reserve a slot
DECR quota:{tenant_id}:jobs   # release on completion (in finally block)
```

If `current >= concurrent_jobs_limit`, the Celery task retries with backoff.

## Storage Quota

Storage usage is tracked in two places:
1. **Redis** (`storage:{tenant_id}:bytes`) — fast, checked on every upload
2. **PostgreSQL** (`tenants.storage_bytes_used`) — authoritative, synced every 15 min via Celery Beat

The Redis value is the enforcement boundary. The sync task reconciles drift (e.g., from failed uploads that didn't roll back the counter).

## Tier Limits

| Dimension | free | starter | pro | enterprise |
|-----------|------|---------|-----|------------|
| Storage | 1 GB | 10 GB | 100 GB | unlimited |
| Req/min | 10 | 60 | 300 | 10,000 |
| Burst | 20 | 120 | 600 | 20,000 |
| Concurrent jobs | 1 | 3 | 10 | 100 |
| Max rows | 100K | 5M | 50M | unlimited |

## Upgrading Tiers

Tier changes are applied immediately via `PATCH /admin/tenants/{id}/tier`. The new quota limits take effect on the next request — there is no grace period or cache to flush.

## Quota Headers

The API returns quota information in response headers:

```
X-RateLimit-Limit: 60
X-RateLimit-Remaining: 42
X-RateLimit-Reset: 1748900000
```

(Implementation: extend `QuotaMiddleware` to read the remaining tokens from Redis and inject headers.)
