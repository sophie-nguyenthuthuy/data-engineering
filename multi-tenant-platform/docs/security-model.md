# Security Model

## Defense in Depth

The platform applies isolation at three independent layers. A bug in any single layer does not compromise tenant data.

```
Layer 1: Application (JWT/API-key → tenant_id resolution)
Layer 2: Database (PostgreSQL RLS policies)
Layer 3: Storage (per-tenant S3 bucket namespace)
```

## Layer 1 — Application Auth

Every request must carry either:
- A **Bearer JWT** containing `tenant_id`, `user_id`, `role`, `exp`
- An **X-API-Key** header, resolved to a tenant via hashed lookup

JWTs are signed with HMAC-SHA256. API keys are stored as SHA-256 hashes — raw keys are never persisted.

Tokens are short-lived (60 min default). API keys can be revoked instantly.

## Layer 2 — Row-Level Security

PostgreSQL RLS is the authoritative enforcement boundary. Even if the application layer has a bug (e.g., wrong tenant_id passed to a query), the database will return an empty result set rather than leaking cross-tenant data.

### How it works

```sql
-- Every session sets this before any query:
SET LOCAL app.tenant_id = '<uuid>';

-- RLS policy on data_records:
CREATE POLICY tenant_isolation ON platform.data_records
    USING (tenant_id = current_setting('app.tenant_id', TRUE)::UUID);

-- FORCE ROW LEVEL SECURITY means even the table owner is subject to policies
ALTER TABLE platform.data_records FORCE ROW LEVEL SECURITY;
```

`SET LOCAL` scopes the setting to the current transaction — it resets automatically on `COMMIT`/`ROLLBACK`, preventing context bleed across pooled connections.

### Application role

The `platform_app` role is used for all application queries. It:
- Cannot bypass RLS (not a superuser)
- Has no schema-creation privileges
- Can only `SELECT/INSERT/UPDATE/DELETE` on platform tables

## Layer 3 — Object Storage Isolation

Each tenant's files live in a dedicated bucket:

```
s3://tenant-{tenant_id}/
```

Presigned URLs are scoped to the tenant's bucket. Path traversal bugs in the `key` parameter cannot cross bucket boundaries because the bucket name is derived from `tenant_id`, not from user input.

## Encryption at Rest

Sensitive field values (PII, secrets) are encrypted using AES-256-GCM with per-tenant derived keys (HKDF from a master key). A compromised tenant's encrypted blob cannot be decrypted with another tenant's derived key.

## Audit Trail

All mutating operations are logged to `platform.audit_logs` (append-only). The logs are:
- RLS-protected (tenants see only their own entries)
- Partitioned by month for retention management
- Written by the API, not editable by tenants

## Threat Model

| Threat | Mitigation |
|--------|-----------|
| JWT forgery | HMAC-SHA256 signature; short expiry |
| Tenant ID spoofing | RLS enforced at DB; app role cannot bypass |
| API key leakage | Only hash stored; bcrypt-equivalent comparison |
| Cross-tenant query | RLS blocks at DB layer regardless of app bug |
| S3 path traversal | Bucket name = tenant_id (not user input) |
| Compromised DB creds | RLS still enforces tenant isolation |
| Data exfiltration via export | Celery job quotas + RLS on the export query |
