-- =============================================================================
-- Migration 001: Initial schema with RLS, schemas, and audit tables
-- =============================================================================

BEGIN;

-- Platform schema holds shared/admin tables
CREATE SCHEMA IF NOT EXISTS platform;

-- -----------------------------------------------------------------------
-- Enums
-- -----------------------------------------------------------------------
DO $$ BEGIN
    CREATE TYPE platform.tenant_tier AS ENUM ('free', 'starter', 'pro', 'enterprise');
EXCEPTION WHEN duplicate_object THEN NULL; END $$;

DO $$ BEGIN
    CREATE TYPE platform.user_role AS ENUM ('owner', 'admin', 'member', 'viewer');
EXCEPTION WHEN duplicate_object THEN NULL; END $$;

-- -----------------------------------------------------------------------
-- Tenant registry (no RLS — platform-level access)
-- -----------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS platform.tenants (
    id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    name            TEXT NOT NULL,
    slug            TEXT NOT NULL UNIQUE,
    tier            platform.tenant_tier NOT NULL DEFAULT 'free',
    is_active       BOOLEAN NOT NULL DEFAULT TRUE,
    storage_bytes_used BIGINT NOT NULL DEFAULT 0,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS platform.tenant_users (
    id          UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    tenant_id   UUID NOT NULL REFERENCES platform.tenants(id) ON DELETE CASCADE,
    user_id     UUID NOT NULL,
    role        platform.user_role NOT NULL DEFAULT 'member',
    created_at  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (tenant_id, user_id)
);

CREATE TABLE IF NOT EXISTS platform.api_keys (
    id          UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    tenant_id   UUID NOT NULL REFERENCES platform.tenants(id) ON DELETE CASCADE,
    name        TEXT NOT NULL,
    key_hash    TEXT NOT NULL UNIQUE,
    is_active   BOOLEAN NOT NULL DEFAULT TRUE,
    last_used_at TIMESTAMPTZ,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at  TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- -----------------------------------------------------------------------
-- Datasets — RLS enforced
-- -----------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS platform.datasets (
    id                  UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    tenant_id           UUID NOT NULL REFERENCES platform.tenants(id) ON DELETE CASCADE,
    name                TEXT NOT NULL,
    description         TEXT,
    schema_definition   JSONB NOT NULL DEFAULT '{}',
    row_count           BIGINT NOT NULL DEFAULT 0,
    size_bytes          BIGINT NOT NULL DEFAULT 0,
    is_public           BOOLEAN NOT NULL DEFAULT FALSE,
    created_at          TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at          TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

ALTER TABLE platform.datasets ENABLE ROW LEVEL SECURITY;

-- Tenants may only see their own datasets
CREATE POLICY tenant_isolation ON platform.datasets
    USING (tenant_id = current_setting('app.tenant_id', TRUE)::UUID);

-- Public datasets are readable by anyone (cross-tenant sharing)
CREATE POLICY public_read ON platform.datasets
    FOR SELECT
    USING (is_public = TRUE);

-- -----------------------------------------------------------------------
-- Data records — RLS enforced
-- -----------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS platform.data_records (
    id          UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    tenant_id   UUID NOT NULL REFERENCES platform.tenants(id) ON DELETE CASCADE,
    dataset_id  UUID NOT NULL REFERENCES platform.datasets(id) ON DELETE CASCADE,
    data        JSONB NOT NULL,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at  TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_data_records_tenant_dataset
    ON platform.data_records (tenant_id, dataset_id);

-- GIN index for JSONB queries within tenant data
CREATE INDEX IF NOT EXISTS idx_data_records_data
    ON platform.data_records USING GIN (data);

ALTER TABLE platform.data_records ENABLE ROW LEVEL SECURITY;

CREATE POLICY tenant_isolation ON platform.data_records
    USING (tenant_id = current_setting('app.tenant_id', TRUE)::UUID);

-- -----------------------------------------------------------------------
-- Audit log — INSERT-only, RLS for reads
-- -----------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS platform.audit_logs (
    id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    tenant_id       UUID NOT NULL,
    actor_id        TEXT NOT NULL,
    action          TEXT NOT NULL,
    resource_type   TEXT NOT NULL,
    resource_id     TEXT,
    metadata        JSONB NOT NULL DEFAULT '{}',
    occurred_at     TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_audit_logs_tenant_time
    ON platform.audit_logs (tenant_id, occurred_at DESC);

ALTER TABLE platform.audit_logs ENABLE ROW LEVEL SECURITY;

-- Only platform admins write; tenants only read their own logs
CREATE POLICY tenant_read ON platform.audit_logs
    FOR SELECT
    USING (tenant_id = current_setting('app.tenant_id', TRUE)::UUID);

-- -----------------------------------------------------------------------
-- Application role (least privilege)
-- -----------------------------------------------------------------------
DO $$ BEGIN
    CREATE ROLE platform_app WITH LOGIN PASSWORD 'change_in_production';
EXCEPTION WHEN duplicate_object THEN NULL; END $$;

GRANT USAGE ON SCHEMA platform TO platform_app;
GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA platform TO platform_app;

-- Force RLS even for table owner (bypass only for superuser)
ALTER TABLE platform.datasets FORCE ROW LEVEL SECURITY;
ALTER TABLE platform.data_records FORCE ROW LEVEL SECURITY;
ALTER TABLE platform.audit_logs FORCE ROW LEVEL SECURITY;

COMMIT;
