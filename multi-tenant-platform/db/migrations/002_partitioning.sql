-- =============================================================================
-- Migration 002: Partition audit_logs by month for query performance
-- =============================================================================
-- NOTE: Apply this only after initial schema is in place.
-- This converts audit_logs to a partitioned table.

BEGIN;

-- Rename existing table
ALTER TABLE platform.audit_logs RENAME TO audit_logs_old;

-- Create partitioned replacement
CREATE TABLE platform.audit_logs (
    id              UUID NOT NULL DEFAULT gen_random_uuid(),
    tenant_id       UUID NOT NULL,
    actor_id        TEXT NOT NULL,
    action          TEXT NOT NULL,
    resource_type   TEXT NOT NULL,
    resource_id     TEXT,
    metadata        JSONB NOT NULL DEFAULT '{}',
    occurred_at     TIMESTAMPTZ NOT NULL DEFAULT NOW()
) PARTITION BY RANGE (occurred_at);

-- RLS on partitioned table
ALTER TABLE platform.audit_logs ENABLE ROW LEVEL SECURITY;
ALTER TABLE platform.audit_logs FORCE ROW LEVEL SECURITY;

CREATE POLICY tenant_read ON platform.audit_logs
    FOR SELECT
    USING (tenant_id = current_setting('app.tenant_id', TRUE)::UUID);

-- Create initial partitions (add a pg_cron job to auto-create future ones)
CREATE TABLE platform.audit_logs_y2026m01 PARTITION OF platform.audit_logs
    FOR VALUES FROM ('2026-01-01') TO ('2026-02-01');

CREATE TABLE platform.audit_logs_y2026m02 PARTITION OF platform.audit_logs
    FOR VALUES FROM ('2026-02-01') TO ('2026-03-01');

CREATE TABLE platform.audit_logs_y2026m03 PARTITION OF platform.audit_logs
    FOR VALUES FROM ('2026-03-01') TO ('2026-04-01');

CREATE TABLE platform.audit_logs_y2026m04 PARTITION OF platform.audit_logs
    FOR VALUES FROM ('2026-04-01') TO ('2026-05-01');

CREATE TABLE platform.audit_logs_y2026m05 PARTITION OF platform.audit_logs
    FOR VALUES FROM ('2026-05-01') TO ('2026-06-01');

CREATE TABLE platform.audit_logs_y2026m06 PARTITION OF platform.audit_logs
    FOR VALUES FROM ('2026-06-01') TO ('2026-07-01');

-- Migrate existing data
INSERT INTO platform.audit_logs SELECT * FROM platform.audit_logs_old;
DROP TABLE platform.audit_logs_old;

GRANT SELECT, INSERT ON ALL TABLES IN SCHEMA platform TO platform_app;

COMMIT;
