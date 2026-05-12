-- PostgreSQL initialization for the learned optimizer.
-- Called automatically by Docker entrypoint.

-- Tune planner settings for research reproducibility
ALTER SYSTEM SET enable_hashjoin = on;
ALTER SYSTEM SET enable_mergejoin = on;
ALTER SYSTEM SET enable_nestloop = on;
ALTER SYSTEM SET enable_seqscan = on;
ALTER SYSTEM SET enable_indexscan = on;
ALTER SYSTEM SET enable_bitmapscan = on;

-- Enable query statistics extension for monitoring
CREATE EXTENSION IF NOT EXISTS pg_stat_statements;

SELECT pg_reload_conf();
