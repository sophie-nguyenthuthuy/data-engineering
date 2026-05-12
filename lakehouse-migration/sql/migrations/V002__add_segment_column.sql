-- Migration V002: add segment column to silver.customers
-- Delta Lake schema evolution — no table rewrite needed.

ALTER TABLE silver.customers ADD COLUMN segment STRING;

-- Backfill from source
MERGE INTO silver.customers AS target
USING legacy.customers AS source
ON target.customer_id = source.customer_id AND target._is_current = true
WHEN MATCHED THEN UPDATE SET target.segment = source.segment;
