-- ─────────────────────────────────────────────────────────────
-- Time Travel Queries (Delta Lake)
-- ─────────────────────────────────────────────────────────────

-- 1. Query a table at a specific version
SELECT * FROM delta.`s3://my-bucket/lakehouse/silver/customers`
VERSION AS OF 5
WHERE country_code = 'VN';

-- 2. Query a table at a point in time
SELECT * FROM delta.`s3://my-bucket/lakehouse/silver/customers`
TIMESTAMP AS OF '2024-01-01 00:00:00'
WHERE _is_current = true;

-- 3. Show full version history of a table
DESCRIBE HISTORY delta.`s3://my-bucket/lakehouse/silver/customers`;

-- 4. Audit: what changed between two versions?
SELECT *
FROM (
    SELECT *, 'v10' AS snapshot FROM delta.`s3://my-bucket/lakehouse/silver/customers` VERSION AS OF 10
    UNION ALL
    SELECT *, 'v5'  AS snapshot FROM delta.`s3://my-bucket/lakehouse/silver/customers` VERSION AS OF 5
)
WHERE customer_id IN (
    -- customers that existed at v5
    SELECT customer_id FROM delta.`s3://my-bucket/lakehouse/silver/customers` VERSION AS OF 5
)
ORDER BY customer_id, snapshot;

-- 5. Change Data Feed: stream of row-level changes since version 3
SELECT * FROM table_changes('silver.customers', 3)
ORDER BY _commit_timestamp;

-- ─────────────────────────────────────────────────────────────
-- Time Travel Queries (Apache Iceberg)
-- ─────────────────────────────────────────────────────────────

-- 6. Query at snapshot ID
SELECT * FROM glue_catalog.lakehouse.customers
FOR SYSTEM_VERSION AS OF 1234567890;

-- 7. Query at timestamp
SELECT * FROM glue_catalog.lakehouse.customers
FOR SYSTEM_TIME AS OF TIMESTAMP '2024-01-01 00:00:00';

-- 8. List Iceberg snapshots
SELECT * FROM glue_catalog.lakehouse.customers.snapshots
ORDER BY committed_at DESC;
