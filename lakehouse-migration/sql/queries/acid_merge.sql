-- ACID MERGE: upsert new/changed rows into Silver (Delta Lake)

MERGE INTO silver.transactions AS target
USING (
    SELECT *
    FROM bronze.transactions
    WHERE _ingested_at > (SELECT MAX(_ingested_at) FROM silver.transactions)
) AS source
ON target.transaction_id = source.transaction_id
WHEN MATCHED AND (
    target.status   <> source.status   OR
    target.amount   <> source.amount
) THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *;


-- Soft-delete: mark rows deleted in source
MERGE INTO silver.customers AS target
USING deleted_ids AS source          -- CTE or temp view of IDs to delete
ON target.customer_id = source.customer_id AND target._is_current = true
WHEN MATCHED THEN UPDATE SET
    target._is_current = false,
    target._valid_to   = current_timestamp();
