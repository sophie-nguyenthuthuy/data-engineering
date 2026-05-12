-- Bronze layer: raw transactions (Delta Lake)
-- Append-only; no deduplication at this layer.

CREATE TABLE IF NOT EXISTS bronze.transactions (
    transaction_id  STRING,
    customer_id     STRING,
    product_id      STRING,
    amount          DOUBLE,
    currency        STRING,
    status          STRING,
    event_date      DATE,
    created_at      TIMESTAMP,
    updated_at      TIMESTAMP,
    -- ingestion metadata
    _ingested_at    TIMESTAMP,
    _source_table   STRING
)
USING DELTA
PARTITIONED BY (event_date)
LOCATION 's3://my-bucket/lakehouse/bronze/transactions'
TBLPROPERTIES (
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.autoOptimize.autoCompact'   = 'true',
    'delta.logRetentionDuration'       = 'interval 7 days'
);
