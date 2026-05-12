-- Silver layer: customers with SCD Type-2 versioning (Delta Lake)

CREATE TABLE IF NOT EXISTS silver.customers (
    customer_id     STRING        NOT NULL,
    first_name      STRING,
    last_name       STRING,
    email           STRING,
    country_code    STRING,
    segment         STRING,
    -- SCD Type-2 columns
    _is_current     BOOLEAN,
    _valid_from     TIMESTAMP,
    _valid_to       TIMESTAMP,
    _ingested_at    TIMESTAMP
)
USING DELTA
PARTITIONED BY (country_code)
LOCATION 's3://my-bucket/lakehouse/silver/customers'
TBLPROPERTIES (
    'delta.columnMapping.mode'  = 'name',
    'delta.minReaderVersion'    = '2',
    'delta.minWriterVersion'    = '5',
    'delta.enableChangeDataFeed' = 'true'
);
