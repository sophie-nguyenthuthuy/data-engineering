-- Lab 17 — Demo: Query via Athena (Conceptual)
-- Equivalent of Trino queries but running serverless over S3.
-- These DDLs/queries would be executed in the AWS Athena console
-- or via boto3 client.start_query_execution().
--
-- S3 layout assumed (from s3_migration_sim.py):
--   s3://company-lakehouse/bronze/data.parquet
--   s3://company-lakehouse/silver/date=YYYY-MM-DD/data.parquet
--   s3://company-lakehouse/gold/date=YYYY-MM-DD/data.parquet
-- ---------------------------------------------------------------------------

-- 0. Create a Glue/Athena database (run once)
CREATE DATABASE IF NOT EXISTS lakehouse
COMMENT 'Lakehouse Bronze/Silver/Gold on S3';


-- ===========================================================================
-- BRONZE LAYER — raw external table (no partition, schema-on-read)
-- ===========================================================================
CREATE EXTERNAL TABLE IF NOT EXISTS lakehouse.bronze_orders (
    order_id        BIGINT,
    order_timestamp STRING,
    quantity        INT,
    unit_price      DOUBLE,
    order_status    STRING,
    payment_method  STRING
)
STORED AS PARQUET
LOCATION 's3://company-lakehouse/bronze/'
TBLPROPERTIES ('parquet.compression' = 'SNAPPY');


-- ===========================================================================
-- SILVER LAYER — cleaned, Hive-partitioned by date
-- ===========================================================================
CREATE EXTERNAL TABLE IF NOT EXISTS lakehouse.silver_orders (
    order_id        BIGINT,
    order_timestamp TIMESTAMP,
    quantity        INT,
    unit_price      DOUBLE,
    order_status    STRING,
    payment_method  STRING,
    total_amount    DOUBLE
)
PARTITIONED BY (date STRING)
STORED AS PARQUET
LOCATION 's3://company-lakehouse/silver/'
TBLPROPERTIES ('parquet.compression' = 'SNAPPY');

-- Tell Athena about the partitions (or use MSCK REPAIR TABLE)
MSCK REPAIR TABLE lakehouse.silver_orders;


-- ===========================================================================
-- GOLD LAYER — daily aggregations, Hive-partitioned by date
-- ===========================================================================
CREATE EXTERNAL TABLE IF NOT EXISTS lakehouse.gold_daily_summary (
    total_orders    BIGINT,
    total_revenue   DOUBLE,
    avg_order_value DOUBLE
)
PARTITIONED BY (date STRING)
STORED AS PARQUET
LOCATION 's3://company-lakehouse/gold/'
TBLPROPERTIES ('parquet.compression' = 'SNAPPY');

MSCK REPAIR TABLE lakehouse.gold_daily_summary;


-- ===========================================================================
-- SAMPLE QUERIES (Lab Task 1: Architecture — identify Athena query patterns)
-- ===========================================================================

-- Q1: Preview raw bronze data (no partition filter — scan full file)
SELECT *
FROM   lakehouse.bronze_orders
LIMIT  10;


-- Q2: Silver — filter by partition (Athena only scans that date's file)
--     Cost-efficient: partition pruning avoids full S3 scan.
SELECT order_id,
       order_timestamp,
       total_amount,
       order_status
FROM   lakehouse.silver_orders
WHERE  date = '2026-04-02'
ORDER  BY total_amount DESC;


-- Q3: Gold — daily revenue trend
SELECT date,
       total_orders,
       ROUND(total_revenue, 2)   AS total_revenue,
       ROUND(avg_order_value, 2) AS avg_order_value
FROM   lakehouse.gold_daily_summary
ORDER  BY date;


-- Q4: Silver — payment method breakdown (cross-partition, will scan all dates)
SELECT payment_method,
       COUNT(*)               AS order_count,
       ROUND(SUM(total_amount), 2) AS revenue
FROM   lakehouse.silver_orders
WHERE  order_status = 'COMPLETED'
GROUP  BY payment_method
ORDER  BY revenue DESC;


-- Q5: CTAS — persist a Gold query result back to S3 as a new table
--     (useful for sharing pre-aggregated data with BI tools / Metabase)
CREATE TABLE lakehouse.gold_payment_summary
WITH (
    format           = 'PARQUET',
    parquet_compression = 'SNAPPY',
    external_location = 's3://company-lakehouse/gold/payment_summary/'
) AS
SELECT payment_method,
       COUNT(*)                    AS order_count,
       ROUND(SUM(total_amount), 2) AS total_revenue
FROM   lakehouse.silver_orders
WHERE  order_status = 'COMPLETED'
GROUP  BY payment_method;
