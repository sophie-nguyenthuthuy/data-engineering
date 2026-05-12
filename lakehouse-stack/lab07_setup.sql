CREATE SCHEMA IF NOT EXISTS lakehouse.bronze WITH (location = 's3a://lakehouse/bronze/');
CREATE SCHEMA IF NOT EXISTS lakehouse.silver WITH (location = 's3a://lakehouse/silver/');
CREATE SCHEMA IF NOT EXISTS lakehouse.gold WITH (location = 's3a://lakehouse/gold/');

DROP TABLE IF EXISTS lakehouse.gold.customer_sales;
DROP TABLE IF EXISTS lakehouse.silver.orders_clean;
DROP TABLE IF EXISTS lakehouse.bronze.orders_raw;

CREATE TABLE IF NOT EXISTS lakehouse.bronze.orders_raw (
    order_id VARCHAR,
    customer_id VARCHAR,
    amount VARCHAR,
    order_date VARCHAR
) WITH (
    format = 'TEXTFILE',
    external_location = 's3a://lakehouse/bronze/orders'
);

INSERT INTO lakehouse.bronze.orders_raw VALUES 
('1', '101', '150.5', '2026-01-01'),
('2', '102', 'bad_amount', '2026-01-02'),
('3', '101', '250.0', '2026-01-03');

CREATE TABLE IF NOT EXISTS lakehouse.silver.orders_clean WITH (
    format = 'PARQUET',
    external_location = 's3a://lakehouse/silver/orders_clean'
) AS
SELECT 
    CAST(order_id AS INTEGER) AS order_id,
    CAST(customer_id AS INTEGER) AS customer_id,
    CAST(TRY(CAST(amount AS DOUBLE)) AS DOUBLE) AS amount,
    CAST(TRY(CAST(order_date AS DATE)) AS DATE) AS order_date
FROM lakehouse.bronze.orders_raw
WHERE TRY(CAST(amount AS DOUBLE)) IS NOT NULL;

CREATE TABLE IF NOT EXISTS lakehouse.gold.customer_sales WITH (
    format = 'PARQUET',
    external_location = 's3a://lakehouse/gold/customer_sales'
) AS
SELECT 
    customer_id,
    COUNT(order_id) AS total_orders,
    SUM(amount) AS total_revenue
FROM lakehouse.silver.orders_clean
GROUP BY customer_id;

-- Views for assertions
SELECT * FROM lakehouse.bronze.orders_raw;
SELECT * FROM lakehouse.silver.orders_clean;
SELECT * FROM lakehouse.gold.customer_sales;
