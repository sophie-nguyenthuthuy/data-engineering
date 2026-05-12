CREATE SCHEMA IF NOT EXISTS lab06_dw;

-- Drop existings to be safe
DROP TABLE IF EXISTS lab06_dw.fact_orders;
DROP TABLE IF EXISTS lab06_dw.dim_customer;
DROP TABLE IF EXISTS lab06_dw.dim_product;

-- Dimension: Customer (SCD Type 2)
CREATE TABLE lab06_dw.dim_customer (
    customer_key SERIAL PRIMARY KEY,
    customer_id INT,
    customer_name TEXT,
    city TEXT,
    effective_from DATE,
    effective_to DATE,
    is_current BOOLEAN
);

-- Dimension: Product
CREATE TABLE lab06_dw.dim_product (
    product_key SERIAL PRIMARY KEY,
    product_id INT,
    product_name TEXT,
    category TEXT
);

-- Fact: Orders
CREATE TABLE lab06_dw.fact_orders (
    order_key SERIAL PRIMARY KEY,
    order_id INT,
    order_date DATE,
    customer_key INT REFERENCES lab06_dw.dim_customer(customer_key),
    product_key INT REFERENCES lab06_dw.dim_product(product_key),
    quantity INT,
    revenue NUMERIC(12,2)
);

-- Load Sample Data
INSERT INTO lab06_dw.dim_customer (customer_id, customer_name, city, effective_from, effective_to, is_current) VALUES 
(1, 'Alice', 'HCM', '2026-01-01', NULL, TRUE),
(2, 'Bob', 'HN', '2026-01-01', NULL, TRUE);

INSERT INTO lab06_dw.dim_product (product_id, product_name, category) VALUES
(100, 'Laptop', 'Electronics'),
(101, 'Mouse', 'Accessories');

INSERT INTO lab06_dw.fact_orders (order_id, order_date, customer_key, product_key, quantity, revenue) VALUES
(1001, '2026-04-01', 1, 1, 1, 1500.00),
(1002, '2026-04-01', 2, 2, 2, 50.00),
(1003, '2026-04-02', 1, 2, 1, 25.00);

-- Query to verify
SELECT * FROM lab06_dw.fact_orders;
