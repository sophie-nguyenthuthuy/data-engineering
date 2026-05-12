CREATE SCHEMA IF NOT EXISTS bootcamp_dw;

DROP TABLE IF EXISTS bootcamp_dw.mart_daily_category_sales;
DROP TABLE IF EXISTS bootcamp_dw.fact_orders;
DROP TABLE IF EXISTS bootcamp_dw.dim_customers;
DROP TABLE IF EXISTS bootcamp_dw.dim_products;
DROP TABLE IF EXISTS bootcamp_dw.dim_date;

CREATE TABLE IF NOT EXISTS bootcamp_dw.dim_date (
    date_key        INT PRIMARY KEY,
    full_date       DATE NOT NULL,
    year            INT,
    quarter         INT,
    month           INT,
    month_name      VARCHAR(20),
    day             INT
);

CREATE TABLE IF NOT EXISTS bootcamp_dw.dim_products (
    product_key     SERIAL PRIMARY KEY,
    product_id      INT NOT NULL,
    product_name    VARCHAR(100),
    category        VARCHAR(50),
    unit_price      NUMERIC(12,2)
);

CREATE TABLE IF NOT EXISTS bootcamp_dw.dim_customers (
    customer_key        SERIAL PRIMARY KEY,
    customer_id         INT NOT NULL,
    full_name           VARCHAR(100),
    city                VARCHAR(50),
    segment             VARCHAR(30),
    effective_date      DATE NOT NULL,
    end_date            DATE,
    current_flag        BOOLEAN DEFAULT TRUE,
    previous_city       VARCHAR(50),
    current_city        VARCHAR(50)
);

CREATE TABLE IF NOT EXISTS bootcamp_dw.fact_orders (
    order_id         INT PRIMARY KEY,
    date_key         INT REFERENCES bootcamp_dw.dim_date(date_key),
    customer_key     INT REFERENCES bootcamp_dw.dim_customers(customer_key),
    product_key      INT REFERENCES bootcamp_dw.dim_products(product_key),
    quantity         INT,
    gross_amount     NUMERIC(12,2)
);

INSERT INTO bootcamp_dw.dim_date(date_key, full_date, year, quarter, month, month_name, day) VALUES
(20260301,'2026-03-01',2026,1,3,'March',1),
(20260302,'2026-03-02',2026,1,3,'March',2),
(20260303,'2026-03-03',2026,1,3,'March',3);

INSERT INTO bootcamp_dw.dim_products(product_id, product_name, category, unit_price) VALUES
(1001,'Notebook','Stationery',15.50),
(1002,'Pen Set','Stationery',20.00),
(1003,'Desk Lamp','Home Office',12.00);

INSERT INTO bootcamp_dw.dim_customers(customer_id, full_name, city, segment, effective_date, end_date, current_flag, previous_city, current_city) VALUES
(101,'Alice Nguyen','Hanoi','Retail','2026-01-01',NULL,TRUE,NULL,'Hanoi'),
(102,'Bao Tran','Danang','Retail','2026-01-01',NULL,TRUE,NULL,'Danang'),
(103,'Chi Le','HCMC','Corporate','2026-01-01',NULL,TRUE,NULL,'HCMC');

INSERT INTO bootcamp_dw.fact_orders(order_id, date_key, customer_key, product_key, quantity, gross_amount) VALUES
(1,20260301,1,1,2,31.00),
(2,20260301,2,2,1,20.00),
(3,20260302,1,3,3,36.00),
(4,20260303,3,1,5,77.50);

-- Bài 1: Truy vấn JOIN
SELECT f.order_id, d.full_date, c.full_name, p.product_name, f.quantity, f.gross_amount
FROM bootcamp_dw.fact_orders f
JOIN bootcamp_dw.dim_date d ON f.date_key = d.date_key
JOIN bootcamp_dw.dim_customers c ON f.customer_key = c.customer_key
JOIN bootcamp_dw.dim_products p ON f.product_key = p.product_key;

-- Bài thực hành SCD
-- SCD Type 1 cho 101
UPDATE bootcamp_dw.dim_customers
SET city = 'Haiphong', current_city='Haiphong'
WHERE customer_id = 101 AND current_flag = TRUE;

-- SCD Type 2 cho 102
UPDATE bootcamp_dw.dim_customers
SET end_date='2026-03-31', current_flag=FALSE
WHERE customer_id=102 AND current_flag=TRUE;

INSERT INTO bootcamp_dw.dim_customers(customer_id, full_name, city, segment, effective_date, end_date, current_flag, previous_city, current_city)
VALUES (102,'Bao Tran','Hue','Retail','2026-04-01',NULL,TRUE,'Danang','Hue');

-- MÔ PHỎNG SCD TYPE 6 CHO CUSTOMER 103
-- Type 6 duy trì dòng lịch sử (như Type 2), vừa overwrite cột current_city ở các dòng cũ.
-- Giả sử KH đổi từ HCMC sang Vung Tau
UPDATE bootcamp_dw.dim_customers
SET end_date='2026-06-30', current_flag=FALSE, current_city='Vung Tau'
WHERE customer_id=103 AND current_flag=TRUE;

INSERT INTO bootcamp_dw.dim_customers(customer_id, full_name, city, segment, effective_date, end_date, current_flag, previous_city, current_city)
VALUES (103,'Chi Le','Vung Tau','Corporate','2026-07-01',NULL,TRUE,'HCMC','Vung Tau');

-- Lấy lại bảng khách hàng sau SCD để check
SELECT customer_key, customer_id, full_name, city, effective_date, end_date, current_flag, previous_city, current_city 
FROM bootcamp_dw.dim_customers 
ORDER BY customer_id, effective_date;

-- Data mart
CREATE TABLE IF NOT EXISTS bootcamp_dw.mart_daily_category_sales AS
SELECT
    d.full_date,
    p.category,
    SUM(f.quantity)      AS total_qty,
    SUM(f.gross_amount)  AS total_revenue
FROM bootcamp_dw.fact_orders f
JOIN bootcamp_dw.dim_date d     ON f.date_key = d.date_key
JOIN bootcamp_dw.dim_products p ON f.product_key = p.product_key
GROUP BY d.full_date, p.category
ORDER BY d.full_date, p.category;

-- Kiểm tra mart
SELECT * FROM bootcamp_dw.mart_daily_category_sales;
