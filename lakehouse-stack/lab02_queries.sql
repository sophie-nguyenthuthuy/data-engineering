-- =========================================================
-- LAB 02 — SQL Fundamentals → Advanced
-- 25 SQL challenges chia thành 5 nhóm, mỗi nhóm 5 bài.
-- Chạy trên PostgreSQL 15 (container de_postgres).
-- =========================================================

-- ---------------------------------------------------------
-- 0. Chuẩn bị dữ liệu mẫu
-- ---------------------------------------------------------
DROP TABLE IF EXISTS orders;
DROP TABLE IF EXISTS products;
DROP TABLE IF EXISTS customers;

CREATE TABLE customers (
    id SERIAL PRIMARY KEY,
    name VARCHAR(100),
    city VARCHAR(100)
);

CREATE TABLE products (
    id SERIAL PRIMARY KEY,
    product_name VARCHAR(100),
    category VARCHAR(50),
    price NUMERIC(10, 2)
);

CREATE TABLE orders (
    order_id SERIAL PRIMARY KEY,
    customer_id INT REFERENCES customers(id),
    product_id INT REFERENCES products(id),
    order_date DATE,
    amount NUMERIC(10, 2)
);

INSERT INTO customers (name, city) VALUES
('Nguyen Van A', 'Hanoi'),
('Tran Thi B', 'HCMC'),
('Le Van C', 'Da Nang'),
('Pham Thi D', 'Hanoi'),
('Hoang Van E', 'Can Tho');  -- Khách chưa có đơn hàng để test LEFT JOIN

INSERT INTO products (product_name, category, price) VALUES
('Laptop Dell',       'Electronics', 1500.00),
('Mouse Logitech',    'Accessories',   50.00),
('Keyboard Keychron', 'Accessories',  100.00),
('Monitor LG',        'Electronics',  300.00),
('USB Hub',           'Accessories',   25.00);

INSERT INTO orders (customer_id, product_id, order_date, amount) VALUES
(1, 1, CURRENT_DATE - INTERVAL '5 days',  1500.00),
(1, 2, CURRENT_DATE - INTERVAL '40 days',   50.00),
(2, 3, CURRENT_DATE - INTERVAL '2 days',   100.00),
(3, 4, CURRENT_DATE - INTERVAL '15 days',  300.00),
(1, 4, CURRENT_DATE - INTERVAL '20 days',  300.00),
(2, 1, CURRENT_DATE - INTERVAL '10 days', 1500.00),
(4, 2, CURRENT_DATE - INTERVAL '3 days',    50.00),
(4, 3, CURRENT_DATE - INTERVAL '1 days',   100.00);


-- =========================================================
-- NHÓM 1 — SELECT / WHERE / ORDER BY  (5 bài)
-- =========================================================

-- Bài 1. Liệt kê toàn bộ dữ liệu từ bảng customers.
SELECT * FROM customers;

-- Bài 2. Chọn tên khách hàng và thành phố, sắp xếp A→Z theo tên.
SELECT name, city FROM customers ORDER BY name ASC;

-- Bài 3. Lọc các đơn hàng có amount > 100, sắp xếp giảm dần theo amount.
SELECT * FROM orders WHERE amount > 100 ORDER BY amount DESC;

-- Bài 4. Đơn hàng trong 30 ngày gần nhất, chỉ lấy 3 dòng đầu.
SELECT *
FROM   orders
WHERE  order_date >= CURRENT_DATE - INTERVAL '30 days'
ORDER  BY order_date DESC
LIMIT  3;

-- Bài 5. Sản phẩm thuộc 'Electronics' có giá từ 250 đến 2000.
SELECT product_name, category, price
FROM   products
WHERE  category = 'Electronics'
  AND  price BETWEEN 250 AND 2000
ORDER  BY price DESC;


-- =========================================================
-- NHÓM 2 — JOIN cơ bản → nâng cao  (5 bài)
-- =========================================================

-- Bài 6. INNER JOIN: ghép orders với customers, hiển thị tên khách + order_id + amount.
SELECT c.name, o.order_id, o.amount
FROM   customers c
INNER  JOIN orders o ON c.id = o.customer_id;

-- Bài 7. LEFT JOIN: liệt kê TẤT CẢ khách hàng, kèm đơn hàng nếu có
-- (khách chưa mua sẽ hiện NULL — xác nhận khách 'Hoang Van E').
SELECT c.name, o.order_id, o.amount
FROM   customers c
LEFT   JOIN orders o ON c.id = o.customer_id
ORDER  BY c.name;

-- Bài 8. RIGHT JOIN: giữ toàn bộ orders, kèm customers;
-- so sánh với INNER JOIN để thấy khác biệt khi có order mồ côi.
SELECT c.name, o.order_id, o.amount
FROM   orders o
RIGHT  JOIN customers c ON c.id = o.customer_id;

-- Bài 9. FULL OUTER JOIN: hợp của 2 bảng (cho thấy cả khách không có order).
SELECT c.name, o.order_id, o.amount
FROM   customers c
FULL   OUTER JOIN orders o ON c.id = o.customer_id
ORDER  BY c.name;

-- Bài 10. JOIN 3 bảng (customers + orders + products): tên khách, tên sản phẩm, danh mục, amount.
SELECT c.name        AS customer,
       p.product_name AS product,
       p.category,
       o.amount,
       o.order_date
FROM   orders o
JOIN   customers c ON c.id = o.customer_id
JOIN   products  p ON p.id = o.product_id
ORDER  BY o.order_date DESC;


-- =========================================================
-- NHÓM 3 — SUBQUERY / CTE  (5 bài)
-- =========================================================

-- Bài 11. Subquery: tìm khách có TỔNG amount lớn hơn mức trung bình toàn bộ khách hàng.
SELECT customer_id, SUM(amount) AS total_amount
FROM   orders
GROUP  BY customer_id
HAVING SUM(amount) > (
    SELECT AVG(total_customer_amount)
    FROM   (SELECT SUM(amount) AS total_customer_amount
            FROM   orders
            GROUP  BY customer_id) AS sub
);

-- Bài 12. CTE recent_orders: đơn hàng 30 ngày gần nhất → JOIN với customers.
WITH recent_orders AS (
    SELECT * FROM orders
    WHERE  order_date >= CURRENT_DATE - INTERVAL '30 days'
)
SELECT c.name, r.order_id, r.amount, r.order_date
FROM   customers c
JOIN   recent_orders r ON c.id = r.customer_id
ORDER  BY r.order_date DESC;

-- Bài 13. 2 CTE liên tiếp: (a) tính doanh thu theo khách → (b) xếp hạng.
WITH revenue_per_customer AS (
    SELECT customer_id, SUM(amount) AS total_revenue
    FROM   orders
    GROUP  BY customer_id
),
ranked_customers AS (
    SELECT customer_id,
           total_revenue,
           RANK() OVER (ORDER BY total_revenue DESC) AS rnk
    FROM   revenue_per_customer
)
SELECT * FROM ranked_customers;

-- Bài 14. Subquery tương quan: mỗi đơn hàng kèm số đơn cùng khách đã có.
SELECT o.order_id,
       o.customer_id,
       o.amount,
       (SELECT COUNT(*)
        FROM   orders o2
        WHERE  o2.customer_id = o.customer_id) AS total_orders_of_customer
FROM   orders o
ORDER  BY o.customer_id, o.order_date;

-- Bài 15. CTE đệ quy: sinh chuỗi 7 ngày gần nhất để LEFT JOIN với orders (điền NULL ngày không có đơn).
WITH RECURSIVE last_7_days(d) AS (
    SELECT CURRENT_DATE
    UNION ALL
    SELECT d - INTERVAL '1 day' FROM last_7_days WHERE d > CURRENT_DATE - INTERVAL '6 days'
)
SELECT l.d::date AS day, COUNT(o.order_id) AS orders_count, COALESCE(SUM(o.amount),0) AS revenue
FROM   last_7_days l
LEFT   JOIN orders o ON o.order_date = l.d
GROUP  BY l.d
ORDER  BY l.d;


-- =========================================================
-- NHÓM 4 — WINDOW FUNCTIONS  (5 bài)
-- =========================================================

-- Bài 16. SUM() OVER(): running total theo khách hàng, sắp theo order_date.
SELECT customer_id, order_date, amount,
       SUM(amount) OVER (PARTITION BY customer_id ORDER BY order_date) AS running_total
FROM   orders
ORDER  BY customer_id, order_date;

-- Bài 17. ROW_NUMBER(): đánh STT đơn hàng của mỗi khách theo thứ tự ngày.
SELECT customer_id, order_date, amount,
       ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY order_date) AS order_seq
FROM   orders;

-- Bài 18. RANK() vs DENSE_RANK(): xếp hạng khách theo tổng doanh thu.
SELECT customer_id,
       SUM(amount) AS total_amount,
       RANK()       OVER (ORDER BY SUM(amount) DESC) AS r_rank,
       DENSE_RANK() OVER (ORDER BY SUM(amount) DESC) AS d_rank
FROM   orders
GROUP  BY customer_id;

-- Bài 19. PARTITION BY vs GROUP BY: cùng bài toán "tổng theo khách" — so sánh số dòng trả về.
-- (a) GROUP BY — mỗi khách 1 dòng
SELECT customer_id, SUM(amount) AS total_amount
FROM   orders
GROUP  BY customer_id;
-- (b) WINDOW — giữ nguyên số dòng đơn hàng
SELECT customer_id, order_id, amount,
       SUM(amount) OVER (PARTITION BY customer_id) AS total_customer_amount
FROM   orders;

-- Bài 20. Top-N theo nhóm: lấy 2 đơn hàng có amount cao nhất của mỗi khách.
WITH ranked AS (
    SELECT customer_id, order_id, amount,
           ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY amount DESC) AS rn
    FROM   orders
)
SELECT * FROM ranked WHERE rn <= 2;


-- =========================================================
-- NHÓM 5 — OPTIMIZATION  (5 bài)
-- =========================================================

-- Bài 21. EXPLAIN plan trước khi có index (dự kiến Seq Scan).
EXPLAIN ANALYZE
SELECT * FROM orders WHERE customer_id = 1;

-- Bài 22. Tạo index cho join key / filter thường xuyên.
CREATE INDEX IF NOT EXISTS idx_orders_customer_id ON orders(customer_id);
CREATE INDEX IF NOT EXISTS idx_orders_order_date  ON orders(order_date);

-- Bài 23. EXPLAIN lại — plan ưu tiên Index Scan khi dữ liệu đủ lớn.
EXPLAIN ANALYZE
SELECT * FROM orders WHERE customer_id = 1;

-- Bài 24. Tránh SELECT *: chỉ lấy cột cần thiết (giảm I/O, dễ dùng index-only scan).
EXPLAIN ANALYZE
SELECT order_id, amount
FROM   orders
WHERE  order_date >= CURRENT_DATE - INTERVAL '30 days';

-- Bài 25. So sánh cùng bài toán "tổng theo khách" bằng GROUP BY vs WINDOW — đọc cost trên EXPLAIN.
EXPLAIN ANALYZE
SELECT customer_id, SUM(amount) FROM orders GROUP BY customer_id;

EXPLAIN ANALYZE
SELECT DISTINCT customer_id,
       SUM(amount) OVER (PARTITION BY customer_id)
FROM   orders;
