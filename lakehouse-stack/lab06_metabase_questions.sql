-- =========================================================
-- LAB 06 — Metabase questions trên data mart (lab06_dw)
-- Connect Metabase (http://localhost:3000) → PostgreSQL
--   host     = postgres
--   port     = 5432
--   database = de_db
--   user     = de_user
--   password = de_password
-- =========================================================

-- Question 1 — Doanh thu theo ngày (Bar chart)
SELECT order_date,
       SUM(revenue) AS total_revenue,
       COUNT(*)     AS orders_count
FROM   lab06_dw.fact_orders
GROUP  BY order_date
ORDER  BY order_date;

-- Question 2 — Top khách hàng theo doanh thu (Pie / Row chart)
SELECT c.customer_name,
       SUM(f.revenue) AS revenue
FROM   lab06_dw.fact_orders f
JOIN   lab06_dw.dim_customer c ON c.customer_key = f.customer_key
WHERE  c.is_current = TRUE
GROUP  BY c.customer_name
ORDER  BY revenue DESC
LIMIT  10;

-- Question 3 — Doanh thu theo category sản phẩm (Bar chart)
SELECT p.category,
       SUM(f.revenue)   AS revenue,
       SUM(f.quantity)  AS units_sold
FROM   lab06_dw.fact_orders f
JOIN   lab06_dw.dim_product p ON p.product_key = f.product_key
GROUP  BY p.category
ORDER  BY revenue DESC;
