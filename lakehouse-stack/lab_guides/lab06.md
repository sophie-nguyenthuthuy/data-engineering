# Lab 06 — ETL / ELT & Data Warehouse Fundamentals

## Objectives
- Thiết kế star schema data mart (fact + dimension).
- Nạp dữ liệu, áp dụng SCD Type 2 trên `dim_customer`.
- Kết nối Metabase vào data mart và tạo 2 questions + 1 dashboard.
- Trả lời: ETL vs ELT, Kimball vs Inmon.

## Services bạn cần bật
```bash
docker compose up -d postgres metabase
```

## Bước 1 — Tạo schema `lab06_dw`
```bash
docker cp lab06_setup.sql de_postgres:/tmp/lab06_setup.sql
docker exec -it de_postgres psql -U de_user -d de_db -f /tmp/lab06_setup.sql
```
Tạo 3 bảng: `dim_customer` (SCD2), `dim_product`, `fact_orders` + load sample rows.

## Bước 2 — Kết nối Metabase
1. Mở `http://localhost:3000`.
2. Setup lần đầu (admin email + password bất kỳ).
3. Add a database:
   - Display name: `Lakehouse DW`
   - Database type: PostgreSQL
   - Host: `postgres`
   - Port: `5432`
   - Database name: `de_db`
   - Username: `de_user`
   - Password: `de_password`
4. Save → Metabase phát hiện schema `lab06_dw`.

## Bước 3 — Tạo 2 questions từ SQL
File tham chiếu: [`lab06_metabase_questions.sql`](../lab06_metabase_questions.sql).

- **Question 1 — Revenue by day (Bar chart)**
```sql
SELECT order_date, SUM(revenue) AS total_revenue
FROM lab06_dw.fact_orders GROUP BY order_date ORDER BY order_date;
```

- **Question 2 — Top customers (Row/Pie)**
```sql
SELECT c.customer_name, SUM(f.revenue) AS revenue
FROM lab06_dw.fact_orders f
JOIN lab06_dw.dim_customer c ON c.customer_key = f.customer_key
WHERE c.is_current = TRUE
GROUP BY c.customer_name ORDER BY revenue DESC LIMIT 10;
```

Save cả hai → Visualization → chọn Bar và Row/Pie tương ứng.

## Bước 4 — Tạo dashboard
1. New → Dashboard `Lab06 Sales Overview`.
2. Add cả 2 questions + (tuỳ chọn) Question 3 (revenue by category) từ file SQL.
3. Save.

## Deliverables
- Ảnh ERD star schema (Mermaid hoặc tool tuỳ chọn).
- File SQL DDL: [`lab06_setup.sql`](../lab06_setup.sql).
- Ảnh 2 questions + 1 dashboard trong Metabase.
- Đoạn viết:
  - Khi nào chọn ETL vs ELT?
  - Khi nào chọn Kimball vs Inmon?
- Khung submission: [`lab06_submission.md`](../lab06_submission.md).

## Self-check
- Modern cloud DWH (BigQuery/Snowflake) đẩy bạn về phía ETL hay ELT? Vì sao?
- Kimball "bottom-up" giao giá trị nhanh — đổi lại phải chấp nhận gì?
- Khi `dim_customer` áp SCD2, query "doanh thu theo khách" của bạn cần điều kiện gì để không bị double-count?
