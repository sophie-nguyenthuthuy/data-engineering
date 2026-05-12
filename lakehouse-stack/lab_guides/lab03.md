# Lab 03 — Data Modeling for Analytics

## Objectives
- Phân biệt star schema vs snowflake; biết khi nào chọn loại nào.
- Hiểu grain, surrogate key, business key, measure, foreign key, hierarchy.
- Triển khai fact + dimension trên PostgreSQL và apply SCD Type 1 / 2 / 6.
- Tạo một data mart aggregate đầu tiên.

## Services bạn cần bật
```bash
docker compose up -d postgres
```

## Bước 1 — Tạo schema `bootcamp_dw` và các bảng
Dùng file [`lab03_setup.sql`](../lab03_setup.sql):
```bash
docker cp lab03_setup.sql de_postgres:/tmp/lab03_setup.sql
docker exec -it de_postgres psql -U de_user -d de_db -f /tmp/lab03_setup.sql
```

Các bảng được tạo: `dim_date`, `dim_products`, `dim_customers`, `fact_orders`, và cuối cùng là mart `mart_daily_category_sales`.

## Bước 2 — Xác minh schema

```bash
docker exec -it de_postgres psql -U de_user -d de_db -c "\dt bootcamp_dw.*"
docker exec -it de_postgres psql -U de_user -d de_db -c \
  "SELECT f.order_id, c.full_name, p.product_name, d.full_date, f.quantity, f.gross_amount
     FROM bootcamp_dw.fact_orders f
     JOIN bootcamp_dw.dim_customers c ON f.customer_key = c.customer_key
     JOIN bootcamp_dw.dim_products  p ON f.product_key  = p.product_key
     JOIN bootcamp_dw.dim_date      d ON f.date_key     = d.date_key;"
```

## Bước 3 — Thực hành SCD

### SCD Type 1 — overwrite (customer 101 chuyển `Hanoi` → `Haiphong`)
```sql
UPDATE bootcamp_dw.dim_customers
   SET city = 'Haiphong', current_city = 'Haiphong'
 WHERE customer_id = 101 AND current_flag = TRUE;
```
Quan sát: không có version cũ, dữ liệu lịch sử bị ghi đè.

### SCD Type 2 — lưu nhiều version (customer 102 đổi `Danang` → `Hue`)
```sql
UPDATE bootcamp_dw.dim_customers
   SET end_date = '2026-03-31', current_flag = FALSE
 WHERE customer_id = 102 AND current_flag = TRUE;

INSERT INTO bootcamp_dw.dim_customers
  (customer_id, full_name, city, segment, effective_date, end_date,
   current_flag, previous_city, current_city)
VALUES
  (102, 'Bao Tran', 'Hue', 'Retail', '2026-04-01', NULL, TRUE, 'Danang', 'Hue');
```

### SCD Type 6 — kết hợp (customer 103)
Giữ cả `previous_city` (hybrid Type 3) và thêm version mới (Type 2) — cùng một dimension đáp ứng cả 2 nhu cầu phân tích.

## Bước 4 — Query data mart
```bash
docker exec -it de_postgres psql -U de_user -d de_db -c \
  "SELECT * FROM bootcamp_dw.mart_daily_category_sales ORDER BY full_date, category;"
```

## Deliverables
- Ảnh chạy `CREATE SCHEMA` / `CREATE TABLE` thành công.
- Ảnh JOIN fact + 3 dimension.
- Ảnh trước/sau SCD Type 1, SCD Type 2 + mô tả SCD Type 6.
- Ảnh data mart `mart_daily_category_sales`.
- Đoạn viết: star schema vs snowflake — bạn chọn gì cho dashboard doanh thu đầu tiên và vì sao?
- Khung submission: [`lab03_submission.md`](../lab03_submission.md).

## Self-check
- Vì sao phải xác định **grain** trước khi thiết kế fact table?
- Khi nào business key đủ, khi nào cần surrogate key?
- SCD Type 1 / 2 / 6 khác nhau về tác động lên báo cáo lịch sử ra sao?
