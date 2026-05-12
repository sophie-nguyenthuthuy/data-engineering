# Lab 07 — Data Lakehouse Architecture

## Objectives
- Hiểu thành phần Lakehouse: object storage + metadata + query engine + table format.
- So sánh Delta vs Iceberg vs Hudi ở mức khái niệm.
- Triển khai flow 3 lớp Bronze → Silver → Gold bằng MinIO + Hive Metastore + Trino.

## Services bạn cần bật
```bash
docker compose up -d minio bucket-init hive-metastore trino
```
Sau khi up, kiểm tra:
- MinIO Console: `http://localhost:9001` (user `minio` / pass `minio12345`).
- Trino: `http://localhost:8081`.

`bucket-init` sẽ tự tạo bucket `lakehouse/` với prefix `raw/`, `bronze/`, `silver/`, `gold/`.

## Bước 1 — Xác nhận bucket tồn tại
Mở MinIO console → Buckets → `lakehouse`. Bạn phải thấy 4 prefix trống.

## Bước 2 — Nạp dữ liệu Bronze (orders.csv)
```bash
docker cp orders.csv minio:/tmp/orders.csv
docker exec -it minio sh -c \
  "mc alias set local http://localhost:9000 minio minio12345 && \
   mc cp /tmp/orders.csv local/lakehouse/bronze/orders/orders.csv"
```

## Bước 3 — Trino: tạo schema + external tables
File tham chiếu: [`lab07_setup.sql`](../lab07_setup.sql). Chạy trong Trino CLI:

```bash
docker exec -it trino trino --catalog hive --schema default --file /etc/trino/catalog/lab07_setup.sql
```
Hoặc mở Trino UI tại `http://localhost:8081` và copy–paste các câu trong file.

Các bước cốt lõi:
```sql
CREATE SCHEMA IF NOT EXISTS hive.lakehouse WITH (location = 's3a://lakehouse/');
-- Bronze external table pointing to raw CSV
-- Silver: CTAS cleaned data
-- Gold: CTAS aggregate daily sales
```

## Bước 4 — Query cả 3 layer
```sql
SELECT COUNT(*) FROM hive.lakehouse.bronze_orders;
SELECT * FROM hive.lakehouse.silver_orders LIMIT 5;
SELECT * FROM hive.lakehouse.gold_daily_sales ORDER BY order_date;
```

Kiểm tra MinIO → `silver/`, `gold/` có Parquet file xuất hiện.

## Deliverables
- Ảnh bucket + 4 prefix trong MinIO.
- Ảnh Trino query đọc Bronze / Silver / Gold.
- File SQL (`lab07_setup.sql`) đã chạy.
- Trả lời 3 câu:
  1. Lakehouse khác DWH ở đâu?
  2. Hive Metastore dùng để làm gì?
  3. Bronze / Silver / Gold khác nhau ra sao?
- Khung submission: [`lab07_submission.md`](../lab07_submission.md).

## Self-check
- Nếu bạn xoá Hive Metastore, những gì còn lại trong MinIO có dùng được không?
- Delta vs Iceberg vs Hudi — chọn cái nào cho workload streaming-heavy?
- Object storage có 3 khái niệm cốt lõi nào? (bucket / object / prefix)
