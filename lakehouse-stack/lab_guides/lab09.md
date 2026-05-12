# Lab 09 — Table Formats & Optimization

## Objectives
- Hiểu table format (Delta / Iceberg / Hudi) là gì — khác file format ra sao.
- Chọn partition key hợp lý; nhận diện good vs bad partitioning.
- Mô phỏng small file problem và áp dụng compaction.
- Hiểu clustering và Z-order ở mức khái niệm.

## Services bạn cần bật
```bash
docker compose up -d minio bucket-init spark hive-metastore trino
```

Prereq: Lab 08 đã ghi silver xong tại `s3a://lakehouse/silver/orders/`.

## Bước 1 — Review job
File: [`spark/apps/lab09_job.py`](../spark/apps/lab09_job.py) chứa 4 phần:
1. Đọc silver → ghi **partitioned gold** theo `order_date`.
2. Mô phỏng **many small files** với `.repartition(20)`.
3. **Compaction** bằng `.coalesce(2)`.
4. **Clustering** bằng `.sort(...).coalesce(2)`.

## Bước 2 — Run
```bash
docker exec -u root -it spark bash -c \
  "spark-submit --packages org.apache.hadoop:hadoop-aws:3.3.4 \
                /opt/bitnami/spark/apps/lab09_job.py"
```

## Bước 3 — Xác minh 4 output trên MinIO
- `lakehouse/gold/daily_sales_partitioned/order_date=YYYY-MM-DD/` — mỗi ngày 1 folder con.
- `lakehouse/silver/orders_many_small_files/` — **nhiều** file `.parquet` nhỏ (mô phỏng vấn đề).
- `lakehouse/silver/orders_compacted/` — chỉ **2** file lớn hơn.
- `lakehouse/silver/orders_clustered/` — sorted + compacted theo `payment_method + order_timestamp`.

## Bước 4 — Đo tác động
Query trên Trino và ghi lại thời gian trả về (dùng `EXPLAIN ANALYZE` hoặc `/v1/query`):

```sql
SELECT COUNT(*) FROM hive.lakehouse.orders_many_small_files;
SELECT COUNT(*) FROM hive.lakehouse.orders_compacted;
```
Quan sát: planning time của query trên nhiều file nhỏ sẽ cao hơn (vì phải list + open nhiều object).

## Deliverables
- Ảnh MinIO **4 đường path** nêu trên.
- File code job (`lab09_job.py`) hoặc notebook.
- Trả lời 4 câu:
  1. Table format khác file format thế nào?
  2. Vì sao partitioning giúp query nhanh hơn? Khi nào nó phản tác dụng (over-partition)?
  3. Small file problem là gì?
  4. Khi nào chọn Delta / Iceberg / Hudi?
- Khung submission: [`lab09_submission.md`](../lab09_submission.md).

## Self-check
- Nếu bạn partition `orders` theo `order_id`, điều tệ gì xảy ra? (cardinality explosion)
- Z-order khác clustering 1 chiều ở đâu?
- Compaction nên chạy đồng bộ (mỗi batch) hay theo lịch riêng?
