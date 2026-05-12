# Lab 08 — Spark Batch Processing

## Objectives
- Hiểu Driver / Executor / Cluster Manager và DAG trong Spark.
- Phân biệt RDD vs DataFrame; hiểu Catalyst + lazy evaluation.
- Xây 1 Spark batch job raw → silver → gold đọc/ghi trên MinIO.

## Services bạn cần bật
```bash
docker compose up -d minio bucket-init spark
```

## Bước 1 — Đẩy input vào Bronze/Raw
```bash
docker exec -it minio sh -c \
  "mc alias set local http://localhost:9000 minio minio12345 && \
   mc cp /tmp/orders.csv local/lakehouse/raw/orders/orders.csv" \
  || docker cp orders.csv minio:/tmp/orders.csv && \
     docker exec -it minio sh -c \
       "mc alias set local http://localhost:9000 minio minio12345 && \
        mc cp /tmp/orders.csv local/lakehouse/raw/orders/orders.csv"
```

## Bước 2 — Review Spark job
File: [`spark/apps/spark_batch_job.py`](../spark/apps/spark_batch_job.py).

Logic:
- **Read raw CSV** từ `s3a://lakehouse/raw/orders/`.
- **Silver**: parse `order_timestamp`, cast `quantity`/`unit_price`, lowercase status, thêm `gross_amount = quantity * unit_price`.
- **Gold**: từ silver, group theo `order_date + payment_method` → `total_orders`, `total_revenue`.

## Bước 3 — Run job
```bash
docker exec -u root -it spark bash -c \
  "spark-submit --packages org.apache.hadoop:hadoop-aws:3.3.4 \
                --conf spark.hadoop.fs.s3a.endpoint=http://minio:9000 \
                --conf spark.hadoop.fs.s3a.access.key=minio \
                --conf spark.hadoop.fs.s3a.secret.key=minio12345 \
                --conf spark.hadoop.fs.s3a.path.style.access=true \
                /opt/bitnami/spark/apps/spark_batch_job.py"
```
Mong đợi terminal in 3 block: `RAW SCHEMA`, `SILVER SAMPLE`, `GOLD SAMPLE`.

## Bước 4 — Xác minh output trên MinIO
Console → `lakehouse/silver/orders/` có `_SUCCESS` + file `*.parquet`.
Console → `lakehouse/gold/daily_sales/` có `*.parquet` tổng hợp theo ngày × payment method.

## Bước 5 — Đọc lại bằng Spark để kiểm tra
```python
spark.read.parquet("s3a://lakehouse/silver/orders/").show()
spark.read.parquet("s3a://lakehouse/gold/daily_sales/").show()
```

## Deliverables
- Ảnh `docker ps` cho thấy `spark` + `minio` đang chạy.
- Ảnh terminal log `spark-submit`.
- Ảnh MinIO `silver/orders/` và `gold/daily_sales/`.
- Trả lời ngắn: Driver vs Executor; DataFrame vs RDD.
- Khung submission: [`lab08_submission.md`](../lab08_submission.md).

## Self-check
- Vì sao Spark dùng lazy evaluation? Nó giúp Catalyst thế nào?
- Catalyst biến logical plan → physical plan ra sao? Nó tối ưu được điều gì tự động?
- Nếu dữ liệu tăng x100, bạn sẽ tối ưu điều gì đầu tiên? (partition, broadcast join, coalesce, cache?)
