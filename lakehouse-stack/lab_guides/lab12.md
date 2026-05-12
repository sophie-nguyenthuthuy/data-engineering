# Lab 12 — Streaming Processing (Spark Structured Streaming)

## Objectives
- Phân biệt batch vs streaming, stateless vs stateful.
- Hiểu event time, processing time, late events, watermark, windowing.
- Build pipeline Kafka → Spark Structured Streaming → Parquet (MinIO).
- Hiểu vì sao `checkpointLocation` bắt buộc.

## Services bạn cần bật
```bash
docker compose up -d zookeeper kafka kafka-ui minio bucket-init spark
```

## Bước 1 — Tạo topic streaming
```bash
docker exec -it kafka bash -c \
  "kafka-topics.sh --bootstrap-server localhost:9092 --create \
     --topic orders_stream --partitions 3 --replication-factor 1 && \
   kafka-topics.sh --bootstrap-server localhost:9092 --describe --topic orders_stream"
```

## Bước 2 — Review streaming job
File: [`spark/apps/stream_orders.py`](../spark/apps/stream_orders.py).
- `readStream.format("kafka")` từ topic `orders_stream`.
- `from_json` parse JSON → schema `order_id, customer_id, amount, event_time`.
- `withWatermark("event_time", "5 minutes")` + `window("5 minutes")` → aggregate `total_orders`, `total_amount` per `customer_id`.
- `writeStream.format("parquet")` sink → `s3a://lakehouse/gold/orders_streaming/` + checkpoint `s3a://lakehouse/checkpoints/orders_streaming/`.

## Bước 3 — Chạy job (terminal A)
```bash
docker exec -u root -it spark bash -c \
  "spark-submit --packages org.apache.hadoop:hadoop-aws:3.3.4,org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0 \
                --conf spark.hadoop.fs.s3a.endpoint=http://minio:9000 \
                --conf spark.hadoop.fs.s3a.access.key=minio \
                --conf spark.hadoop.fs.s3a.secret.key=minio12345 \
                --conf spark.hadoop.fs.s3a.path.style.access=true \
                /opt/bitnami/spark/apps/stream_orders.py"
```
Mong đợi: log `Micro-batch 0 completed`, query ở trạng thái RUNNING.

## Bước 4 — Produce sample events (terminal B)
```bash
docker exec -it kafka bash -c \
  "kafka-console-producer.sh --bootstrap-server localhost:9092 --topic orders_stream"
```
Paste:
```json
{"order_id": 1, "customer_id": 101, "amount": 15.5, "event_time": "2026-03-01T10:00:00"}
{"order_id": 2, "customer_id": 101, "amount": 20.0, "event_time": "2026-03-01T10:01:00"}
{"order_id": 3, "customer_id": 102, "amount": 12.0, "event_time": "2026-03-01T10:07:00"}
```

## Bước 5 — Xác minh output
Kiểm tra MinIO console:
- `lakehouse/gold/orders_streaming/` — xuất hiện Parquet file sau micro-batch đầu.
- `lakehouse/checkpoints/orders_streaming/` — có `commits/`, `offsets/`, `sources/`, `state/`.

## Deliverables
- Ảnh `docker ps` (kafka + spark + minio).
- Ảnh topic `orders_stream` trong Kafka UI + vài event.
- File `stream_orders.py`.
- Ảnh log micro-batch + query state.
- Ảnh MinIO output + checkpoint.
- Trả lời 5 câu:
  1. Batch vs streaming?
  2. Stateless vs stateful?
  3. Event time vs processing time?
  4. Vì sao watermark quan trọng?
  5. Khi nào chọn Flink thay Spark Structured Streaming?
- Khung submission: [`lab12_submission.md`](../lab12_submission.md).

## Self-check
- Nếu bạn bỏ `checkpointLocation`, điều gì xảy ra khi job restart?
- Micro-batch của Spark Structured Streaming có latency khoảng bao nhiêu? Flink thì sao?
- `withWatermark("event_time", "5 minutes")` giới hạn điều gì về memory state?
