# Lab 11 — CDC with Debezium

## Objectives
- Hiểu CDC là gì, khác batch ETL ở đâu.
- Bật logical replication trên PostgreSQL; tạo publication.
- Deploy Debezium PostgresConnector qua Kafka Connect REST API.
- Quan sát event `c / u / d` với `before / after / op`.

## Services bạn cần bật
```bash
docker compose up -d postgres kafka zookeeper kafka-ui connect
```
Kiểm tra:
- `docker ps` — có `de_postgres`, `kafka`, `de_connect`.
- Kafka Connect REST: `curl -s http://localhost:8083/` trả về JSON version.

Note: `docker-compose.yml` đã cấu hình Postgres với `wal_level=logical` (xem dòng `command`).

## Bước 1 — Chuẩn bị bảng nguồn
```bash
docker exec -it de_postgres psql -U de_user -d de_db <<'SQL'
CREATE TABLE IF NOT EXISTS public.orders (
  id SERIAL PRIMARY KEY,
  customer_name TEXT,
  status TEXT,
  amount NUMERIC(10,2)
);
ALTER TABLE public.orders REPLICA IDENTITY FULL;
CREATE PUBLICATION dbz_publication FOR TABLE public.orders;
SQL
```

## Bước 2 — Deploy connector
File sẵn có: [`pg-orders-connector.json`](../pg-orders-connector.json).

```bash
curl -X POST http://localhost:8083/connectors \
     -H "Content-Type: application/json" \
     -d @pg-orders-connector.json
```
Mong đợi: HTTP 201 kèm JSON status.

Kiểm tra trạng thái:
```bash
curl -s http://localhost:8083/connectors/pg-orders-connector/status | jq
```
Phải thấy `"state": "RUNNING"` cho cả connector + task.

## Bước 3 — Sinh event và quan sát

**Terminal A — consume change topic:**
```bash
docker exec -it kafka bash -c \
  "kafka-console-consumer.sh --bootstrap-server localhost:9092 \
     --topic app.public.orders --from-beginning"
```

**Terminal B — sinh INSERT / UPDATE / DELETE:**
```bash
docker exec -it de_postgres psql -U de_user -d de_db <<'SQL'
INSERT INTO public.orders (customer_name, status, amount) VALUES ('Alice', 'created', 120.50);
UPDATE public.orders SET status = 'paid' WHERE customer_name = 'Alice';
DELETE FROM public.orders WHERE customer_name = 'Alice';
SQL
```

Trong terminal A bạn sẽ thấy **3 event JSON** với:

| Event  | `op` | `before` | `after` |
|--------|------|----------|---------|
| INSERT | `c`  | null     | row mới |
| UPDATE | `u`  | row cũ   | row mới |
| DELETE | `d`  | row cũ   | null    |

## Deliverables
- Ảnh `docker ps` (postgres + kafka + connect).
- Response của `POST /connectors` (HTTP 201).
- 3 ảnh event JSON: `c`, `u`, `d`.
- Trả lời 4 câu:
  1. CDC khác batch ETL ở đâu?
  2. Vì sao CDC đỡ tải hơn scan full table?
  3. WAL có vai trò gì?
  4. `before / after / op` để làm gì trong downstream?
- Khung submission: [`lab11_submission.md`](../lab11_submission.md).

## Self-check
- `REPLICA IDENTITY FULL` giúp bạn có được gì trong event DELETE?
- Nếu connector crash, bạn mong muốn resume từ LSN nào? Ai lưu LSN đó?
- Bạn sẽ dùng schema registry khi nào (vượt ra ngoài scope lab này)?
