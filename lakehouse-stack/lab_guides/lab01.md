# Lab 01 — Big Data Engineer Overview

## Objectives
- Giải thích vai trò của Data Engineer trong vòng đời dữ liệu.
- Phân biệt OLTP vs OLAP; Data Warehouse vs Data Lake vs Lakehouse.
- Chuẩn bị môi trường local: Docker + PostgreSQL chạy được.

## Services bạn cần bật
```bash
docker compose up -d postgres
```
Service này là Postgres 15 (container `de_postgres`) — dùng xuyên suốt các lab sau.

## Bước 1 — Xác nhận Docker hoạt động
```bash
docker --version
docker ps
```
Mong đợi: `Docker version 24.x+` và liệt kê container `de_postgres` với status `Up`.

## Bước 2 — Kết nối vào PostgreSQL
```bash
docker exec -it de_postgres psql -U de_user -d de_db -c "SELECT version();"
```
Mong đợi: banner `PostgreSQL 15.x on x86_64-pc-linux-gnu …`.

## Bước 3 — Viết trả lời ngắn (4 câu)
Trong file submission của bạn, trả lời:

1. Vai trò của Data Engineer là gì? (3–5 dòng).
2. Cho 2 use case OLTP + 2 use case OLAP.
3. Vì sao Data Lake phù hợp với Big Data / ML?
4. Vẽ sơ đồ pipeline: `ingestion → storage → processing → BI` (Mermaid hoặc hình).

## Bước 4 — Self-check map tech stack
Điền nhanh bảng sau trước khi nộp:

| Layer          | Ví dụ (sẽ gặp trong bootcamp) |
|----------------|-------------------------------|
| Storage        | PostgreSQL, MinIO             |
| Processing     | Spark, SQL (Trino)            |
| Orchestration  | Airflow                       |
| Streaming      | Kafka, Debezium               |
| BI             | Trino + Metabase              |

## Deliverables
- Ảnh `docker ps` cho thấy `de_postgres` đang chạy.
- Kết quả câu `SELECT version();`.
- File submission trả lời 4 câu + sơ đồ pipeline. Tham khảo khung mẫu tại `lab01_submission.md`.

## Self-check
- Data Engineer khác Data Analyst / Data Scientist ở đâu?
- Khi một team "chỉ cần dashboard tháng", bạn đề xuất DWH hay Lakehouse? Vì sao?
- Nếu bạn chỉ còn 8 GB RAM, bạn tắt service nào trước trong docker-compose này?
