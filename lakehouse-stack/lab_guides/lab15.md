# Lab 15 — Data Governance & Metadata (DataHub)

## Objectives
- Hiểu governance vs management; 3 nhóm metadata (technical / business / operational).
- Ingest metadata từ Postgres và Kafka vào DataHub.
- Cấu hình **DataHub Airflow plugin** (push-mode) để Airflow tự emit lineage.
- Xem lineage end-to-end Postgres → Airflow → Postgres.
- Enrich asset bằng owner / tags / glossary.

## Prerequisites
DataHub CLI cần Python **3.10 hoặc 3.11** (Python 3.13 hiện chưa có wheel cho `blis`):
```bash
python3.11 -m venv .venv
source .venv/bin/activate
pip install "acryl-datahub[postgres,kafka]"
```

DataHub stack (không nằm trong `docker-compose.yml` chính; dùng quickstart của DataHub):
```bash
datahub docker quickstart
```
DataHub UI chạy tại `http://localhost:9002` (login `datahub` / `datahub`), GMS REST tại `http://localhost:8080`.

⚠️ **Port conflict:** quickstart bind cổng 9092 cho Kafka riêng → đụng với Kafka của bootcamp. Trước khi chạy quickstart:
```bash
docker stop kafka kafka-ui de_connect
```
Sau lab, restore:
```bash
datahub docker quickstart --stop
docker start kafka kafka-ui de_connect
```

## Bước 1 — Start DataHub
```bash
docker compose up -d postgres airflow-db airflow-init airflow-webserver airflow-scheduler
docker stop kafka kafka-ui de_connect
datahub docker quickstart
```
Đợi `✔ DataHub is now running`.

Init CLI:
```bash
datahub init   # accept default http://localhost:8080, leave token blank
curl -s http://localhost:8080/config | head -c 200    # expect "models" JSON
```

## Bước 2 — Ingest Postgres metadata
File: [`recipes/postgres.yml`](../recipes/postgres.yml).
```bash
datahub ingest -c recipes/postgres.yml
```
Mong đợi `Pipeline finished successfully` + `tables_scanned: 11`.

UI → **Datasets → postgres → de_db** → thấy `public.orders`, `lab06_dw.*`, `bootcamp_dw.*`.

## Bước 3 — Configure Airflow → DataHub plugin (push lineage)
DataHub **không có** pull-based source `airflow`. Thay vào đó cài `acryl-datahub-airflow-plugin` trong container Airflow → plugin tự emit lineage mỗi lần task chạy, dựa vào `inlets/outlets` trên Operator.

`docker-compose.yml` đã cấu hình sẵn các thành phần cần thiết:
- `_PIP_ADDITIONAL_REQUIREMENTS: "acryl-datahub-airflow-plugin==0.14.1.2"` cho 3 service Airflow.
- `AIRFLOW__DATAHUB__ENABLED=true` + connection `datahub_rest_default` → `host.docker.internal:8080`.
- `extra_hosts: host.docker.internal:host-gateway` để container Linux gọi được host (DataHub GMS).

Apply config + recreate container Airflow để plugin được cài:
```bash
docker compose up -d --force-recreate airflow-init airflow-webserver airflow-scheduler
docker logs de_airflow_init --tail 20    # đợi "Airflow DB + DataHub plugin initialized."
```

Verify plugin nạp:
```bash
docker exec --user airflow de_airflow_scheduler airflow plugins | grep -i datahub
# expect: datahub_lineage_plugin / datahub_action_plugin
```

## Bước 4 — Trigger DAG để emit lineage
DAG đã có sẵn `inlets`/`outlets` (xem [`airflow/dags/lab13_pipeline.py`](../airflow/dags/lab13_pipeline.py)):
```python
from datahub_airflow_plugin.entities import Dataset
ingest = BashOperator(
    task_id="ingest_data",
    bash_command="echo ...",
    inlets=[Dataset("postgres", "de_db.public.orders")],
    outlets=[Dataset("postgres", "de_db.bootcamp_dw.fact_sales")],
)
```

Trigger:
```bash
docker exec --user airflow de_airflow_webserver \
  airflow dags trigger lab13_end_to_end_pipeline
```

Đợi DAG hoàn tất (≈ 30s):
```bash
docker exec --user airflow de_airflow_webserver \
  airflow dags list-runs -d lab13_end_to_end_pipeline | head -5
```

## Bước 5 — Xem lineage trong DataHub UI
1. UI search bar → `orders` → click `de_db.public.orders` (postgres).
2. Tab **Lineage** → toggle "Full Lineage".
3. Đồ thị xuất hiện: `postgres.de_db.public.orders → ingest_data → fact_sales → validate_data → transform_data → fact_orders → publish_data → dim_customer`.
4. UI **Pipelines → Airflow → lab13_end_to_end_pipeline** → tab **Tasks** → mỗi task có inlets/outlets riêng.

## Bước 6 — Governance actions
Trên dataset `de_db.public.orders`:
1. **Owners** → Add Owner → assign user `datahub`.
2. **Tags** → Add tag `PII`, `bronze`.
3. **Glossary Terms** → Add Term → tạo mới `Customer Order` với definition.
4. **About** → Edit → 2-line description.

Search tag-based: URL `http://localhost:9002/search?query=tag:PII` → tất cả asset có tag `PII`.

## Bước 7 — (Optional) Crawl Airflow metadata DB
Recipe fallback ingest cấu trúc bảng `dag/dag_run/task_instance` từ Postgres của Airflow (port 5434) — KHÔNG phải lineage:
```bash
datahub ingest -c recipes/airflow.yml
```

## Teardown
```bash
datahub docker quickstart --stop          # stop DataHub
docker start kafka kafka-ui de_connect    # restore bootcamp Kafka
```

## Deliverables
- Ảnh `docker ps` (gồm container `datahub-*`).
- Ảnh DataHub catalog có ≥ 1 dataset Postgres + 1 DAG Airflow.
- Ảnh **Lineage** view với edge từ Postgres → Airflow task → Postgres.
- Ảnh asset đã gán owner/tag/glossary.
- Trả lời 3 câu: metadata là gì, lineage giúp gì, governance khác management ở đâu.
- Khung submission: [`lab15_submission.md`](../lab15_submission.md).

## Self-check
- Vì sao DataHub Airflow integration là **push** (plugin) chứ không **pull** (recipe)?
- Lineage chỉ xuất hiện khi DAG đã **chạy thành công** — đúng hay sai? Vì sao?
- Một cột downstream báo sai — lineage giúp bạn quay lại root cause như thế nào?
- Khi nào business metadata quan trọng hơn technical metadata?
- Nếu không có catalog tập trung, team 50 người sẽ gặp vấn đề gì?
