# Lab 13 — Airflow Orchestration

## Objectives
- Hiểu kiến trúc Airflow (webserver / scheduler / workers / metadata DB).
- Viết DAG gồm ingest → validate → transform → publish.
- Dùng ≥ 2 loại Operator (PythonOperator + BashOperator) và 1 Sensor.
- Cấu hình dependency, retry và trigger rule.

## Services bạn cần bật
```bash
docker compose up -d airflow-db airflow-init airflow-webserver airflow-scheduler
```
Đợi ~30s để `airflow-init` hoàn tất, sau đó:
- Webserver: `http://localhost:8085`
- Login: user `airflow` / pass `airflow`

## Bước 1 — Review DAG đã có
File: [`airflow/dags/lab13_pipeline.py`](../airflow/dags/lab13_pipeline.py) — cấu trúc gợi ý:

```text
ingest (BashOperator)
  └─> wait_for_file (PythonSensor)
        └─> validate (PythonOperator)
              └─> transform (BashOperator)
                    └─> publish (BashOperator, trigger_rule=all_success)
```

Điểm chính:
- `start_date=datetime(2026,1,1)`, `schedule="@daily"`, `catchup=False`.
- Retry: mỗi task `retries=2, retry_delay=timedelta(minutes=1)`.
- `PythonSensor` poll đến khi file `orders.csv` tồn tại.

## Bước 2 — Enable DAG trong UI
1. Mở `http://localhost:8085`.
2. Tìm `lab13_pipeline` → toggle ON.
3. Trigger DAG bằng ▶️ play button.

## Bước 3 — Xem DAG graph + logs
- Tab **Graph** → quan sát dependency.
- Tab **Grid** → click từng task → `Logs` → xem output của Bash/Python.
- Tab **Gantt** → đo latency của từng task.

## Bước 4 — Thử failure path (tuỳ chọn)
Sửa `validate` để raise:
```python
def validate():
    raise ValueError("bad input")
```
Trigger lại DAG → task `publish` sẽ không chạy (vì `trigger_rule=all_success`). Đổi thành `all_done` để quan sát khác biệt.

## Deliverables
- Ảnh Airflow UI với DAG graph.
- File DAG `.py`.
- Ảnh logs của 1 run thành công.
- Trả lời 3 câu:
  1. Airflow khác cron ở đâu?
  2. DAG là gì?
  3. Khi nào dùng Sensor?
- Khung submission: [`lab13_submission.md`](../lab13_submission.md).

## Self-check
- `all_success` vs `one_failed` vs `all_done` — dùng ở tình huống nào?
- `catchup=True` gây ra gì với một DAG có `start_date` lùi xa?
- Webserver + Scheduler + Worker + Metadata DB — nếu bạn tắt Scheduler, chuyện gì xảy ra?
