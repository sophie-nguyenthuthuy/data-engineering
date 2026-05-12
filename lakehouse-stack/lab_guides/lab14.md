# Lab 14 — Data Quality with Great Expectations + Deequ

## Objectives
- Phân loại 5 data quality dimensions: completeness, accuracy, consistency, timeliness, validity.
- Dùng Great Expectations để viết ≥ 3 rule-based checks.
- Liệt kê Deequ-style statistical metrics: completeness, uniqueness, distribution, size.
- Thiết kế DAG đặt gate validation giữa ingest → transform → publish.

## Prerequisites
Python 3.10+, không cần Docker cho Phần A.
```bash
python3 -m venv .venv && source .venv/bin/activate
pip install pandas 'great_expectations>=0.18'
```

## Phần A — Great Expectations (rule-based)

### Bước A1 — Review script
File: [`ge_validate.py`](../ge_validate.py). Nó đọc `orders.csv` từ repo root và áp các expectation:

| Rule                                         | Dimension    |
|----------------------------------------------|--------------|
| `expect_column_values_to_not_be_null(order_id)`  | completeness |
| `expect_column_values_to_be_unique(order_id)`    | accuracy     |
| `expect_column_values_to_not_be_null(customer_id)`| completeness |
| `expect_column_values_to_be_between(quantity,1,20)`| validity    |
| `expect_column_values_to_be_between(unit_price,0,1000)`| validity |
| `expect_column_values_to_be_in_set(order_status, {...})`| consistency |
| `expect_table_row_count_to_be_between(1, 100000)`| size        |

### Bước A2 — Run
```bash
python3 ge_validate.py
```
Mong đợi: `success: True/False` cùng danh sách expectation pass/fail. Nếu `orders.csv` chứa dòng bẩn cố ý → ít nhất 1 rule fail.

## Phần B — Deequ-style metrics trên Spark
Deequ full runtime cần Scala; ở lab này ta mô phỏng 4 metric families trên PySpark.

File: [`spark/apps/dq_metrics.py`](../spark/apps/dq_metrics.py) — đọc `s3a://lakehouse/silver/orders/` (đã ghi từ Lab 08) và in:

1. **Completeness** — tỉ lệ non-null cho 6 cột.
2. **Uniqueness** — `rows`, `distinct_order_id`, `uniqueness_ratio` (dupes nếu < 1.0).
3. **Distribution** (numeric) — min/max/avg/stddev/p50/p95 cho `quantity` + `unit_price`.
4. **Categorical distribution** — value counts cho `order_status` và `payment_method`.
5. **Size** — tổng row count (so sánh với baseline để detect anomaly).

Chạy:
```bash
docker exec -u root -i spark python3 /opt/bitnami/spark/apps/dq_metrics.py
```
> Note: `-u root` cần thiết vì user mặc định trong container không có `HOME` ghi được — Ivy resolver cần ghi vào `~/.ivy2/`.
hoặc (nếu muốn pass s3a configs ngoài app):
```bash
docker exec -u root spark bash -c \
  "spark-submit --packages org.apache.hadoop:hadoop-aws:3.3.4 \
                --conf spark.hadoop.fs.s3a.endpoint=http://minio:9000 \
                --conf spark.hadoop.fs.s3a.access.key=minio \
                --conf spark.hadoop.fs.s3a.secret.key=minio12345 \
                --conf spark.hadoop.fs.s3a.path.style.access=true \
                /opt/bitnami/spark/apps/dq_metrics.py"
```

Mong đợi: 6 banner sections in liên tiếp, mỗi cái một bảng PySpark `.show()`.

## Phần C — Airflow gate (design)
DAG khung:
```python
ingest >> ge_validate >> transform >> publish
```
Cài `trigger_rule=all_success` cho `transform` → nếu `ge_validate` fail, `transform` skip, giữ bronze sạch khỏi silver/gold.

## Deliverables
- Script/notebook GE đã chạy.
- Screenshot hoặc text output showing pass + fail expectations.
- Danh sách Deequ metrics bạn sẽ monitor + lý do (completeness, uniqueness, distribution, size).
- DAG snippet / design showing vị trí gate validation.
- Khung submission: [`lab14_submission.md`](../lab14_submission.md).

## Self-check
- Rule-based khác statistical ở đâu? Bài toán nào cần mỗi loại?
- Bạn bắt bug ở Bronze hay Gold thì rẻ hơn? Vì sao?
- Làm sao để 1 bronze load sai không làm hỏng silver/gold downstream?
