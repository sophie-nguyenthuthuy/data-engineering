# Bài nộp Lab 16 — Feature Store Fundamentals with Feast

## Lab Overview
Build an end-to-end feature pipeline:
1. **Build Offline Feature** từ Silver table (parquet)
2. **Register Feature View** (`feast apply`)
3. **Materialize to Online Store** (`feast materialize-incremental`)
4. **Query Online Feature** (`store.get_online_features`)

---

## 1. Feature Definition (`customer_features.py`)

**Lab Step 1: Feature Definition** — hai features chính:
- `customer_order_count`: Tổng số đơn hàng của customer
- `avg_order_value`: Giá trị trung bình mỗi đơn hàng

```python
from datetime import timedelta
from feast import Entity, FeatureService, FeatureView, Field, FileSource
from feast.types import Float64, Int64

customer = Entity(name="customer", join_keys=["customer_id"])

customer_orders_source = FileSource(
    name="customer_orders_source",
    path="data/customer_orders.parquet",
    timestamp_field="event_timestamp",
    created_timestamp_column="created",
)

customer_stats_fv = FeatureView(
    name="customer_order_stats",
    entities=[customer],
    ttl=timedelta(days=14),
    schema=[
        Field(name="customer_order_count", dtype=Int64,
              description="Total number of orders placed by a customer"),
        Field(name="avg_order_value", dtype=Float64,
              description="Average monetary value of customer orders"),
        Field(name="total_revenue", dtype=Float64,
              description="Homework: total revenue = order_count × avg_order_value"),
    ],
    online=True,
    source=customer_orders_source,
    tags={"team": "lakehouse", "layer": "silver"},
)

customer_activity_svc = FeatureService(
    name="customer_activity_v1",
    features=[customer_stats_fv],
)
```

---

## 2. Kết quả `feast apply`

```text
Applying changes for project feature_repo
Created project feature_repo
Created entity customer
Created entity driver
Created feature view driver_hourly_stats_fresh
Created feature view customer_order_stats
Created feature view driver_hourly_stats
Created on demand feature view transformed_conv_rate_fresh
Created on demand feature view transformed_conv_rate
Created feature service driver_activity_v2
Created feature service customer_activity_v1
Created feature service driver_activity_v1
Created feature service driver_activity_v3

Created sqlite table feature_repo_customer_order_stats
Created sqlite table feature_repo_driver_hourly_stats_fresh
Created sqlite table feature_repo_driver_hourly_stats
```

---

## 3. Kết quả `feast materialize-incremental` (Lab Step 2: Materialization)

```text
Materializing 3 feature views to 2026-05-12 00:00:00+00:00 into the sqlite online store.

driver_hourly_stats_fresh from 2026-05-10 12:39:01+00:00 to 2026-05-12 00:00:00+00:00:
0it [00:00, ?it/s]
customer_order_stats from 2026-05-04 12:39:01+00:00 to 2026-05-12 00:00:00+00:00:
100%|████████████████████████████| 10/10 [00:00<00:00, 2873.40it/s]
driver_hourly_stats from 2026-05-10 12:39:01+00:00 to 2026-05-12 00:00:00+00:00:
0it [00:00, ?it/s]
```

10 customer records materialized thành công từ offline (parquet) → online (SQLite).

---

## 4. Kết quả `store.get_online_features` (Lab Step 3: Online Query)

```python
store = FeatureStore(repo_path=".")

feature_vector = store.get_online_features(
    features=[
        "customer_order_stats:customer_order_count",
        "customer_order_stats:avg_order_value",
    ],
    entity_rows=[
        {"customer_id": "C1001"},
        {"customer_id": "C1002"},
        {"customer_id": "C1003"},
    ],
).to_dict()
```

**Output:**
```text
Online Features for Customers:
  C1001: order_count=7, avg_order_value=122.53
  C1002: order_count=15, avg_order_value=113.82
  C1003: order_count=7, avg_order_value=75.19

Raw dict: {'customer_id': ['C1001', 'C1002', 'C1003'], 'customer_order_count': [7, 15, 7], 'avg_order_value': [122.53, 113.82, 75.19]}
```

---

## 5. Homework

### a. Add new feature — `total_revenue`

Thêm feature `total_revenue = customer_order_count × avg_order_value` vào `customer_features.py` (đã có trong phần định nghĩa ở trên).

Dữ liệu parquet được tái sinh với cột `total_revenue`, rồi chạy `feast apply` + `feast materialize` để đẩy lên online store.

### b. Tune TTL — 7 → 14 ngày

TTL được điều chỉnh từ `timedelta(days=7)` lên `timedelta(days=14)` để đảm bảo features không bị expired quá sớm trong các pipeline batch chạy weekly.

```text
Updated feature view customer_order_stats
    ttl: seconds: 604800 -> seconds: 1209600
```

### c. Integrate feature into model (`predict_churn.py`)

Script `predict_churn.py` lấy 3 features từ online store và tính churn score:

```python
store = FeatureStore(repo_path=".")
fv = store.get_online_features(
    features=[
        "customer_order_stats:customer_order_count",
        "customer_order_stats:avg_order_value",
        "customer_order_stats:total_revenue",
    ],
    entity_rows=entity_rows,
).to_dict()

# Heuristic: churn_score = 1 - (order_count/20) - (avg_value/300)
churn_score = max(0.0, 1.0 - (order_count / 20.0) - (avg_value / 300.0))
```

**Output:**
```text
Customer    Orders    AvgVal    Revenue   Score Risk
-------------------------------------------------------
C1001            7    122.53     857.71  0.2416 LOW
C1002           15    113.82    1707.30  0.0000 LOW
C1003            7     75.19     526.33  0.3994 LOW
C1004           11     77.00     847.00  0.1933 LOW
C1005            4     34.29     137.16  0.6857 MEDIUM
C1006            3     17.78      53.34  0.7907 HIGH
C1007            2    112.47     224.94  0.5251 MEDIUM
C1008            6     15.11      90.66  0.6496 MEDIUM
C1009            1     56.07      56.07  0.7631 HIGH
C1010           12     18.11     217.32  0.3396 LOW
```

---

## 6. Trả lời câu hỏi kiến thức

**a. Offline vs Online feature store khác nhau thế nào?**

- **Offline feature store:** Lưu trữ dữ liệu lịch sử quy mô lớn (file parquet, BigQuery, Snowflake). Phục vụ batch processing như tạo training dataset, batch scoring. Ưu tiên read throughput cao và query phức tạp hơn là latency thấp.
- **Online feature store:** Chỉ lưu giá trị mới nhất (latest value) của mỗi entity (Redis, DynamoDB, SQLite). Phục vụ real-time inference với low-latency (milliseconds). Đánh đổi: không query lịch sử được.

**b. Point-in-time correctness dùng để làm gì?**

Point-in-time (PIT) correctness đảm bảo khi xây dựng training dataset, feature values được join đúng với timestamp của event (không dùng dữ liệu tương lai). Ví dụ: nếu event xảy ra lúc T1=9:00AM, chỉ dùng feature values có sẵn tại T1 — không lấy giá trị cập nhật lúc 9:01AM. Điều này ngăn chặn **data leakage** (rò rỉ dữ liệu tương lai vào model training).

**c. Tại sao cần Feature Store trong Lakehouse stack?**

Lakehouse lưu Silver/Gold tables nhưng không giải quyết được vấn đề reuse feature logic giữa nhiều teams và train-serve skew. Feature Store (Feast) đóng vai trò là cầu nối: đọc từ Silver table (offline), materialize sang Redis/SQLite (online), đảm bảo cùng một feature definition được dùng nhất quán cho cả training lẫn real-time inference.
