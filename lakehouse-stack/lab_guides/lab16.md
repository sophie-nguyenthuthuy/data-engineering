# Lab 16 — Feature Store Fundamentals (Feast)

## Objectives
- Phân biệt offline store (historical, batch training) vs online store (latest value, low-latency serving).
- Định nghĩa `Entity`, `FileSource`, `FeatureView` và đăng ký với registry.
- Materialize features từ offline → online store.
- Retrieve online features theo `entity_rows` cho realtime inference.
- Hiểu **point-in-time correctness** và vì sao nó ngăn data leakage.

## Prerequisites
Không cần Docker. Chỉ Python 3.9+ và môi trường ảo:
```bash
python3 -m venv feast_env
source feast_env/bin/activate
pip install "feast[sqlite]"
```

## Bước 1 — Khởi tạo repo Feast
Từ project root:
```bash
cd feature_repo
feast init -t local feature_repo  # đã chạy sẵn, chỉ inspect nếu muốn
cd feature_repo
```
Cấu trúc đã có:
- `feature_store.yaml` — config offline=`file`, online=`sqlite`.
- `example_repo.py` — định nghĩa `driver` entity + `driver_hourly_stats` FeatureView.
- `data/driver_stats.parquet` — sample offline data (timestamped rows).
- `fetch_feature.py` — script retrieve online feature.

## Bước 2 — Review định nghĩa Feature View
File: [`feature_repo/feature_repo/example_repo.py`](../feature_repo/feature_repo/example_repo.py). Điểm chính:
```python
driver = Entity(name="driver", join_keys=["driver_id"])

driver_stats_source = FileSource(
    name="driver_hourly_stats_source",
    path="data/driver_stats.parquet",
    timestamp_field="event_timestamp",
    created_timestamp_column="created",
)

driver_stats_fv = FeatureView(
    name="driver_hourly_stats",
    entities=[driver],
    ttl=timedelta(days=1),
    schema=[
        Field(name="conv_rate", dtype=Float32),
        Field(name="acc_rate", dtype=Float32),
        Field(name="avg_daily_trips", dtype=Int64),
    ],
    online=True,
    source=driver_stats_source,
)
```

## Bước 3 — Apply
```bash
feast apply
```
Mong đợi log:
```text
Created entity driver
Created feature view driver_hourly_stats
Created sqlite table feature_repo_driver_hourly_stats
```

## Bước 4 — Materialize offline → online
```bash
CURRENT_TIME=$(date -u +"%Y-%m-%dT%H:%M:%S")
feast materialize-incremental $CURRENT_TIME
```
Mong đợi progress bar `5/5` (đúng số rows trong parquet).

## Bước 5 — Retrieve online feature
```bash
python3 fetch_feature.py
```
Mong đợi:
```text
Online Feature for Driver 1001:
{'driver_id': [1001], 'avg_daily_trips': [874], 'acc_rate': [0.1804...], 'conv_rate': [0.9914...]}
```

## Bước 6 — Historical retrieval (tuỳ chọn)
Để build training set point-in-time correct:
```python
from feast import FeatureStore
import pandas as pd
store = FeatureStore(repo_path=".")
entity_df = pd.DataFrame({
    "driver_id": [1001, 1002, 1003],
    "event_timestamp": pd.to_datetime(["2026-04-01 10:00", "2026-04-01 10:00", "2026-04-01 10:00"]),
})
training_df = store.get_historical_features(
    entity_df=entity_df,
    features=["driver_hourly_stats:conv_rate", "driver_hourly_stats:avg_daily_trips"],
).to_df()
print(training_df)
```
Feast đảm bảo join chỉ lấy feature value có `event_timestamp ≤` thời điểm trong `entity_df`, tránh rò rỉ future.

## Deliverables
- Output của `feast apply`.
- Output của `feast materialize-incremental`.
- Snippet `example_repo.py` (Entity + FeatureView).
- Output của `fetch_feature.py`.
- Trả lời 2 câu:
  1. Offline vs online feature store khác nhau ở đâu?
  2. Point-in-time correctness dùng để làm gì?
- Khung submission: [`lab16_submission.md`](../lab16_submission.md).

## Self-check
- Vì sao online store chỉ lưu **latest** value thay vì toàn bộ lịch sử?
- `ttl=timedelta(days=1)` nghĩa là gì trong ngữ cảnh materialize?
- Nếu bạn gọi `get_online_features` cho `driver_id=9999` (không có trong parquet), điều gì xảy ra?
- Training/serving skew là gì, feature store giúp giải quyết thế nào?
