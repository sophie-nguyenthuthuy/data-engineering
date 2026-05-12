# Lab 04 — Python for Data Engineering

## Objectives
- Sử dụng Pandas cho ETL: extract → transform → load.
- Apply schema enforcement (casting, missing data, validate).
- Viết unit test cho pipeline bằng `pytest`.

## Prerequisites
Python 3.10+ local. Không cần Docker cho bài này.

```bash
cd lab04_python_etl
python3 -m venv .venv
source .venv/bin/activate
pip install pandas pytest
```

## Cấu trúc repo đã có sẵn
```
lab04_python_etl/
├── data/raw/orders.csv            # dữ liệu mẫu có nhiều lỗi
├── data/processed/orders_clean.csv
├── src/etl_pipeline.py
└── tests/test_etl_pipeline.py
```

## Bước 1 — Đọc dữ liệu raw
```bash
python3 - <<'PY'
import pandas as pd
df = pd.read_csv("data/raw/orders.csv")
print(df)
print(df.dtypes)
PY
```
Mong đợi: thấy rõ các dòng bẩn (ngày `bad_date`, amount âm, amount rỗng).

## Bước 2 — Chạy pipeline
```bash
python3 src/etl_pipeline.py
```
Mong đợi:
```text
INFO - Wrote 2 rows to data/processed/orders_clean.csv
```
Pipeline đã:
- Ép `amount` → numeric, `order_date` → datetime với `errors="coerce"`.
- Drop NaN, drop `amount <= 0`.
- Thêm cột `year_month` (YYYY-MM).

## Bước 3 — Xem output
```bash
cat data/processed/orders_clean.csv
```
Chỉ còn 2 dòng hợp lệ (order_id = 1 và 5).

## Bước 4 — Chạy unit test
```bash
pytest -q
```
Mong đợi: `1 passed in 0.xxs` — test kiểm tra số dòng = 2, tất cả `amount > 0`, `order_date` không NaN, có cột `year_month`.

## Bước 5 — Mở rộng (optional, cho điểm cao hơn)
Thêm 1 expectation: `status` chỉ được thuộc set `{paid, pending, refund, shipped, delivered, cancelled}`. Viết thêm 1 test kiểm tra.

## Deliverables
- Ảnh terminal chạy `python src/etl_pipeline.py` thành công.
- File `data/processed/orders_clean.csv`.
- File `src/etl_pipeline.py`, `tests/test_etl_pipeline.py`.
- Ảnh output `pytest -q`.
- Đoạn viết: vì sao schema enforcement quan trọng trong ETL?
- Khung submission: [`lab04_submission.md`](../lab04_submission.md).

## Self-check
- `errors="coerce"` khác `errors="raise"` ở đâu? Khi nào dùng `raise` an toàn hơn?
- Nếu upstream đổi tên cột `amount` → `total`, test của bạn có bắt được không?
- Khi scale lên hàng triệu dòng, bước nào sẽ bị nghẽn? Bạn sẽ chuyển sang tool gì?
