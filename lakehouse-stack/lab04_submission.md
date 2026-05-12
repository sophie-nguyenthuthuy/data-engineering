# Bài nộp Lab 04 — Python cho Data Engineering (Mini ETL Pipeline)

## 1. Kết quả thực thi Pipeline
Đoạn trích log terminal thể hiện script đã Extract, Transform (Schema enforcement, lọc các dòng Missing/Lỗi) và Load bằng việc ghi thư mục `orders_clean.csv`:

```text
Thuy@lakehouse-stack % python src/etl_pipeline.py
INFO - Wrote 2 rows to data/processed/orders_clean.csv
```

## 2. Kết quả Unit Test (Pytest)
Pytest đã giả lập mô hình để kiểm tra việc Dataframe lọc chuẩn chỉ không có số bé hơn 0 hoặc null date. Cả 4 bài test nhỏ trong function đều Passed.

```text
Thuy@lakehouse-stack % pytest -q tests/test_etl_pipeline.py
INFO - Wrote 2 rows to /Users/Thuy/lakehouse-stack/lab04_python_etl/data/processed/orders_clean.csv
.                                                                        [100%]
1 passed in 11.18s
```

## 3. Nội dung dữ liệu đã xử lý `orders_clean.csv`
Từ 5 record thô chứa lỗi (như amount rỗng, ngày điền sai `bad_date` hoặc amount bị âm `-50`), Pipeline đã dọn dẹp và chắt lọc thành công 2 dòng dữ liệu sạch hợp lệ như sau:
```csv
order_id,customer_id,order_date,amount,status,year_month
1,101,2026-01-01,100.5,paid,2026-01
5,104,2026-01-04,300.75,paid,2026-01
```

## 4. File Source Code

### Mã nguồn `src/etl_pipeline.py`
[etl_pipeline.py](file:///Users/Thuy/lakehouse-stack/lab04_python_etl/src/etl_pipeline.py)
```python
from pathlib import Path
import logging
import pandas as pd

logging.basicConfig(level=logging.INFO, format="%(levelname)s - %(message)s")

RAW_PATH = Path("data/raw/orders.csv")
OUT_PATH = Path("data/processed/orders_clean.csv")

def run_etl() -> pd.DataFrame:
    if not RAW_PATH.exists():
        raise FileNotFoundError(f"Missing input file: {RAW_PATH}")

    df = pd.read_csv(RAW_PATH)
    required_cols = {"order_id", "customer_id", "order_date", "amount", "status"}
    if not required_cols.issubset(df.columns):
        missing = required_cols.difference(df.columns)
        raise ValueError(f"Missing columns: {missing}")

    df["amount"] = pd.to_numeric(df["amount"], errors="coerce")
    df["order_date"] = pd.to_datetime(df["order_date"], errors="coerce")
    df = df.dropna(subset=["amount", "order_date"])
    df = df[df["amount"] > 0].copy()
    df["year_month"] = df["order_date"].dt.strftime("%Y-%m")

    OUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(OUT_PATH, index=False)
    logging.info("Wrote %s rows to %s", len(df), OUT_PATH)
    return df

if __name__ == "__main__":
    run_etl()
```

### Mã nguồn `tests/test_etl_pipeline.py`
[test_etl_pipeline.py](file:///Users/Thuy/lakehouse-stack/lab04_python_etl/tests/test_etl_pipeline.py)
```python
import sys
from pathlib import Path

# Add lab04_python_etl directory to sys.path
sys.path.append(str(Path(__file__).parent.parent))

from src.etl_pipeline import run_etl

def test_run_etl_returns_clean_rows():
    df = run_etl()
    assert len(df) == 2
    assert (df["amount"] > 0).all()
    assert df["order_date"].notna().all()
    assert "year_month" in df.columns
```


## 5. Trả lời câu hỏi lý thuyết
**Hỏi: Vì sao schema enforcement quan trọng trong ETL?**

**Đáp:** Schema enforcement đóng vai trò là "người gác cổng" bảo vệ sự ổn định và đáng tin cậy của toàn hệ thống phân tích. 
1. **Phòng tránh "Rác Vô, Rác Ra" (Garbage In - Garbage Out):** Nhờ cơ chế validation/ép kiểu nghiêm ngặt từ sớm (Ví dụ: Order_Date phải là kiểu datetime), các hệ thống báo cáo (Downstream) sẽ không bị sụp đổ bởi các phép tính sai số học vì dính input chữ hay format file lạ (`bad_date`).
2. **Fail Fast - Báo lỗi chủ động sớm nhất:** Cơ chế rào chắn Schema giúp dừng ngay luồng ETL và ra notification cảnh báo hỏng hóc từ khâu đọc file Raw CSV (E của ETL). Thay vì tải đống dữ liệu không tương thích đó vô DWH, tốn compute để rồi nổ banh chành Data Mart ở chuỗi pipeline muộn.
3. **Thúc đẩy tự động hoá tĩnh (Resiliency):** Việc chủ động lường trước được các datatype, mình có thể áp dụng các hàm như ép chuyển lỗi sang `NaN` và `dropna()`. Việc này giúp hệ thống tự làm sạch dị vật và lọt qua luồng pipeline mỗi ngày mà không cần Data Engineer can thiệp fix lỗi thủ công.
