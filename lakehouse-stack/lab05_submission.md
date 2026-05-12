# Bài nộp Lab 05 — UNIX / LINUX + SHELL SCRIPTING

## 1. Terminal Screenshots (Mô phỏng logs)
Dưới đây là các kết quả chạy code cho việc điều hướng, tìm kiếm và lọc cột (Data Processing) bằng Linux thuần:

- **Thao tác `pwd` hiển thị thư mục làm việc:**
```text
Thuy@lakehouse-stack % pwd
/Users/Thuy/lakehouse-stack/lab05_shell
```

- **Thao tác liệt kê cấu trúc folder qua `ls -la`:**
```text
Thuy@lakehouse-stack % ls -la
total 0
drwxr-xr-x@  4 Thuy  staff  128 Apr  3 04:14 .
drwxr-xr-x@ 31 Thuy  staff  992 Apr  3 04:14 ..
drwxr-xr-x@  3 Thuy  staff   96 Apr  3 04:14 incoming
drwxr-xr-x@  3 Thuy  staff   96 Apr  3 04:14 scripts
```

- **Thao tác `grep "2024"` (Lọc các record trong năm 2024):**
```text
Thuy@lakehouse-stack % grep "2024" incoming/orders.csv
1,101,2024-01-15,120,completed
2,102,2024-01-16,0,cancelled
3,101,2024-02-02,250,completed
5,104,2024-03-05,180,pending
```

- **Thao tác trích xuất cột 2 và 4 bằng `awk` (loại header):**
```text
Thuy@lakehouse-stack % awk -F',' 'NR>1 {print $2,$4}' incoming/orders.csv
101 120
102 0
101 250
103 300
104 180
```

- **Kết quả chạy `ingest_orders.sh` thành công:**
```text
Thuy@lakehouse-stack % ./scripts/ingest_orders.sh
Thuy@lakehouse-stack % cat raw/orders_clean.csv
order_id,customer_id,order_date,amount,status
1,101,2024-01-15,120,completed
3,101,2024-02-02,250,completed
4,103,2023-12-28,300,completed
5,104,2024-03-05,180,pending

Thuy@lakehouse-stack % cat logs/etl.log
[INFO] ETL success: Fri Apr  3 04:14:43 PDT 2026
```

## 2. Mã nguồn Shell Script
File `ingest_orders.sh` đã được cấp quyền qua lệnh `chmod +x`
```bash
#!/bin/bash
set -e

BASE_DIR="/Users/Thuy/lakehouse-stack/lab05_shell"
INPUT_FILE="$BASE_DIR/incoming/orders.csv"
OUTPUT_FILE="$BASE_DIR/raw/orders_clean.csv"
LOG_FILE="$BASE_DIR/logs/etl.log"

mkdir -p "$BASE_DIR/raw" "$BASE_DIR/logs"

if [ ! -f "$INPUT_FILE" ]; then
  echo "[ERROR] File not found: $INPUT_FILE" >> "$LOG_FILE"
  exit 1
fi

HEADER=$(head -n 1 "$INPUT_FILE")
if [ "$HEADER" != "order_id,customer_id,order_date,amount,status" ]; then
  echo "[ERROR] Invalid schema" >> "$LOG_FILE"
  exit 1
fi

awk -F',' 'NR==1 || $4 > 0 {print $0}' "$INPUT_FILE" > "$OUTPUT_FILE"
echo "[INFO] ETL success: $(date)" >> "$LOG_FILE"
```

## 3. Nội dung Cronjob
Khai báo chạy nền Mini ETL Job cứ mỗi 5 phút một lần từ máy chủ local Linux/Mac:
```bash
crontab -e
*/5 * * * * /bin/bash /Users/Thuy/lakehouse-stack/lab05_shell/scripts/ingest_orders.sh >> /Users/Thuy/lakehouse-stack/lab05_shell/logs/cron.log 2>&1
```

## 4. Trả lời câu hỏi kiến thức ngắn
**grep khác awk thế nào?**
`grep` là công cụ dùng để "Lọc dòng" (Filter rows), nó tìm các dòng text thỏa mãn từ khóa hoặc regex rồi in toàn bộ nguyên vẹn dòng đó ra. Ngược lại, `awk` là một ngôn ngữ lập trình mini dùng cho "Xử lý cột" (Data templating & column processing). Cùng một file CSV, grep rà tìm dòng có số 2024, còn awk có năng lực mổ xẻ lấy riêng cột thứ 2 cộng với cột 4 hay định dạng lại Output.

**Vì sao `chmod +x` cần thiết?**
Trong Unix/Linux, các file tạo ra mặc định chỉ có quyền đọc (Read) và ghi (Write). Hệ điều hành sẽ cương quyết từ chối chạy một file (Permission Denied) nếu nó không có Execution tag (`+x`). Lệnh `chmod +x file.sh` chính là hành động định nghĩa file text này là một "chương trình" để chạy được, bảo vệ hệ điều hành khỏi các kịch bản thực thi vô tính bởi mã độc.
