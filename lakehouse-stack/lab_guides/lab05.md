# Lab 05 — Unix / Linux + Shell Scripting

## Objectives
- Thuần thục các lệnh điều hướng, xem/xử lý file: `ls`, `cd`, `cat`, `head`, `tail`, `grep`, `sed`, `awk`.
- Viết mini ETL bằng Bash: validate input → lọc → ghi log.
- Tự động hoá bằng cronjob cơ bản.

## Prerequisites
Shell macOS/Linux (zsh/bash). Không cần Docker.

## Cấu trúc đã có sẵn
```
lab05_shell/
├── incoming/orders.csv        # raw input
├── raw/orders_clean.csv       # cleaned output
├── logs/etl.log               # ETL log
└── scripts/ingest_orders.sh   # pipeline script
```

## Bước 1 — Ôn lại lệnh cơ bản
```bash
cd lab05_shell
pwd
ls -la
cat incoming/orders.csv
head -n 3 incoming/orders.csv
tail -n 2 incoming/orders.csv
wc -l incoming/orders.csv
```

## Bước 2 — Lọc/biến đổi text
```bash
# grep: tìm dòng chứa chuỗi
grep "2024" incoming/orders.csv
grep -c "paid" incoming/orders.csv

# sed: thay thế
sed 's/pending/in_progress/g' incoming/orders.csv

# awk: xử lý cột (CSV)
awk -F',' 'NR>1 {print $2, $4}' incoming/orders.csv
```

## Bước 3 — Chạy mini ETL
```bash
chmod +x scripts/ingest_orders.sh
./scripts/ingest_orders.sh
cat raw/orders_clean.csv
cat logs/etl.log
```

Script làm:
1. Kiểm tra file input tồn tại.
2. Validate header khớp `order_id,customer_id,order_date,amount,status`.
3. Giữ header + các dòng có `amount > 0` bằng `awk`.
4. Ghi log thành công kèm timestamp.

## Bước 4 — Cronjob (demo, không cần thực thi)
```bash
crontab -e
# Thêm dòng sau để chạy mỗi 5 phút:
*/5 * * * * /bin/bash $(pwd)/scripts/ingest_orders.sh >> $(pwd)/logs/cron.log 2>&1
```
Tip: bỏ comment/xoá entry sau khi demo để không spam cron.

## Deliverables
- Ảnh terminal các lệnh `pwd / ls / grep / awk / ./ingest_orders.sh`.
- File `scripts/ingest_orders.sh`.
- Nội dung crontab (hoặc ảnh chụp cron expression).
- Đoạn viết: `grep` khác `awk` thế nào? Vì sao cần `chmod +x`?
- Khung submission: [`lab05_submission.md`](../lab05_submission.md).

## Self-check
- Khi nào dùng `grep/sed/awk` hiệu quả hơn Python?
- Rủi ro gì nếu script ETL không có error handling + logging?
- Khi nào nên bỏ cron để chuyển sang Airflow (Lab 13)?
