# Lab 02 — SQL Fundamentals → Advanced

## Objectives
- Thành thạo SELECT/WHERE/ORDER BY, 4 loại JOIN, subquery, CTE.
- Dùng window functions (SUM OVER, ROW_NUMBER, RANK/DENSE_RANK).
- Đọc query plan bằng EXPLAIN và tạo index đúng chỗ.

## Services bạn cần bật
```bash
docker compose up -d postgres
```

## Bước 1 — Nạp schema + dữ liệu mẫu + 25 challenge
File đã có sẵn tại repo root: [`lab02_queries.sql`](../lab02_queries.sql). Copy vào container và chạy:

```bash
docker cp lab02_queries.sql de_postgres:/tmp/lab02_queries.sql
docker exec -it de_postgres psql -U de_user -d de_db -f /tmp/lab02_queries.sql
```

Mong đợi: các block `CREATE TABLE`, `INSERT`, các bài 1–25 in kết quả ra terminal (bao gồm `EXPLAIN ANALYZE` cuối file).

## Bước 2 — Tổ chức 25 bài theo 5 nhóm

| Nhóm | Chủ đề                            | Bài     |
|------|-----------------------------------|---------|
| 1    | SELECT / WHERE / ORDER BY         | 1–5     |
| 2    | JOIN (INNER / LEFT / RIGHT / FULL / 3-way) | 6–10 |
| 3    | SUBQUERY / CTE (có CTE đệ quy)    | 11–15   |
| 4    | WINDOW (running total, ranking, Top-N) | 16–20 |
| 5    | OPTIMIZATION (EXPLAIN, INDEX)     | 21–25   |

Chạy lại từng bài riêng với `psql -c "<query>"` nếu muốn chụp từng kết quả.

## Bước 3 — Kiểm tra query plan
Chạy Bài 21 (trước index) và Bài 23 (sau index), so sánh:
```text
Seq Scan on orders          ←  Bài 21 (nhỏ, không có index)
Index Scan using idx_orders ←  Bài 23 (khi dữ liệu đủ lớn)
```
Lưu ý: với 8 dòng `orders`, Postgres có thể vẫn chọn Seq Scan vì rẻ hơn — đó là **đúng**, không phải bug.

## Bước 4 — Capture ít nhất 5 kết quả tiêu biểu
Đề xuất chọn: Bài 10 (3-way JOIN), Bài 13 (2 CTE xếp hạng), Bài 16 (running total), Bài 18 (RANK vs DENSE_RANK), Bài 25 (EXPLAIN GROUP BY vs WINDOW).

## Deliverables
- File `lab02_queries.sql` chứa đủ 25 bài (đã có sẵn).
- ≥5 ảnh output kết quả truy vấn tiêu biểu.
- Đoạn viết ngắn (4 ý): khi nào dùng JOIN / CTE / GROUP BY / WINDOW FUNCTION?
- Khung submission mẫu: [`lab02_submission.md`](../lab02_submission.md).

## Self-check
- GROUP BY giảm số dòng; WINDOW giữ nguyên số dòng — bạn dùng loại nào khi cần cả **chi tiết đơn hàng** lẫn **tổng theo khách** trên cùng 1 bảng?
- Khi nào Postgres chọn Seq Scan dù đã có index?
- CTE đệ quy ở Bài 15 đang giải bài toán gì?
