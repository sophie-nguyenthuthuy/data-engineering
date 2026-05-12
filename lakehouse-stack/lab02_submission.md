# Bài nộp Lab 02 - SQL Fundamentals đến Advanced

## Đáp án truy vấn (File đính kèm)
Toàn bộ 25 bài SQL challenge được tổ chức theo đúng 5 nhóm của handout và lưu tại `lab02_queries.sql`:

| Nhóm | Chủ đề | Bài |
|------|--------|-----|
| 1 | SELECT / WHERE / ORDER BY | Bài 1–5 |
| 2 | JOIN (INNER/LEFT/RIGHT/FULL + 3-way) | Bài 6–10 |
| 3 | SUBQUERY / CTE (bao gồm CTE đệ quy) | Bài 11–15 |
| 4 | WINDOW FUNCTIONS (running total, ROW_NUMBER, RANK, Top-N) | Bài 16–20 |
| 5 | OPTIMIZATION (EXPLAIN, INDEX, tránh SELECT *) | Bài 21–25 |

Để thiết lập Database, em đã tự định nghĩa và chèn dữ liệu mẫu cho 3 bảng: `customers` (Khách hàng, 5 dòng — có 1 khách chưa mua để minh hoạ LEFT JOIN), `products` (5 dòng) và `orders` (8 dòng).

## Hình ảnh mô phỏng kết quả truy vấn tiêu biểu
(Được chạy trực tiếp từ PostgreSQL Container bằng Docker Exec)

**1. Window Function: Tính tổng Running Total (cộng dồn) theo khách hàng (SUM() OVER)**
```text
 customer_id | order_date | amount  | running_total 
-------------+------------+---------+---------------
           1 | 2026-02-22 |   50.00 |         50.00
           1 | 2026-03-14 |  300.00 |        350.00
           1 | 2026-03-29 | 1500.00 |       1850.00
           2 | 2026-03-24 | 1500.00 |       1500.00
           2 | 2026-04-01 |  100.00 |       1600.00
           3 | 2026-03-19 |  300.00 |        300.00
```
_Giải thích:_ Có thể thấy khách hàng mang ID 1 lũy kế qua từng mốc thời gian order thì tổng tiền chi tiêu càng tăng. Window function làm được điều này mà không gộp mất thông tin ngày mua của các hóa đơn cũ, khác hẳn Group By.

**2. Subquery nâng cao: Tìm các khách chịu chi (hơn mức TB của tất cả khác)**
```text
 customer_id | total_amount 
-------------+--------------
           2 |      1600.00
           1 |      1850.00
```

**3. Tối ưu truy vấn (EXPLAIN ANALYZE)**
Khi kiểm tra execution plan cho lệnh phân tích lịch sử khách số 1:
`EXPLAIN ANALYZE SELECT * FROM orders WHERE customer_id = 1;`
```text
                                           QUERY PLAN                                            
-------------------------------------------------------------------------------------------------
 Seq Scan on orders  (cost=0.00..1.07 rows=1 width=32) (actual time=0.002..0.002 rows=3 loops=1)
   Filter: (customer_id = 1)
   Rows Removed by Filter: 3
 Planning Time: 0.048 ms
 Execution Time: 0.006 ms
```
*(Ghi chú: Vì dữ liệu dummy size quá bé (chỉ có 6 rows) nên Postgres Optimizer cố tình bỏ qua Index mà tự động dùng `Sequential Scan` để tối ưu chi phí khởi tạo cây tìm kiếm. Nếu bảng `orders` chứa hàng triệu dòng, nó sẽ hiện ra `Index Scan` nhờ em đã cài `CREATE INDEX`.)*


## Giải thích kiến thức - Tự đánh giá bản thân

**1. Khi nào dùng từng loại JOIN?**
*   **INNER JOIN**: Lấy số liệu "khớp hoàn toàn" từ cả hai bảng (Ví dụ: danh sách những khách Hàng ĐÃ CÓ hóa đơn đặt mua trong tháng).
*   **LEFT JOIN**: Bảo toàn mọi dữ liệu từ bảng bên TRÁI, nếu phía PHẢI không có thì bù bằng NULL (Ví dụ: Danh sách toàn bộ 1000 tập khách hàng, và số lượng đơn bên cạnh. Khách nào chưa mua thì cột số lượng mang giá trị NULL, tiện cho việc tìm khách hàng chưa chuyển đổi).
*   **FULL OUTER JOIN**: Hợp tất cả các hàng từ cả trái và phải (Phục vụ truy vết chéo, audit data bị orphan).

**2. CTE so với Subquery**
*   Subquery là câu truy vấn lồng nhau đặt ở cụm `FROM` hoặc `WHERE`. Nếu nó chỉ chạy một lần thì dùng rất tiện.
*   **CTE (Common Table Expression - `WITH` clause)**: Giống như việc bạn tạo "Biến" tạm trong SQL. Rất hữu hiệu khi chuỗi tư duy của bạn dài hàng chục step, CTE giúp chia nhỏ block code dễ đọc hơn. Ngoài ra, nếu trong cùng 1 câu tính toán đòi hỏi gọi 1 tập dữ liệu tận 2,3 lần thì gọi tên CTE sẽ DRY (Don't Repeat Yourself) và rõ nghĩa hơn Subquery rối rắm.

**3. GROUP BY khác gì WINDOW FUNCTION?**
*   `GROUP BY` có tính chất thu gọn (collapse) hàng loạt bản ghi về chỉ còn cấu trúc đại diện (Grouping) - Giảm số hàng dữ liệu đầu ra và làm mất chi tiết (mất các column không agg). 
*   `WINDOW FUNCTION (OVER PARTITION BY)` thì thực hiện tính toán hàm Aggregation (SUM, AVG) trên một nhóm hoặc khung thời gian nhất định (Window frames) được định sẵn, nhưng VẪN TRẢ VỀ TOÀN BỘ SỐ LƯỢNG RECORDS NHƯ CŨ, đồng thời in thêm các kết quả tính toán bên cạnh mỗi bản ghi.

Buổi Lab này đòi hỏi tư duy phân tích từng block (CTE->Window), do đó sử dụng các công cụ phân tích cấu trúc execution plan (EXPLAIN) sẽ tạo tiền đề để nâng cấp skill tuning về sau.
