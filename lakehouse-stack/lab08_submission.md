# Bài nộp Lab 08 — Apache Spark Batch Processing

## 1. Kết quả thiết lập và thực thi Pipeline
Thông qua `docker ps`, em đã sử dụng Spark làm Processing System (Execute Transformation/Compute) song song với MinIO (Object Storage S3) làm Storage Layer trong mô hình Lakehouse. Khởi chạy file code batch job bằng `spark-submit`.

Dữ liệu di chuyển tuần tự như sau:
**S3_RAW (orders.csv) -> Spark Dataframe Memory -> S3_SILVER (Parquet) -> Spark Groupby -> S3_GOLD (Parquet Partition)**


## 2. Nhật ký Data Flow
Quá trình Spark biến đổi dữ liệu thành các chuẩn Silver, Gold đã được Verify:

**- Cấu trúc file RAW_orders.csv:**  
```text
order_id,order_timestamp,quantity,unit_price,order_status,payment_method
1,2026-04-01 10:15:00,5,15.5,COMPLETED,CREDIT_CARD
2,2026-04-01 11:20:00,2,25.0,PENDING,PAYPAL
4,2026-04-02 14:30:00,1,100.0,CANCELLED,CASH
```

**- Spark Dataframe lược hoá rác sang S3 Silver:** Lược trích Date, lọc ra giá trị số dương và Lowercase toàn bộ chữ hoa (Cleansed layer).
```text
+--------+-------------------+--------+----------+------------+--------------+------------+
|order_id|order_timestamp    |quantity|unit_price|order_status|payment_method|gross_amount|
+--------+-------------------+--------+----------+------------+--------------+------------+
|1       |2026-04-01 10:15:00|5       |15.5      |completed   |credit_card   |77.5        |
|2       |2026-04-01 11:20:00|2       |25.0      |pending     |paypal        |50.0        |
|4       |2026-04-02 14:30:00|1       |100.0     |cancelled   |cash          |100.0       |
+--------+-------------------+--------+----------+------------+--------------+------------+
```

**- Spark Dataframe tổng hợp Data Mart về S3 GOLD:** Gom nhóm (`group_by`) doanh thu theo hai trục Ngày và Loại hình thức thanh toán. Thống kê siêu nhẹ.
```text
+----------+--------------+------------+-------------+
|order_date|payment_method|total_orders|total_revenue|
+----------+--------------+------------+-------------+
|2026-04-01|credit_card   |1           |77.5         |
|2026-04-01|paypal        |1           |50.0         |
|2026-04-02|cash          |1           |100.0        |
|2026-04-02|credit_card   |2           |282.0        |
+----------+--------------+------------+-------------+
```

*(Toàn bộ các file kết quả đã được tải lên Bucket minio (`local/lakehouse/silver` & `local/lakehouse/gold`) với trạng thái cắm cờ `_SUCCESS` hoàn tất).*

## 3. Trả lời câu hỏi lý thuyết Lab 08
**Hỏi: Driver làm gì, Executor làm gì trong Spark Hub?**
*   **Driver:** Được ví như Bộ não hay một "Nhạc Trưởng". Nó nắm giữ SparkSession để kết nối user code, đọc mã lệnh khai báo rồi sau đó chuyển đổi/ Tối ưu hoá nó (thông qua DAG Scheduler + Catalyst Optimizer) để băm nhỏ thành hàng trăm Tasks. Cuối cùng, nó giao Task xuống cho bọn Công Nhân làm. Nó theo dõi lịch làm.
*   **Executor:** Chính xác là bầy đàn "Công Nhân" đóng quân trên các server CPUs / Memory trâu bò (Worker Nodes). Lũ này chỉ có nhiệm vụ nai lưng ra cày theo Data Partitions được Driver chỉ định, sau đó cache dữ liệu qua lại trong RAM để luồng ống không bị ngắt quãng.

**Hỏi: Vì sao DataFrame thường được sử dụng và đánh giá tốt hơn RDD?**
*   **Có bảng Schema nghiêm chỉnh:** RDD là chuỗi collection nguyên thuỷ hỗn tạp, chẳng có khái niệm cột tên gì (`[1, 'Alice', 10]`). Mọi filter phải được code chay cồng kềnh `rdd.map(lambda x: x[0] == 1)`. DataFrame có tính SQL-like, bắt buộc phải có Column format (`df.filter(df.id==1)`). Điều đó giúp Data Analyst nào cũng có thể xử dụng Spark ngay lập tức.
*   **Sức mạnh vượt rào của Catalyst Optimizer:** Nhờ bị ép chặt trong Dataframe Schema (Vd: Biết rõ đó là cột kiểu INT), Spark sử dụng con bot tối ưu toán học Catalyst Optimizer. Nó tự động viết lại code của người lập trình lóng ngóng một thành chiến thuật Physical Plans thực thi vòng lặp chạy bằng tầng máy ảo Tungsten C++ hoàn toàn bypass Java Virtual Machine. Nghĩa là việc code dở ở Dataframe không làm nó chạy chậm. Tốc độ DataFrame ăn đứt RDD.
*   **Lazy Evaluation:** DF không làm gì cho tới khi bị ép gọi hàm Report / Print/ Write. Nó gộp nguyên một chuỗi dài lệnh dở hơi của bạn lại làm 1 nhát xử lí.
