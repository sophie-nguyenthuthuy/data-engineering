# Bài nộp Lab 09 - Table Formats & Optimization in Lakehouse

## 1. Kết quả thực thi các thủ thuật Data Optimization (Spark)
Quá trình phân tách, nén và cải tổ dữ liệu từ S3_SILVER sang các thư mục mô phỏng đã hoàn tất bằng Python PySpark.

- **Task Khởi tạo Partition & Khảo sát thư mục trên MinIO Bucket `lakehouse`:**
Nhật ký Output `mc ls` từ MinIO hiển thị danh sách các prefix thư mục Parquet đã được Spark sinh ra:
```text
[2026-04-03 11:48:21 UTC]     0B orders/
[2026-04-03 11:48:21 UTC]     0B orders_clean/
[2026-04-03 11:48:21 UTC]     0B orders_clustered/
[2026-04-03 11:48:21 UTC]     0B orders_compacted/
[2026-04-03 11:48:21 UTC]     0B orders_many_small_files/
```

- **Task Gold Data Partitioned theo Date:**
Đối với thư mục `gold/daily_sales_partitioned` kết quả cho thấy Spark đã đẩy thành công `_SUCCESS` File và chia Folder theo cây thư mục phân cấp Partition Date để phục vụ Data Skipping:
```text
[2026-04-03 11:48:19 UTC]     0B STANDARD _SUCCESS
[2026-04-03 11:48:21 UTC]     0B order_date=2026-04-01/
[2026-04-03 11:48:21 UTC]     0B order_date=2026-04-02/
```


## 2. Mã Nguồn Spark Pipeline

[lab09_job.py](file:///Users/Thuy/lakehouse-stack/spark/apps/lab09_job.py)
*(Code đã vận hành trên Container: Khởi chạy Partition, mô hình hoá Small File bằng repartition(20), tiến hành Compaction bằng Coalesce(2) và Clustering bằng Sort. Coalesce.)*


## 3. Trả lời câu hỏi lý thuyết Tối Ưu Hoá Lakehouse
**(1) Table Format khác File Format thế nào?**
*   **File Format** (Parquet, ORC, CSV): Giải quyết câu chuyện định dạng lưu trữ file cơ bản bằng byte (lưu data vật lý theo dạng dọc/columnar hay ngang).
*   **Table Format** (Delta Lake, Apache Iceberg, Hudi): Là tầng "Quản trị siêu dữ liệu" (Metadata Layer) ảo đắp lên trên File Format. Metadata cho phép Engine nhận diện hàng trăm nghìn Parquet Files rải rác dưới bùn thành "Một Table duy nhất", cho phép thực hiện cú pháp ACID Update/Delete an toàn trong khi người khác vẫn đang Read, và Schema Evolution. 

**(2) Vì sao partitioning giúp tăng tốc Query?**
Cách thức Partitioning (VD: `order_date=YYYY-MM-DD`) đóng vai trò như Mục Lục từ điển vật lý trên máy. Khi user chạy Query `WHERE order_date = '2026-04-01'`, Engine sẽ tự động nhảy thẳng tới thư mục số 1 và làm lơ (Skip) hàng Tỷ tỷ files Parquet ở những ngày khác. Thuật ngữ này gọi là *Partition Pruning*, giúp Query quét CỰC NHANH nhờ đọc ít dữ liệu dư bọc lót đi qua RAM.

**(3) Small file problem là gì?**
Triệu chứng phát sinh phổ biến trong Streaming khi mỗi 1 phút lại nhả ra 1 file Data bé tẹo 3-4KB. Dần dà Bucket có hàng triệu Files rác.
*Hậu quả:* Quá trình liệt kê danh sách files (Listing overhead) và quá trình đọc Header/Metadata trên từng File tốn RAM nhiều hơn lượng dữ liệu thật sinh ra ngẽn CPU (Out of Memory - Treo Spark). Ta ưu tiên dồn chúng thành các File 100MB-1GB để tối ưu (Qua cơ chế `Compaction/Coalesce`).

**(4) Khi nào sử dụng Delta, Iceberg, hay Hudi?**
*   **Delta Lake:** Ưu tiên khi công ty bạn là fan ruột / dùng hệ sinh thái liên kết trực tiếp vào gã khổng lồ Databricks & Apache Spark ecosystem.
*   **Apache Iceberg:** Ưu tiện chọn làm Lakehouse Open Format. Cấu trúc Metadata siêu việt của nó độc lập hoàn toàn ngôn ngữ / engine dẫn tới Trino, Snowflake, AWS Athena hay Flink đều Read & Write siêu tự do vào nó.
*   **Apache Hudi:** Thiết kế riêng nhắm vào các kiến trúc Streaming Dữ liệu bạo lực liên tục như mảng Grab/Uber. Tối ưu cực mạnh cho thao tác Upsert / Delete liên tục. (Có index BloomFilter rà soát hàng Update rất nhanh).
