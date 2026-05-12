# Bài nộp Lab 07 — KIẾN TRÚC DATA LAKEHOUSE 

## 1. Kết quả thực hành
Môi trường đã được khởi tạo và liên kết thành công trên Local Docker với ba Node chính (Object Storage MinIO S3, Hive Metastore & Trino SQL Serverless Engine).

**Ảnh chụp các lệnh Deploy 3 lớp dữ liệu thông qua script `lab07_setup.sql` đã truyền vào Trino Cli:**
```sql
CREATE SCHEMA
CREATE SCHEMA
CREATE SCHEMA
DROP TABLE
DROP TABLE
DROP TABLE
CREATE TABLE
INSERT: 3 rows
CREATE TABLE: 2 rows
CREATE TABLE: 1 row
```

**Ảnh chụp Output quét từ S3 Lakehouse bằng lệnh qua Trino!**
- Lớp **BRONZE** (Chứa Dữ liệu thô Text có dòng mã số 2 bị lỗi Amount dạng chuỗi text `bad_amount`):
```text
"1","101","150.5","2026-01-01"
"2","102","bad_amount","2026-01-02"
"3","101","250.0","2026-01-03"
```
- Lớp **SILVER** (Dữ liệu đã qua trạm lọc TRY CAST của Trino, ép kiểu về DATE/INTEGER/DOUBLE, lưu dưới dạng Parquet Format. Hàng lỗi Null "bad_amount" tại `id = 2` đã bị drop an toàn nhằm duy trì sự trong sạch của Silver Layer):
```text
-- Chỉ còn 2 dòng hợp lệ được bảo tồn!
"1","101","150.5","2026-01-01"
"3","101","250.0","2026-01-03"
```
- Lớp **GOLD** (Truy vấn Aggregate dựa hoàn toàn vào Silver Point of Truth ra tổng số đơn và doanh thu cho từng người dùng):
```text
-- "CustomerID", "Total_Orders", "Total_Revenue"
"101","2","400.5"
```


## 2. Trả lời câu hỏi 

**(1) Lakehouse khác DWH (Data Warehouse) thế nào?**
Data Warehouse sử dụng cơ chế Schema-on-write, chỉ phù hợp lưu trữ dữ liệu tính toán đã có cấu trúc nghiêm ngặt vào các ổ cứng đắt tiền, tối ưu mạnh cho business report.
Lakehouse là bước tiến hóa triệt để khi kết hợp sự rẻ tiền, sức chứa vô cực đủ mọi hình hài (Structured, Images, Audio, Logs) của Data Lake (MinIO/S3), đồng thời đắp thêm lớp áo Table Format (Delta Lake, Apache Iceberg) lên trên cùng. Qua đó nó ban cho các object files vô tri tính chất ACID (thêm, sửa, xóa, roll-back transaction/time-travel an toàn) giống hệt DWH mà không hề hi sinh chi phí linh hoạt nào. Bằng Lakehouse, công ty dùng chung một kho cho cả Machine Learning lẫn Dashboard.

**(2) Hive Metastore dùng để làm gì?**
Hive Metastore đóng vai trò là Cuốn danh bạ trung tâm (Metadata Layer). S3/MinIO thực chất chỉ lưu dữ liệu dưới dạng File (như .parquet, .txt) nằm lộn xộn trong các Folder chứ không có khái niệm Table, Column, Row như database truyền thống. Hive Metastore cất chứa từ điển để giải thích cho các Query Engine (như Trino, Spark) phân giải được File nào thuộc Table nào, Cột 'Amount' là Double hay Varchar, và Partition nào nằm ở ngách nào, qua đó parse thành SQL để end-user query thoải mái.

**(3) S3 Bronze, Silver và Gold khác nhau ra sao?**
Hệ thống Lakehouse (Medallion Architecture) phân loại 3 lớp theo độ tinh khiết:
- **Bronze (Màu đồng / Raw):** Đáy chuỗi, hứng toàn bộ log data, file tải trực tiếp từ nguồn giữ nguyên định dạng, cất tủ nhằm audit và replay lại Pipeline trong tình huống thảm hoạ logic. Rất dơ.
- **Silver (Màu bạc / Cleansed):** Data đã làm sạch rác, ép kiểu thống nhất (VD như script lab lược bỏ row 2 chứa chữ rác), đổi đuôi file sang dạng chuẩn như Parquet định danh cột để query tối đa performance. Đây là SSOT (Single Source of Truth) cho khoa học dữ liệu.
- **Gold (Màu vàng / Curated):** Dữ liệu cao cấp nhất, nhỏ nhẹ, đã được nhóm Group By & Aggregate và sắp xếp thành các cấu trúc Dimension / Fact star schema. Bàn giao thẳng cho sếp xem Dashboard BI tại tầng này!

*(Code DDL query các bảng qua định dạng External S3 Table được nộp trong src file đính kèm!)*
