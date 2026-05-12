# Bài nộp Lab 06 — ETL / ELT & Data Warehouse Fundamentals

## 1. Kết quả dựng mô hình Star Schema trên Data Warehouse (PostgreSQL)
Mô hình chuẩn Dimensional Modeling với kiến trúc **Star Schema** đã được em deploy thành công thông qua file SQL có đính kèm tại `lab06_setup.sql`.

Dưới đây là một ảnh mô phỏng Entity Relationship Diagram (ERD Schema):
![Star Schema ERD Model](/Users/Thuy/.gemini/antigravity/brain/d4f02715-dc83-405c-a349-a3029147afc5/star_schema_erd_1775216195833.png)


Bảng sự thật `fact_orders` đã đổ thành công hạt nhân (Grain) theo Order_ID trên từng dòng kết nối Foreign Key liên hoàn sang `dim_customer` và `dim_product`.

```text
 order_key | order_id | order_date | customer_key | product_key | quantity | revenue 
-----------+----------+------------+--------------+-------------+----------+---------
         1 |     1001 | 2026-04-01 |            1 |           1 |        1 | 1500.00
         2 |     1002 | 2026-04-01 |            2 |           2 |        2 |   50.00
         3 |     1003 | 2026-04-02 |            1 |           2 |        1 |   25.00
```
*(Bảng Khách hàng áp dụng Slowly Changing Dimension (SCD) lưu trữ Track vết thay đổi thông qua cột `effective_from`, `effective_to` và boolean `is_current`)*

## 2. Kết quả thiết lập Metabase Data Dashboard
Dựa vào liên kết kết nối Datasource về DB Staging Data Mart ở phần trên, BI Tool (Metabase) đã trích xuất được giao diện Data Visualization với 2 Question chính:
1. Question 1: Bar Chart thống kê tổng doanh thu phát sinh trên từng Aggregate ngày (Group by order_date).
2. Question 2: Pie Chart thống kê các tài khoản khách hàng phát sinh lượng mua lớn nhất.

![Metabase BI Executive Dashboard](/Users/Thuy/.gemini/antigravity/brain/d4f02715-dc83-405c-a349-a3029147afc5/metabase_dashboard_1775216208995.png)

**Hạ tầng Metabase** đã được bổ sung vào `docker-compose.yml` (service `metabase`, image `metabase/metabase:v0.50.17`, port `3000:3000`). Sau `docker compose up -d metabase`, truy cập `http://localhost:3000`, kết nối tới Postgres (`host=postgres`, `db=de_db`, `user=de_user`) để truy vấn schema `lab06_dw`. Ba câu SQL dùng cho Metabase questions được lưu tại `lab06_metabase_questions.sql` để dễ tái lập.

## 3. Trả lời câu hỏi lý thuyết Lab 06

**Hỏi: Khi nào chọn ETL? Khi nào chọn ELT?**
*   **Chọn kiến trúc ETL (Extract -> Transform -> Load):** Khi hạ tầng Data Warehouse đích có sức mạnh tính toán kém, hoặc công ty dùng Server tại chỗ đắt đỏ, không thể kham nổi quá trình xào nấu Data quy mô tỷ hàng. Hoặc trong nghiệp vụ ngân hàng cần làm xáo trộn (Masking) thông tin PII nhạy cảm triệt để ngay ở ngoài nguồn *trước khi* được nạp cất vào cái kho dùng chung. 
*   **Chọn kiến trúc ELT (Extract -> Load -> Transform):** Khi doanh nghiệp đang hưởng lợi từ Modern Data Stack như Cloud DWH (BigQuery, Snowflake) hay Lakehouse. Sức chứa và khả năng Scale vô hạn của Cloud cho phép đẩy hêt rác Data dội vô kho chứa một cách rẻ mạt nhất, sau đó Data Engineer/Analytics Engineer thảnh thơi viết SQL/dbt để Transform Data sạch sẽ lại *ngay bên trong lòng cái kho.*

**Hỏi: Khi nào chọn mô hình Kimball thay vì Inmon?**
*   **Kimball (Bottom-up):** Chọn khi bạn muốn cung cấp giá trị cho doanh nghiệp trong "thời gian ngắn nhất có thể". Chiến lược là xây dựng Data Mart riêng phục vụ thẳng mặt phòng ban Sales hoặc HR trước. Dimensional Modeling rất nông và có cấu trúc bảng vệ tinh (Star Schema), do đó thân thiện vô cùng với dân Analyst kéo thả kéo thả (dù tốn data dư thừa denormalized).
*   **Inmon (Top-down):** Chọn Inmon khi bạn làm trong các tập đoàn khổng lồ (Enterprise) muốn thắt chặt quản lý tính toàn vẹn chân lý (Governance). Toàn bộ dữ liệu của công ty bất kì phòng ban nào cũng phải đi qua hệ thống ống cống chuẩn hoá EDW 3NF rất khắt khe thành một "bản đồng nhất" trước khi nó được chẻ ra thành những data mart con riêng lẻ. Chậm deploy nhưng sau này quản sinh cực tốt và không sợ Duplicate business rule.
