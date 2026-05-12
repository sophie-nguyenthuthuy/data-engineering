# Bài nộp Lab 17 — Cloud Integration: From Local Stack to AWS

## Lab Artifacts

| File | Mô tả |
|------|-------|
| [`lab17/s3_migration_sim.py`](lab17/s3_migration_sim.py) | Demo: Move Data to S3 — simulation với moto (mock AWS), Bronze/Silver/Gold upload + partition verify |
| [`lab17/athena_queries.sql`](lab17/athena_queries.sql) | Demo: Query via Athena — CREATE EXTERNAL TABLE DDL + sample SQL queries over S3 |
| [`lab17/homework_cloud_architecture.md`](lab17/homework_cloud_architecture.md) | Homework: Capstone AWS architecture diagram, cost estimate, security controls |

---

## 1. Mapping Local Stack -> AWS
Dưới đây là bảng chuyển đổi (mapping) các dịch vụ thuộc kiến trúc Lakehouse xây tại Local sang Amazon Web Services (AWS) tương ứng:

| Lớp kiến trúc (Layer) | Local Stack | AWS Tương đương | Giải thích ngắn |
|---|---|---|---|
| **Storage / Datalake** | **MinIO** | **Amazon S3** | Object storage tương đương nhưng khả năng lưu trữ không giới hạn độ bền 99.999999999%, tách biệt hoàn toàn tính toán và lưu trữ. Nền tảng trung tâm cho file Parquet/Iceberg. |
| **Catalog / Metadata** | **Hive Metastore** | **AWS Glue Data Catalog** | Lưu trữ metadata cấu trúc bảng tập trung. Cho phép tất cả các dịch vụ (EMR, Athena, Redshift Spectrum) dùng chung một Schema Map duy nhất thay vì mạnh ai nấy cấu hình. |
| **Query Engine** | **Trino** | **Amazon Athena** | Nền tảng truy vấn serverless trực tiếp trên S3, không quản lý cụm ảo, hỗ trợ ANSI SQL chuẩn xác giống Trino, cơ chế tính tiền tiết kiệm (Pay-per-query scanned). |
| **Data Processing** | **Spark (Local Docker)**| **Amazon EMR / AWS Glue** | EMR cung cấp Managed Spark cluster cấu hình mạnh, hỗ trợ auto-scale. AWS Glue là Serverless Spark ETL hoàn toàn không cần cấp phát hạ tầng. |
| **Orchestration** | **Cron / Airflow** | **AWS Step Functions** | Step Functions điều phối Serverless Workflows hiệu năng cao thông qua state machine, tích hợp natively với hàng nghìn API nội bộ của AWS. |

## 2. Thiết kế AWS Lakehouse (Bronze / Silver / Gold)
Mô hình lưu trữ trên Amazon S3 Lakehouse dựa vào kiến trúc Tiering Data nhằm tổ chức vòng đời và chất lượng dữ liệu:
*   **Bronze Layer (s3://company-lakehouse-bronze):** Chứa dữ liệu gốc (Raw Data) JSON, CSV đẩy trực tiếp từ Kafka, Postgres CDC mà không đổi cấu trúc. Nơi cách ly dữ liệu thô phục vụ phục hồi nếu pipeline lỗi.
*   **Silver Layer (s3://company-lakehouse-silver):** Dữ liệu đã được làm sạch, gán schema, lọc trùng, chuẩn hóa thành định dạng columnar như Parquet / Apache Iceberg. Đóng vai trò là Single Point of Truth (Sự thật duy nhất) cho Data Scientist thao tác.
*   **Gold Layer (s3://company-lakehouse-gold):** Dữ liệu chuẩn bị xong xuôi phục vụ Analytics. Cấu trúc thường là các bảng Aggregation, Fact/Dim (Báo cáo kinh doanh, ML features, user stats), trực tiếp tương tác bởi Data Analyst trên các Dashboards thông qua Amazon Athena.

**Vai trò của AWS Glue Catalog:** Các file rải khắp S3 Bronze/Silver/Gold sẽ được Glue Crawler quét liên tục, ghi nhận schema vào AWS Glue Data Catalog. Nhờ đó AWS Athena hay Amazon EMR đọc chung 1 bảng siêu dữ liệu mà không cần phải định nghĩa lại cấu trúc thư mục S3 bên dưới.

## 3. Migration Plan: Đưa Local Stack lên Cloud
Thứ tự thực thi triển khai theo 3 giai đoạn để kiểm soát rủi ro:
*   **Bước 1 (Storage Layer - Di chuyển dữ liệu):** Sử dụng các file sync tool (S3cmd, AWS CLI `aws s3 sync`) để bốc toàn bộ dữ liệu từ MinIO đẩy thẳng lên Amazon S3 theo đúng cấu trúc thư mục (chia zone).
*   **Bước 2 (Metadata Layer - Khai báo danh bạ):** Export toàn bộ DDL (Create table script) của các Hive Metastore Database ở Local. Chạy tái định nghĩa chúng trên AWS Glue Data Catalog và cấu hình Location trỏ vào Root path S3 mới tương ứng. Có thể áp dụng các Crawler tự động đi check catalog.
*   **Bước 3 (Compute/Query Layer - Chuyển đổi mã thực thi):**
    *   Trỏ các query BI dashboard từ giao diện Trino -> kết nối JDBC thẳng vào Amazon Athena.
    *   Sửa các script mã nguồn Spark: Đổi toàn bộ endpoint đọc ghi dữ liệu `s3a://<minio-path>` thành native protocol `s3://<aws-path>`.
    *   Dịch chuyển lịch hẹn cronjob hoặc DAG Airflow sang cấu trúc file cấu hình JSON của AWS Step Functions để kích hoạt ETL daily tự động.

## 4. Cost, Security & Governance Considerations

**a. Security (Bảo mật)**
*   **IAM (Identity & Access Management):** Chấp hành chặt chẽ chính sách *Least Privilege* (Đặc quyền tối thiểu). Tool/User nào chạy việc gì thì chỉ cấp quyền đúng thư mục S3 Prefix đó.
*   **Encryption (Mã hóa):** Mã hóa toàn bộ dữ liệu ở trạng thái nghỉ (Data-at-rest) trên S3 với chuẩn SSE-KMS và bắt buộc giao tiếp mã hóa SSL/TLS cho dữ liệu In-transit.

**b. Governance (Quản trị)**
*   Kích hoạt **AWS Lake Formation** để quản trị linh hoạt truy cập dữ liệu tầng cột (Column-level) và dòng (Row-level) nếu gặp dữ liệu nhạy cảm bộ phận khác PII/Credit card.
*   Khai thác tính năng Audit Logging của System thông qua CloudTrail để phát hiện ai đã/đang request vào bucket dữ liệu Raw.

**c. Cost Review (Chi phí & Anti-Patterns)**
*   **Cost factors:** Sử dụng Spot Instances của EMR tiết kiệm 70% đến 90% giá tiền để chạy xử lý Batch. 
*   **Tránh Anti-pattern Over-provisioning:** Không bật cụm EMR 24/7 lãng phí, chỉ bật khi pipeline báo chạy, tắt ngay khi xong. Không chạy query `SELECT *` với Amazon Athena mà không có `WHERE partition_key = ...` vì việc scan nhầm 1TB File Log Parquet không nén có thể tốn ngay $5-$20 đô la cho một giây bất cẩn.
*   **Lifecycle Policy:** Tự động di chuyển dữ liệu Data Lake lạnh (dữ liệu event cũ hơn 3 tháng ở Bronze) sang AWS S3 Glacier (Cold Storage) để rẻ hoá tiền bill hàng tháng.

## 5. Trả lời câu hỏi nhanh
**(1) Vì sao production pipelines cần cloud?**
Bởi tính Reliability và Managed Service của Cloud. Do tính chất môi trường local, máy rất dễ tốn công vận hành (Self-managed), hay lỗi hỏng hóc vật lý và bị giới hạn Disk Space cũng như RAM. Khởi động Production trên cloud giúp doanh nghiệp linh hoạt mở rộng vô tận theo lưu lượng vào (Scalability AWS S3), tăng hiệu quả chi tiêu qua hình thức xài bao nhiêu tính tiền bấy nhiêu thay vì phải duy trì máy ảo cố định.

**(2) Glue Catalog có vai trò gì?**
Nó chịu trách nhiệm là bộ não (Centralized Store) nắm siêu dữ liệu cấu trúc (Metadata) của Data Lake. Nếu không biết File lưu ở đâu và cột là kiểu Int hay String, các Compute Engine như Athena/EMR sẽ không thể biết tìm cách parse nó như thế nào.

**(3) Anti-pattern nào nguy hiểm nhất và vì sao?**
"Scale compute cùng với Scale Storage cứng nhắc" - ở một local self hosted RDBMS, kho chứa dữ liệu thường dính với sức mạnh bộ vi xử lý CPU khiến cấu hình máy rất to nhưng lãng phí, và nó thường online 24/7. Ở Cloud, nếu đem nguyên tư duy này thiết lập trên máy chủ ảo (EC2 / RDS kích cỡ bự 24/7) bỏ xó thì cực kì tốn tiền thay vì tận dụng lợi ích lớn nhất của công nghệ Serverless và Decoupled (Tách biệt tính toán / Lưu trữ pay-as-you-go). Lúc phân tích thì hãy xoay Node; không phân tích thì dữ liệu S3 tĩnh nằm đó không tốn tiền Compute!

---

## 6. Demo: Move Data to S3 — Simulation Output

Script: [`lab17/s3_migration_sim.py`](lab17/s3_migration_sim.py)  
Sử dụng **moto** (mock AWS) — không cần credentials thật, chạy local hoàn toàn.

```text
=================================================================
  Demo: Move Data to S3 — Bronze / Silver / Gold Migration
=================================================================

[1] Created bucket: s3://company-lakehouse/

[2] Uploading Bronze (raw data, no partition):
  uploaded s3://company-lakehouse/bronze/data.parquet  (5 rows)

[3] Uploading Silver (cleaned Parquet, partitioned by date):
  uploaded s3://company-lakehouse/silver/date=2026-04-01/data.parquet  (2 rows)
  uploaded s3://company-lakehouse/silver/date=2026-04-02/data.parquet  (2 rows)

[4] Uploading Gold (aggregated, partitioned by date):
  uploaded s3://company-lakehouse/gold/date=2026-04-01/data.parquet  (1 rows)
  uploaded s3://company-lakehouse/gold/date=2026-04-02/data.parquet  (1 rows)

[5] All objects in bucket:
Key                                                         Size
-----------------------------------------------------------------
  bronze/data.parquet                                       4251 B
  gold/date=2026-04-01/data.parquet                         2941 B
  gold/date=2026-04-02/data.parquet                         2941 B
  silver/date=2026-04-01/data.parquet                       5271 B
  silver/date=2026-04-02/data.parquet                       5264 B

Total objects: 5

[6] Simulate Athena scan — read silver/date=2026-04-02/data.parquet:
 order_id       date  total_amount order_status
        3 2026-04-02         120.0    COMPLETED
        5 2026-04-02         136.5    COMPLETED

[✓] Migration simulation complete.
    In production: replace @mock_aws with real boto3 + IAM role.
```

**Nhận xét partition pruning:**  
Silver layer được chia thành `date=2026-04-01/` và `date=2026-04-02/` — khi Athena query `WHERE date = '2026-04-02'`, chỉ scan đúng file đó (~5KB) thay vì toàn bộ Silver layer. Đây là kỹ thuật tiết kiệm chi phí Athena quan trọng nhất.

---

## 7. Demo: Query via Athena (Conceptual)

SQL file: [`lab17/athena_queries.sql`](lab17/athena_queries.sql)

**Bước 1 — Tạo External Table (Hive-style trên S3):**
```sql
CREATE EXTERNAL TABLE IF NOT EXISTS lakehouse.silver_orders (
    order_id BIGINT, order_timestamp TIMESTAMP,
    quantity INT, unit_price DOUBLE,
    order_status STRING, payment_method STRING, total_amount DOUBLE
)
PARTITIONED BY (date STRING)
STORED AS PARQUET
LOCATION 's3://company-lakehouse/silver/'
TBLPROPERTIES ('parquet.compression' = 'SNAPPY');

MSCK REPAIR TABLE lakehouse.silver_orders;
```

**Bước 2 — Query với partition filter (cost-efficient):**
```sql
SELECT order_id, order_timestamp, total_amount, order_status
FROM   lakehouse.silver_orders
WHERE  date = '2026-04-02'
ORDER  BY total_amount DESC;
```

**Bước 3 — CTAS (Create Table As Select) để persist Gold result:**
```sql
CREATE TABLE lakehouse.gold_payment_summary
WITH (format = 'PARQUET', external_location = 's3://company-lakehouse/gold/payment_summary/')
AS
SELECT payment_method, COUNT(*) AS order_count, ROUND(SUM(total_amount), 2) AS total_revenue
FROM   lakehouse.silver_orders
WHERE  order_status = 'COMPLETED'
GROUP  BY payment_method;
```

---

## 8. Homework — Cloud Architecture, Cost & Security

Xem chi tiết tại: [`lab17/homework_cloud_architecture.md`](lab17/homework_cloud_architecture.md)

**Tóm tắt:**
- **Architecture:** S3 (Bronze/Silver/Gold) → Glue Catalog → Athena/EMR → QuickSight
- **Monthly cost estimate:** ~$25–30/month cho small-medium dataset (100 GB/month ingested)
  - Storage: ~$10.60 | Glue ETL: ~$13.20 | Athena: ~$0.45
- **Top 3 cost levers:** Spot Instances on EMR, S3 Lifecycle → Glacier, partition pruning
- **Security controls:** IAM least-privilege, SSE-KMS encryption, Lake Formation row/column access, CloudTrail audit, AWS Budgets alerts
