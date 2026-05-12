# Lab 17 — Cloud Integration: From Local Stack to AWS

> **Design-only lab.** Không cần deploy AWS thật. Mục tiêu là ánh xạ các thành phần local → AWS, vẽ lakehouse trên S3, và viết migration plan + cost/security/governance review.

## Objectives
- Ánh xạ mỗi thành phần trong local lakehouse-stack sang service AWS tương đương.
- Thiết kế Bronze/Silver/Gold trên S3 + AWS Glue Data Catalog.
- Viết migration plan 3 giai đoạn (storage → metadata → compute/query).
- Phân tích cost, security, governance anti-patterns.

## Prerequisites
Chỉ cần trình soạn thảo. Tham khảo `docker-compose.yml` để nhận diện các service local.

## Bước 1 — Vẽ bảng mapping Local → AWS
Điền bảng sau (đáp án mẫu trong [`lab17_submission.md`](../lab17_submission.md)):

| Layer          | Local Stack          | AWS equivalent        | Vì sao?                                               |
|----------------|----------------------|-----------------------|-------------------------------------------------------|
| Storage        | MinIO                | Amazon S3             | Object storage, 11×9 durability, tách compute/storage |
| Catalog        | Hive Metastore       | AWS Glue Data Catalog | Schema tập trung, shared across Athena/EMR/Redshift   |
| Query engine   | Trino                | Amazon Athena         | Serverless SQL trực tiếp trên S3, pay-per-scan        |
| Processing     | Spark (Docker)       | Amazon EMR / AWS Glue | EMR managed Spark; Glue serverless Spark ETL          |
| Orchestration  | Cron / Airflow       | AWS Step Functions / MWAA | State machine serverless; MWAA nếu cần Airflow managed |
| Streaming      | Kafka                | Amazon MSK / Kinesis  | MSK = Managed Kafka; Kinesis = native AWS streaming   |
| CDC            | Debezium + Connect   | AWS DMS               | Managed CDC từ RDS/Aurora                              |
| Metadata/governance | DataHub         | AWS Glue + Lake Formation | Glue catalog + Lake Formation cho row/column ACL    |

## Bước 2 — Thiết kế AWS Lakehouse
**Buckets:**
- `s3://company-lakehouse-bronze/` — raw dump (JSON/CSV từ Kafka, CDC), không đổi schema.
- `s3://company-lakehouse-silver/` — clean + schema-enforced (Parquet/Iceberg), dedupe.
- `s3://company-lakehouse-gold/` — aggregated/fact-dim cho BI + ML features.

**Glue Catalog:** Crawler quét từng prefix → populate schema → Athena/EMR query chung 1 catalog.

**Sơ đồ (ASCII gợi ý):**
```text
Postgres ─┐                                        ┌─> Athena → QuickSight
RDS CDC ──┼─> DMS/MSK ─> S3 Bronze ─Glue ETL─> S3 Silver ─Glue ETL─> S3 Gold ─┤
Apps    ──┘                                        └─> SageMaker training
                          │           │           │
                          └────── Glue Data Catalog ──────┘
                                  (schemas + partitions)
```

## Bước 3 — Migration plan 3 giai đoạn

**Phase 1 — Storage (data first).**
- `aws s3 sync` hoặc `rclone` bốc toàn bộ từ MinIO → S3, giữ nguyên prefix (`bronze/`, `silver/`, `gold/`).
- Verify bằng `aws s3 ls` + hash check.

**Phase 2 — Metadata (catalog).**
- Export DDL từ Hive Metastore (`SHOW CREATE TABLE …`).
- Chạy lại trên Glue qua Athena hoặc `boto3 glue.create_table`.
- Trỏ `LOCATION` về S3 path mới.
- (Tuỳ chọn) Dùng Glue Crawler để auto-discover schema mới.

**Phase 3 — Compute / query.**
- BI: đổi JDBC Trino → Athena connector (cùng dialect, ít sửa SQL).
- Spark jobs: đổi `s3a://` → `s3://`, gỡ `hadoop-aws` config, chạy trên EMR/Glue.
- Airflow DAG → Step Functions (hoặc giữ Airflow qua MWAA nếu team quen).
- Cutover: chạy song song 1-2 tuần, so sánh row count + aggregate hash trước khi tắt local.

## Bước 4 — Cost / Security / Governance review

### Security
- **IAM least-privilege** — role theo team/pipeline, không dùng root.
- **Encryption** — SSE-KMS at-rest cho mọi bucket, TLS in-transit.
- **VPC endpoints** cho S3/Glue để traffic không ra internet.

### Governance
- **AWS Lake Formation** — column/row-level ACL cho PII.
- **CloudTrail** — audit mọi API call vào bucket.
- **Tagging** — `owner`, `env`, `cost-center` cho mọi resource.

### Cost
- **Spot instances** cho EMR batch (tiết kiệm 70-90%).
- **S3 Lifecycle** — Bronze > 90 ngày → Glacier Deep Archive.
- **Athena partitioning** — luôn filter `WHERE dt = '...'`, tránh full scan.
- **Parquet + compression** — giảm bytes scanned → giảm bill.
- **Anti-pattern:** EMR cluster chạy 24/7 dù không có job → bật autoscaling hoặc dùng Glue serverless.

## Deliverables
- Bảng mapping Local → AWS (Bước 1).
- Sơ đồ S3 3-layer + vai trò Glue Catalog (Bước 2).
- Migration plan 3 phase (Bước 3).
- Cost/Security/Governance checklist (Bước 4).
- Trả lời 3 câu:
  1. Vì sao production pipelines cần cloud?
  2. Glue Catalog có vai trò gì?
  3. Anti-pattern nào nguy hiểm nhất và vì sao?
- Khung submission: [`lab17_submission.md`](../lab17_submission.md).

## Self-check
- Compute/storage decoupling trên AWS khác gì RDBMS truyền thống?
- Khi nào chọn EMR vs Glue vs Athena cho cùng một job?
- `s3a://` (Hadoop) khác `s3://` (AWS SDK) ở điểm nào?
- Nếu dataset của bạn là 100 GB scan/ngày trong Athena, chi phí hàng tháng cỡ bao nhiêu ($5/TB scanned)?
- Bạn sẽ bật Lake Formation column-level ACL khi có trường nào?
