# Homework — Cloud Architecture for Capstone Project

## 1. AWS Lakehouse Architecture (Capstone Design)

```
┌──────────────────────────────────────────────────────────────────────────┐
│                       AWS LAKEHOUSE — CAPSTONE                           │
│                                                                          │
│  INGEST                STORE               PROCESS         SERVE         │
│  ──────                ─────               ───────         ─────         │
│                                                                          │
│  Postgres CDC ──►  S3 Bronze  ──► AWS Glue ETL ──► S3 Silver            │
│  (orders DB)       (raw CSV/       (PySpark       (clean Parquet         │
│                     JSON)           job)           + partitioned)        │
│                                                         │                │
│  Kafka Streams ──► Kinesis                    AWS Glue ETL ──► S3 Gold  │
│  (real-time        Firehose                   (aggregation    (daily     │
│   events)          → S3                        job)           summaries) │
│                                                         │                │
│                    AWS Glue Data Catalog ◄──────────────┘                │
│                    (schema registry,                                     │
│                     partition metadata)                                  │
│                           │                                              │
│                    ┌──────┴──────┐                                       │
│                    │             │                                       │
│               Amazon Athena   Amazon EMR                                 │
│               (serverless SQL  (batch Spark                              │
│                over S3)         jobs)                                    │
│                    │                                                     │
│               Amazon QuickSight / Metabase (BI dashboards)               │
│                                                                          │
│  ORCHESTRATION: AWS Step Functions (replaces Airflow DAGs)               │
│  SECURITY:      IAM Roles + S3 Bucket Policies + KMS encryption         │
│  MONITORING:    CloudWatch + AWS Cost Explorer + CloudTrail audit logs   │
└──────────────────────────────────────────────────────────────────────────┘
```

### Component Mapping (Local Stack → AWS)

| Layer | Local Stack | AWS Service | Notes |
|---|---|---|---|
| Object Storage | MinIO | **Amazon S3** | Infinite scale, 99.999999999% durability |
| ETL / Processing | Spark (Docker) | **AWS Glue / EMR** | Glue = serverless; EMR = managed cluster |
| Streaming Ingest | Kafka | **Amazon Kinesis** | Kinesis Firehose → S3 Bronze auto-delivery |
| Metadata Catalog | Hive Metastore | **AWS Glue Data Catalog** | Single schema store for Athena + EMR |
| Query Engine | Trino | **Amazon Athena** | Serverless SQL, pay-per-TB-scanned |
| Orchestration | Airflow | **AWS Step Functions** | Native AWS service integration |
| BI / Dashboards | Metabase | **Amazon QuickSight** | Or keep Metabase via Athena JDBC |
| Feature Store | Feast (local) | **Feast on EMR + Redis ElastiCache** | Offline=S3, Online=ElastiCache Redis |


---

## 2. Cost Estimate (High-Level, ap-southeast-1 region)

> Assumptions: small-to-medium e-commerce dataset, ~100 GB/month ingested,
> ~500 GB total stored, 10 Glue jobs/day, 100 Athena queries/day.

### Storage (Amazon S3)

| Tier | Volume | $/GB/month | Monthly Cost |
|------|--------|-----------|-------------|
| Bronze (Standard) | 200 GB | $0.025 | **$5.00** |
| Silver (Standard) | 150 GB | $0.025 | **$3.75** |
| Gold (Standard) | 50 GB | $0.025 | **$1.25** |
| Bronze >90 days → S3 Glacier | 150 GB | $0.004 | **$0.60** |
| **Storage total** | | | **~$10.60/month** |

### Compute (AWS Glue ETL)

| Job | DPU | Hours/run | Runs/month | Cost/DPU-hr | Monthly |
|-----|-----|-----------|-----------|-------------|---------|
| Bronze→Silver | 2 | 0.25 | 30 | $0.44 | **$6.60** |
| Silver→Gold | 2 | 0.25 | 30 | $0.44 | **$6.60** |
| **Glue total** | | | | | **~$13.20/month** |

### Query (Amazon Athena)

| Pattern | TB scanned/query | Queries/month | Cost/TB | Monthly |
|---------|-----------------|---------------|---------|---------|
| Partitioned Silver queries | 0.001 TB | 80 | $5.00 | **$0.40** |
| Full-scan Gold queries | 0.0005 TB | 20 | $5.00 | **$0.05** |
| **Athena total** | | | | **~$0.45/month** |

### Total Estimated Monthly Cost: **~$25–30/month**

> Savings levers:
> - Use **Spot Instances** on EMR for batch jobs → save 70–90% vs On-Demand
> - Enable **S3 Intelligent-Tiering** on Bronze after 30 days
> - Add **S3 Lifecycle Policy**: Bronze → Glacier after 90 days
> - Use **columnar + compressed Parquet (SNAPPY)** to minimize Athena scan cost


---

## 3. Security Controls

### IAM — Least Privilege per Service

```
Role: glue-etl-role
  ├── s3:GetObject   → s3://company-lakehouse/bronze/*
  ├── s3:PutObject   → s3://company-lakehouse/silver/*
  └── glue:*         → arn:aws:glue:*:*:catalog

Role: athena-analyst-role
  ├── s3:GetObject   → s3://company-lakehouse/silver/*
  ├── s3:GetObject   → s3://company-lakehouse/gold/*
  ├── s3:PutObject   → s3://athena-query-results/*
  └── athena:StartQueryExecution, GetQueryResults

Role: step-functions-role
  ├── glue:StartJobRun
  └── states:StartExecution
```

### Encryption

| Layer | Mechanism |
|-------|-----------|
| S3 at-rest | SSE-KMS (AWS managed key or CMK) |
| S3 in-transit | Enforce `aws:SecureTransport: true` bucket policy |
| Glue connections | SSL/TLS for JDBC sources |
| Athena results | Encrypted result bucket (SSE-KMS) |

### Governance

| Control | AWS Service |
|---------|------------|
| Column/row-level access | **AWS Lake Formation** |
| API audit trail | **AWS CloudTrail** → S3 → Athena for querying |
| Cost alerts | **AWS Budgets** — alert at 80% and 100% of monthly budget |
| Data quality | **AWS Glue Data Quality** (DQ rules on Glue jobs) |
| PII detection | **Amazon Macie** — scans S3 for sensitive data patterns |

### S3 Bucket Policy Example (deny non-HTTPS access)

```json
{
  "Version": "2012-10-17",
  "Statement": [{
    "Sid": "DenyNonSSL",
    "Effect": "Deny",
    "Principal": "*",
    "Action": "s3:*",
    "Resource": [
      "arn:aws:s3:::company-lakehouse",
      "arn:aws:s3:::company-lakehouse/*"
    ],
    "Condition": {
      "Bool": { "aws:SecureTransport": "false" }
    }
  }]
}
```
