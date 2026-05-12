# Terraform (AWS) — Phase 7

The compose stack is the local / dev inner loop. This module tree stands up
the cloud-side equivalent on AWS so the same pipeline can run on managed
services instead of containers-on-a-laptop.

The mapping isn't 1:1 — some local components (Kafka Connect, ClickHouse,
MinIO, Prometheus/Loki/Tempo, etc.) are replaced by their managed-service
cousins rather than lifted as-is.

## Mapping

| Local (compose)                      | AWS (terraform)                            |
| ------------------------------------ | ------------------------------------------ |
| Kafka (KRaft) + SCRAM/TLS            | **MSK Serverless** + IAM auth              |
| MinIO                                | **S3** buckets (raw + analysis)            |
| Hive-style partition layout          | **Glue Catalog** database + table          |
| ClickHouse (columnar)                | *out of scope* — run ClickHouse Cloud or self-hosted on EKS; the table schema in `infra/clickhouse/init.sql` is portable |
| Dagster (SQLite metadata)            | Dagster (self-host) + **RDS Postgres** for metadata |
| producer, api (compose services)     | **ECS Fargate** services; **ECR** repos; **ALB** in front of API |
| OTel Collector → Prometheus/Loki/Tempo | **AMP** (Prometheus) + **Managed Grafana**; task logs → **CloudWatch** |
| Docker secrets                       | **Secrets Manager** (DB creds); IAM where possible |
| —                                    | **OIDC role** for GitHub Actions plan / apply |

## Tree

```
infra/terraform/
├── modules/
│   ├── network/        VPC + 2/3 AZ public+private subnets, NAT, S3 gateway VPCE
│   ├── kafka/          MSK Serverless cluster + SG (IAM auth on :9098)
│   ├── storage/        S3 raw + analysis buckets (SSE, versioning, lifecycle) + Glue DB + raw_events table
│   ├── analytics/      RDS Postgres (Dagster metadata) + Secrets Manager entry
│   ├── compute/        ECR, ECS cluster, Fargate tasks for producer + api, ALB, IAM task roles
│   ├── observability/  AMP workspace + Managed Grafana workspace + IAM role
│   └── iam/            GitHub OIDC provider + plan (RO) and apply (PowerUser) roles
└── envs/
    ├── dev/            2 AZs, small instances, force_destroy=true, skip_final_snapshot=true
    └── prod/           3 AZs, larger instances, destroy-safety on
```

## Bootstrap (once per account)

State bucket + lock table must exist before `init`. Do this once via the AWS
console or a throwaway root admin session:

```bash
aws s3api create-bucket --bucket end2end-tfstate --region us-east-1
aws s3api put-bucket-versioning --bucket end2end-tfstate \
  --versioning-configuration Status=Enabled
aws s3api put-bucket-encryption --bucket end2end-tfstate \
  --server-side-encryption-configuration \
  '{"Rules":[{"ApplyServerSideEncryptionByDefault":{"SSEAlgorithm":"AES256"}}]}'

aws dynamodb create-table --table-name end2end-tfstate-lock \
  --attribute-definitions AttributeName=LockID,AttributeType=S \
  --key-schema AttributeName=LockID,KeyType=HASH \
  --billing-mode PAY_PER_REQUEST --region us-east-1
```

Bucket name + table name are hard-coded in `envs/*/backend.tf`. Change
both if you need them per-account.

## Deploy (dev)

```bash
cd infra/terraform/envs/dev
cp terraform.tfvars.example terraform.tfvars
$EDITOR terraform.tfvars   # set github_owner at minimum

terraform init
terraform plan -out=tfplan
terraform apply tfplan
```

First `apply` takes 20–30 min — MSK Serverless + RDS dominate. Subsequent
applies are typically seconds unless compute or MSK changes.

On success, the root outputs print the API's ALB URL, ECR repo URLs, the
Kafka bootstrap brokers, the AMP remote-write endpoint, and both GHA role
ARNs. Drop those ARNs into repo **Variables** (not Secrets — they're not
sensitive) as `AWS_PLAN_ROLE_ARN` and `AWS_APPLY_ROLE_ARN` to wire up the
workflow.

## What's wired vs what's deferred

**Wired end-to-end:**

- VPC + subnets + NAT + S3 VPC endpoint
- MSK Serverless with IAM auth; producer task role has `kafka-cluster:*`
  on the cluster + topic ARN pattern
- S3 + Glue (raw_events table partitioned by `dt` / `hour` — matches the
  layout Dagster writes in Phase 4)
- RDS Postgres + Secrets Manager entry (Dagster reads the secret ARN via
  the task execution role)
- ECS Fargate services, ALB for API, task autoscaling via `desired_count`
- AMP + Managed Grafana workspaces, IAM for Grafana to query both
- GitHub OIDC provider + scoped plan/apply roles

**Deferred on purpose** (the roadmap's Phase 7 scope is Terraform, not a
full cutover — these are called out because they'd be the next week of
work, not next month):

| Deferred                              | Why                                                               |
| ------------------------------------- | ----------------------------------------------------------------- |
| **ClickHouse**                        | AWS has no managed ClickHouse; you either use ClickHouse Cloud (separate account + VPC peering) or self-host on EKS. Too big a choice to bake into a module. |
| **ADOT sidecar / remote_write config** | AMP endpoint is output, but the ECS task defs don't yet include the ADOT Collector sidecar. Add one `adot-collector` container next to `api` with the AMP endpoint from module outputs. |
| **TLS on the ALB**                    | Listener is plain HTTP :80. Add an ACM cert + :443 listener once a domain is in place. |
| **Dagster Fargate service**           | Dagster webserver + daemon need to run somewhere — left as a follow-up because it's substantial on its own (persistent task queue, S3 I/O for assets). RDS is provisioned and waiting. |
| **Kafka Connect → ClickHouse**        | Depends on ClickHouse choice. MSK Connect or self-hosted Connect on ECS both work; wire once the CH endpoint is real. |
| **Image publishing to ECR**           | Workflow scaffolding only — no `docker build → push` step yet. Add after choosing whether to publish per-commit or per-tag. |
| **CloudFront in front of the API**    | ALB is public today. CloudFront + WAFv2 is the natural Phase 8 addition. |
| **Route53 hosted zone**               | Infra provisions no DNS. Bring your own zone; we output the ALB DNS + zone id so an alias record is a one-liner. |

## CI

[`.github/workflows/terraform.yml`](../.github/workflows/terraform.yml) runs
`terraform fmt -check -recursive` and `terraform validate` on both envs on
every PR that touches `infra/terraform/`. When the repo variable
`AWS_PLAN_ROLE_ARN` is set, it also runs `terraform plan` for `dev` on PRs
and uploads the plan as an artifact.

`apply` is manual-dispatch only (`workflow_dispatch`) — no PR merge
auto-applies. The `environment:` key routes through
[GitHub Environments](https://docs.github.com/en/actions/deployment/targeting-different-environments/using-environments-for-deployment),
so you can gate apply on a reviewer approval per env.

## OIDC role trust

The `iam` module creates `*-gha-plan` (ReadOnlyAccess) and `*-gha-apply`
(PowerUserAccess + scoped IAM management on `*-*` names) roles. Both roles'
trust policies bind to the GitHub OIDC provider, filtered on the `sub`
claim. Defaults allow:

- `repo:<owner>/<repo>:ref:refs/heads/main` (apply)
- `repo:<owner>/<repo>:pull_request`       (plan from PR branches)

Set `gha_allowed_refs` in tfvars to narrow further — prod should typically
only allow `environment:prod` so an approval in GitHub Environments is
mandatory.

## Cost notes (dev env, us-east-1, napkin math)

| Line item                               | ~$/mo  |
| --------------------------------------- | ------ |
| MSK Serverless baseline (no data)       | $5–10  |
| RDS db.t4g.micro + 20 GB gp3            | $12    |
| NAT Gateway (1 AZ, idle)                | $33    |
| ALB baseline                            | $17    |
| ECS Fargate (producer 0.25 vCPU, api 2× 0.5 vCPU, 24/7) | ~$35 |
| S3 + Glue + ECR (storage only)          | < $2   |
| AMP (no ingest)                         | free tier |
| Managed Grafana (1 editor)              | $9     |
| **Total idle dev**                      | **~$115/mo** |

Most of it is the NAT GW and Fargate — not MSK. Turn off the Fargate
services (`desired_count = 0`) and destroy the NAT to drop to ~$20/mo.
