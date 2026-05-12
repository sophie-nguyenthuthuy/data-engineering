# infra/terraform

Phase 7 IaC for the AWS-side deployment of the pipeline. See
[../../docs/TERRAFORM.md](../../docs/TERRAFORM.md) for the full writeup —
architecture mapping, bootstrap, deploy flow, CI wiring, and deferred work.

Quick entry points:

```bash
cd envs/dev
cp terraform.tfvars.example terraform.tfvars   # edit github_owner
terraform init
terraform plan -out=tfplan
terraform apply tfplan
```
