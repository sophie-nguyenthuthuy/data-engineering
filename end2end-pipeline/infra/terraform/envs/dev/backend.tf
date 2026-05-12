# Remote state. The bucket + DynamoDB lock table must exist before `init` —
# bootstrap them once per account with the commands in docs/TERRAFORM.md.
terraform {
  backend "s3" {
    bucket         = "end2end-tfstate"
    key            = "dev/terraform.tfstate"
    region         = "us-east-1"
    dynamodb_table = "end2end-tfstate-lock"
    encrypt        = true
  }
}
