terraform {
  backend "s3" {
    bucket         = "end2end-tfstate"
    key            = "prod/terraform.tfstate"
    region         = "us-east-1"
    dynamodb_table = "end2end-tfstate-lock"
    encrypt        = true
  }
}
