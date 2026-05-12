variable "name_prefix" {
  description = "Prefix applied to every resource name."
  type        = string
}

variable "vpc_id" {
  description = "VPC for ECS services, ALB, and security groups."
  type        = string
}

variable "public_subnet_ids" {
  description = "Public subnets for the ALB."
  type        = list(string)
}

variable "private_subnet_ids" {
  description = "Private subnets for the ECS tasks."
  type        = list(string)
}

variable "msk_cluster_arn" {
  description = "MSK Serverless cluster ARN — needed for IAM Kafka policy + SG reference."
  type        = string
}

variable "msk_security_group_id" {
  description = "MSK cluster SG; tasks egress to it on 9098."
  type        = string
}

variable "msk_bootstrap_brokers" {
  description = "Bootstrap servers injected as BROKERS env var."
  type        = string
}

variable "raw_bucket_arn" {
  description = "S3 raw bucket ARN — scoped R/W policy."
  type        = string
}

variable "analysis_bucket_arn" {
  description = "S3 analysis bucket ARN — scoped R/W policy."
  type        = string
}

variable "db_secret_arn" {
  description = "Secrets Manager ARN containing the Dagster DB credentials."
  type        = string
}

variable "topic" {
  description = "Kafka topic the producer writes to and the API indirectly reads from."
  type        = string
  default     = "user-interactions"
}

variable "producer_image" {
  description = "Container image for the producer (optional override)."
  type        = string
  default     = null
}

variable "api_image" {
  description = "Container image for the API (optional override)."
  type        = string
  default     = null
}

variable "producer_cpu" {
  description = "Fargate CPU units for the producer task."
  type        = number
  default     = 256
}

variable "producer_memory" {
  description = "Fargate memory (MiB) for the producer task."
  type        = number
  default     = 512
}

variable "api_cpu" {
  description = "Fargate CPU units for the API task."
  type        = number
  default     = 512
}

variable "api_memory" {
  description = "Fargate memory (MiB) for the API task."
  type        = number
  default     = 1024
}

variable "api_desired_count" {
  description = "Number of API replicas."
  type        = number
  default     = 2
}

variable "tags" {
  description = "Tags merged into every resource."
  type        = map(string)
  default     = {}
}
