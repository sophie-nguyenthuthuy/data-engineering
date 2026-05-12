variable "name_prefix" {
  description = "Prefix applied to every resource name."
  type        = string
}

variable "vpc_id" {
  description = "VPC in which to place the DB subnet group and security group."
  type        = string
}

variable "private_subnet_ids" {
  description = "Private subnets (≥2 AZs) for the DB subnet group."
  type        = list(string)
}

variable "vpc_cidr_block" {
  description = "VPC CIDR; scopes the RDS ingress rule."
  type        = string
}

variable "db_name" {
  description = "Logical database name used by Dagster's SQLAlchemy URL."
  type        = string
  default     = "dagster"
}

variable "db_username" {
  description = "Master username."
  type        = string
  default     = "dagster"
}

variable "instance_class" {
  description = "RDS instance class. db.t4g.micro keeps dev cheap (~$12/mo)."
  type        = string
  default     = "db.t4g.micro"
}

variable "allocated_storage_gb" {
  description = "GP3 allocated storage in GB."
  type        = number
  default     = 20
}

variable "skip_final_snapshot" {
  description = "Skip the final snapshot on destroy. Dev-only convenience."
  type        = bool
  default     = false
}

variable "tags" {
  description = "Tags merged into every resource."
  type        = map(string)
  default     = {}
}
