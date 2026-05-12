variable "name_prefix" {
  description = "Prefix applied to every resource name."
  type        = string
}

variable "vpc_id" {
  description = "VPC in which to place the MSK Serverless cluster."
  type        = string
}

variable "private_subnet_ids" {
  description = "At least two private subnets in different AZs — MSK Serverless requires ≥2."
  type        = list(string)
  validation {
    condition     = length(var.private_subnet_ids) >= 2
    error_message = "MSK Serverless requires at least two subnets in different AZs."
  }
}

variable "vpc_cidr_block" {
  description = "VPC CIDR used to scope the cluster security group ingress."
  type        = string
}

variable "tags" {
  description = "Tags merged into every resource."
  type        = map(string)
  default     = {}
}
