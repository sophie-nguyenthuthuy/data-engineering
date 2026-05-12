variable "name_prefix" {
  description = "Prefix applied to every resource name."
  type        = string
}

variable "cidr_block" {
  description = "IPv4 CIDR for the VPC. /16 leaves room for 4096 hosts per /20 subnet."
  type        = string
  default     = "10.42.0.0/16"
}

variable "azs" {
  description = "Availability zones. Two is enough for MSK Serverless + ECS spread."
  type        = list(string)
}

variable "tags" {
  description = "Tags merged into every resource."
  type        = map(string)
  default     = {}
}
