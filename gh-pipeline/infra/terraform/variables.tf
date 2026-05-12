variable "project_id" {
  type = string
}

variable "region" {
  type    = string
  default = "us-central1"
}

variable "bucket_name" {
  type    = string
  default = "gh-archive-pipeline"
}

variable "dataset_raw" {
  type    = string
  default = "gh_raw"
}

variable "dataset_staging" {
  type    = string
  default = "gh_staging"
}

variable "dataset_marts" {
  type    = string
  default = "gh_marts"
}
