terraform {
  required_version = ">= 1.6"
  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "~> 5.30"
    }
  }
}

provider "google" {
  project = var.project_id
  region  = var.region
}

# --------------------------------------------------------------------------
# Storage — raw landing for GH Archive dumps and Kafka stream sinks.
# --------------------------------------------------------------------------
resource "google_storage_bucket" "data_lake" {
  name                        = "${var.project_id}-${var.bucket_name}"
  location                    = var.region
  force_destroy               = true
  uniform_bucket_level_access = true

  lifecycle_rule {
    condition { age = 30 }
    action { type = "SetStorageClass"; storage_class = "NEARLINE" }
  }
  lifecycle_rule {
    condition { age = 365 }
    action { type = "Delete" }
  }
}

# --------------------------------------------------------------------------
# BigQuery — raw / staging / marts datasets.
# --------------------------------------------------------------------------
resource "google_bigquery_dataset" "raw" {
  dataset_id = var.dataset_raw
  location   = var.region
  description = "Raw GH Archive events landed by dlt; partitioned by event_hour."
  delete_contents_on_destroy = true
}

resource "google_bigquery_dataset" "staging" {
  dataset_id = var.dataset_staging
  location   = var.region
  description = "dbt staging models over raw events."
  delete_contents_on_destroy = true
}

resource "google_bigquery_dataset" "marts" {
  dataset_id = var.dataset_marts
  location   = var.region
  description = "dbt marts: fct_daily_activity, dim_repos, dim_users."
  delete_contents_on_destroy = true
}

# --------------------------------------------------------------------------
# Service account used by dlt, dbt, Spark, Kestra.
# --------------------------------------------------------------------------
resource "google_service_account" "pipeline" {
  account_id   = "gh-pipeline"
  display_name = "GH Archive pipeline runner"
}

locals {
  pipeline_roles = [
    "roles/bigquery.dataEditor",
    "roles/bigquery.jobUser",
    "roles/storage.objectAdmin",
  ]
}

resource "google_project_iam_member" "pipeline" {
  for_each = toset(local.pipeline_roles)
  project  = var.project_id
  role     = each.value
  member   = "serviceAccount:${google_service_account.pipeline.email}"
}

resource "google_service_account_key" "pipeline" {
  service_account_id = google_service_account.pipeline.name
}

resource "local_file" "sa_key" {
  filename        = "${path.module}/sa-key.json"
  content         = base64decode(google_service_account_key.pipeline.private_key)
  file_permission = "0600"
}
