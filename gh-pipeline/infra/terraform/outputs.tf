output "bucket" {
  value = google_storage_bucket.data_lake.name
}

output "datasets" {
  value = {
    raw     = google_bigquery_dataset.raw.dataset_id
    staging = google_bigquery_dataset.staging.dataset_id
    marts   = google_bigquery_dataset.marts.dataset_id
  }
}

output "service_account_email" {
  value = google_service_account.pipeline.email
}
