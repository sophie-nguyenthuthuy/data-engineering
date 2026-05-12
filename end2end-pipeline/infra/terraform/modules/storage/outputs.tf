output "raw_bucket_id" {
  value = aws_s3_bucket.raw.id
}

output "raw_bucket_arn" {
  value = aws_s3_bucket.raw.arn
}

output "analysis_bucket_id" {
  value = aws_s3_bucket.analysis.id
}

output "analysis_bucket_arn" {
  value = aws_s3_bucket.analysis.arn
}

output "glue_database_name" {
  value = aws_glue_catalog_database.this.name
}
