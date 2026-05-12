# Bucket names are globally unique. Suffix with a random string so `apply` in a
# fresh account doesn't collide with somebody else's bucket.
resource "random_id" "suffix" {
  byte_length = 3
}

locals {
  raw_bucket_name      = "${var.name_prefix}-raw-${random_id.suffix.hex}"
  analysis_bucket_name = "${var.name_prefix}-analysis-${random_id.suffix.hex}"
}

resource "aws_s3_bucket" "raw" {
  bucket        = local.raw_bucket_name
  force_destroy = var.force_destroy
  tags          = merge(var.tags, { Name = local.raw_bucket_name, Purpose = "raw-events-parquet" })
}

resource "aws_s3_bucket" "analysis" {
  bucket        = local.analysis_bucket_name
  force_destroy = var.force_destroy
  tags          = merge(var.tags, { Name = local.analysis_bucket_name, Purpose = "analysis-artifacts" })
}

resource "aws_s3_bucket_public_access_block" "raw" {
  bucket                  = aws_s3_bucket.raw.id
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

resource "aws_s3_bucket_public_access_block" "analysis" {
  bucket                  = aws_s3_bucket.analysis.id
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

resource "aws_s3_bucket_server_side_encryption_configuration" "raw" {
  bucket = aws_s3_bucket.raw.id
  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm = "AES256"
    }
  }
}

resource "aws_s3_bucket_server_side_encryption_configuration" "analysis" {
  bucket = aws_s3_bucket.analysis.id
  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm = "AES256"
    }
  }
}

resource "aws_s3_bucket_versioning" "raw" {
  bucket = aws_s3_bucket.raw.id
  versioning_configuration {
    status = "Enabled"
  }
}

resource "aws_s3_bucket_versioning" "analysis" {
  bucket = aws_s3_bucket.analysis.id
  versioning_configuration {
    status = "Enabled"
  }
}

# Raw Parquet rolls from Standard → IA → Glacier; analysis artifacts are kept hot.
resource "aws_s3_bucket_lifecycle_configuration" "raw" {
  bucket = aws_s3_bucket.raw.id
  rule {
    id     = "tiered-transition"
    status = "Enabled"
    filter {}
    transition {
      days          = 30
      storage_class = "STANDARD_IA"
    }
    transition {
      days          = 90
      storage_class = "GLACIER"
    }
    noncurrent_version_expiration {
      noncurrent_days = 30
    }
  }
}

# Glue catalog gives Athena / external Spark query access to the Parquet that
# Dagster writes. Schema mirrors services/orchestrator raw_events output.
resource "aws_glue_catalog_database" "this" {
  name        = replace("${var.name_prefix}_events", "-", "_")
  description = "Catalog for raw + analysis Parquet written by the Dagster pipeline."
}

resource "aws_glue_catalog_table" "raw_events" {
  name          = "raw_events"
  database_name = aws_glue_catalog_database.this.name
  table_type    = "EXTERNAL_TABLE"

  parameters = {
    EXTERNAL              = "TRUE"
    "parquet.compression" = "ZSTD"
    classification        = "parquet"
  }

  partition_keys {
    name = "dt"
    type = "string"
  }
  partition_keys {
    name = "hour"
    type = "string"
  }

  storage_descriptor {
    location      = "s3://${aws_s3_bucket.raw.id}/raw/user_interactions/"
    input_format  = "org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat"
    output_format = "org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat"

    ser_de_info {
      serialization_library = "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe"
    }

    columns {
      name = "event_id"
      type = "string"
    }
    columns {
      name = "occurred_at"
      type = "timestamp"
    }
    columns {
      name = "user_id"
      type = "string"
    }
    columns {
      name = "session_id"
      type = "string"
    }
    columns {
      name = "event_type"
      type = "string"
    }
    columns {
      name = "status"
      type = "string"
    }
    columns {
      name = "error_code"
      type = "string"
    }
    columns {
      name = "latency_ms"
      type = "int"
    }
    columns {
      name = "country"
      type = "string"
    }
    columns {
      name = "device"
      type = "string"
    }
    columns {
      name = "ingested_at"
      type = "timestamp"
    }
  }
}
