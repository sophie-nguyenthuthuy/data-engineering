#!/usr/bin/env sh
# Create the MinIO bucket the Dagster ingest asset writes to. Idempotent.
set -eu

: "${S3_BUCKET:?}"
: "${MINIO_ENDPOINT:?}"
: "${MINIO_ROOT_USER:?}"
: "${MINIO_ROOT_PASSWORD:?}"

mc alias set local "$MINIO_ENDPOINT" "$MINIO_ROOT_USER" "$MINIO_ROOT_PASSWORD"
mc mb --ignore-existing "local/${S3_BUCKET}"
mc ls "local/${S3_BUCKET}" >/dev/null
echo "bucket ready: ${S3_BUCKET}"
