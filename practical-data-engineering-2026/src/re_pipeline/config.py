import os

S3_ENDPOINT = os.environ.get("S3_ENDPOINT", "http://localhost:9000")
S3_ACCESS_KEY = os.environ.get("S3_ACCESS_KEY", "minio")
S3_SECRET_KEY = os.environ.get("S3_SECRET_KEY", "minio12345")
WAREHOUSE_PATH = os.environ.get("WAREHOUSE_PATH", "s3://warehouse/")
CATALOG_URI = os.environ.get("CATALOG_URI", "sqlite:///./dagster_home/catalog.db")
CATALOG_NAME = "re"
NAMESPACE = "real_estate"

S3_STORAGE_OPTIONS = {
    "s3.endpoint": S3_ENDPOINT,
    "s3.access-key-id": S3_ACCESS_KEY,
    "s3.secret-access-key": S3_SECRET_KEY,
    "s3.path-style-access": "true",
    "s3.region": "us-east-1",
}

# For dlt filesystem destination
DLT_BUCKET_URL = "s3://raw"
DLT_CREDENTIALS = {
    "aws_access_key_id": S3_ACCESS_KEY,
    "aws_secret_access_key": S3_SECRET_KEY,
    "endpoint_url": S3_ENDPOINT,
    "region_name": "us-east-1",
}
