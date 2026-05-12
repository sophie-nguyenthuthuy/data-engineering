#!/bin/bash
# Runs inside LocalStack on first start — creates the S3 bucket.
set -e
echo "Creating S3 bucket: tiered-storage-warm"
awslocal s3 mb s3://tiered-storage-warm
echo "Bucket created."
