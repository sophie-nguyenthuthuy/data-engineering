#!/bin/bash
# Creates the S3 bucket in LocalStack on startup
awslocal s3 mb s3://encrypted-pii-records
echo "LocalStack: bucket created"
