"""PySpark session factory wired for MinIO / S3A."""

from __future__ import annotations

from pyspark.sql import SparkSession

from orchestrator.resources import S3Resource

# Must match Spark 3.5's bundled Hadoop. `hadoop-aws:3.3.4` is what ships in
# the spark-3.5.x-bin-hadoop3 distribution, and aws-java-sdk-bundle 1.12.262 is
# the version hadoop-aws 3.3.4 was compiled against.
_HADOOP_AWS = "org.apache.hadoop:hadoop-aws:3.3.4"
_AWS_SDK = "com.amazonaws:aws-java-sdk-bundle:1.12.262"


def build_spark_session(s3: S3Resource, app_name: str = "pipeline-analysis") -> SparkSession:
    return (
        SparkSession.builder.appName(app_name)
        .master("local[*]")
        .config("spark.jars.packages", f"{_HADOOP_AWS},{_AWS_SDK}")
        .config("spark.hadoop.fs.s3a.endpoint", s3.endpoint_url)
        .config("spark.hadoop.fs.s3a.access.key", s3.access_key)
        .config("spark.hadoop.fs.s3a.secret.key", s3.secret_key)
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        .config(
            "spark.hadoop.fs.s3a.aws.credentials.provider",
            "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider",
        )
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.driver.memory", "1g")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
