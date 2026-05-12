"""Unit tests for incremental ingestion logic (no real Spark needed for pure-logic tests)."""

import pytest
from unittest.mock import MagicMock, patch
from pyspark.sql import SparkSession
from pyspark.sql import functions as F


@pytest.fixture(scope="session")
def spark():
    return (
        SparkSession.builder
        .master("local[1]")
        .appName("test-lakehouse-migration")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .getOrCreate()
    )


def test_watermark_default_when_table_missing(spark, tmp_path):
    from src.ingestion.incremental import _last_watermark
    result = _last_watermark(spark, str(tmp_path), "nonexistent_table", "updated_at")
    assert result == "1970-01-01 00:00:00"


def test_append_bronze_skips_empty_df(spark, tmp_path, caplog):
    from src.ingestion.incremental import append_bronze
    empty_df = spark.createDataFrame([], schema="transaction_id STRING, updated_at TIMESTAMP")
    count = append_bronze(empty_df, str(tmp_path), "transactions", [])
    assert count == 0


def test_upsert_silver_creates_table_when_missing(spark, tmp_path):
    from src.ingestion.incremental import upsert_silver
    from pyspark.sql.types import StructType, StructField, StringType, TimestampType, BooleanType

    schema = StructType([
        StructField("customer_id", StringType()),
        StructField("email", StringType()),
        StructField("_ingested_at", TimestampType()),
    ])
    df = spark.createDataFrame([("c1", "a@b.com", None)], schema=schema)
    upsert_silver(spark, df, str(tmp_path), "customers", ["customer_id"], scd_type=1)

    result = spark.read.format("delta").load(f"{tmp_path}/customers")
    assert result.count() == 1
