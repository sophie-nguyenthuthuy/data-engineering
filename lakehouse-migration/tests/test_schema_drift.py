"""Tests for schema drift detection."""

import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType


@pytest.fixture(scope="session")
def spark():
    return (
        SparkSession.builder
        .master("local[1]")
        .appName("test-schema-drift")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .getOrCreate()
    )


def test_no_drift_when_schemas_match(spark, tmp_path):
    from src.schema.evolution import detect_schema_drift

    schema = StructType([StructField("id", StringType()), StructField("value", DoubleType())])
    df = spark.createDataFrame([("a", 1.0)], schema=schema)
    df.write.format("delta").save(str(tmp_path))

    drift = detect_schema_drift(df, str(tmp_path), spark)
    assert drift == {"added": [], "removed": [], "type_changed": []}


def test_detects_added_column(spark, tmp_path):
    from src.schema.evolution import detect_schema_drift

    old_schema = StructType([StructField("id", StringType())])
    new_schema = StructType([StructField("id", StringType()), StructField("new_col", IntegerType())])

    spark.createDataFrame([("a",)], old_schema).write.format("delta").save(str(tmp_path))
    source_df = spark.createDataFrame([("a", 1)], new_schema)

    drift = detect_schema_drift(source_df, str(tmp_path), spark)
    assert "new_col" in drift["added"]
