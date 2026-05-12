"""
Generate a fragmented Delta Lake table for testing the compaction engine.

Creates a `spark-warehouse/events` Delta table with:
- 3 years of daily partitions
- Intentionally many small files (simulating streaming ingestion)
- Skewed data distribution across regions

Run with:
    python examples/generate_test_data.py
"""

import random
import sys
from datetime import date, timedelta
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DateType, DoubleType


def build_spark() -> SparkSession:
    return (
        SparkSession.builder
        .appName("GenerateTestData")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.sql.shuffle.partitions", "4")
        .getOrCreate()
    )


SCHEMA = StructType([
    StructField("event_id", StringType(), False),
    StructField("event_date", DateType(), False),
    StructField("region", StringType(), False),
    StructField("user_id", IntegerType(), False),
    StructField("event_type", StringType(), False),
    StructField("revenue", DoubleType(), True),
])

REGIONS = ["us-east", "us-west", "eu-central", "ap-southeast", "sa-east"]
EVENT_TYPES = ["purchase", "view", "click", "signup", "refund"]


def generate_batch(batch_date: date, n: int = 500) -> list[dict]:
    return [
        {
            "event_id": f"{batch_date}-{i}-{random.randint(1000, 9999)}",
            "event_date": batch_date,
            "region": random.choices(REGIONS, weights=[40, 25, 20, 10, 5])[0],
            "user_id": random.randint(1, 50_000),
            "event_type": random.choice(EVENT_TYPES),
            "revenue": round(random.uniform(0, 500), 2) if random.random() > 0.3 else None,
        }
        for i in range(n)
    ]


def main():
    spark = build_spark()
    table_path = "spark-warehouse/events"
    start_date = date(2022, 1, 1)
    end_date = date(2024, 12, 31)

    print(f"Generating fragmented Delta table at {table_path}")
    print(f"Date range: {start_date} → {end_date}")
    print("Writing many small batches to simulate streaming ingestion fragmentation...")

    current = start_date
    total_batches = 0

    while current <= end_date:
        # Write 3–6 small batches per day (simulates micro-batch streaming)
        batches_per_day = random.randint(3, 6)
        for _ in range(batches_per_day):
            rows = generate_batch(current, n=random.randint(200, 800))
            df = spark.createDataFrame(rows, schema=SCHEMA)
            (
                df.write
                .format("delta")
                .mode("append")
                .partitionBy("event_date")
                .save(table_path)
            )
            total_batches += 1

        if current.day == 1:
            print(f"  Progress: {current} ({total_batches} batches written)")

        current += timedelta(days=1)

    print(f"\nDone. Written {total_batches} batches.")

    # Show table stats
    detail = spark.sql(f"DESCRIBE DETAIL delta.`{table_path}`").collect()[0]
    print(f"\nTable stats:")
    print(f"  Files:     {detail['numFiles']}")
    print(f"  Size:      {detail['sizeInBytes'] / (1024**3):.2f} GB")
    avg_size = detail["sizeInBytes"] / detail["numFiles"] / (1024**2)
    print(f"  Avg file:  {avg_size:.1f} MB  (target: 128 MB)")

    spark.stop()


if __name__ == "__main__":
    main()
