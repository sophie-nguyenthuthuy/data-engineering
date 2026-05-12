"""
Lab 17 — Demo: Move Data to S3 (Conceptual Simulation)

Simulates the Bronze/Silver/Gold → Amazon S3 migration pattern
using moto (mock AWS) so no real AWS credentials are required.

In production, replace @mock_aws with real boto3 calls and set:
  AWS_ACCESS_KEY_ID / AWS_SECRET_ACCESS_KEY / AWS_DEFAULT_REGION
or use an IAM role on EC2/ECS/Glue.

Migration flow (from slide):
  Source Data
    └─► Bronze Stage (raw CSV/JSON)
    └─► Silver Stage (cleaned Parquet)
    └─► Gold Stage  (aggregated Parquet)
         └─► Amazon S3 Destination (partitioned)
"""

import io
import os
import json
import boto3
import pandas as pd
from moto import mock_aws
from datetime import datetime, timezone

BUCKET = "company-lakehouse"
REGION = "ap-southeast-1"


# ---------------------------------------------------------------------------
# Synthetic data helpers (stand-ins for real MinIO / local Parquet files)
# ---------------------------------------------------------------------------

def make_bronze_data() -> pd.DataFrame:
    """Raw orders — as received from Kafka / Postgres CDC."""
    return pd.DataFrame({
        "order_id":       [1, 2, 3, 4, 5],
        "order_timestamp":["2026-04-01 10:15:00", "2026-04-01 11:20:00",
                           "2026-04-02 09:05:00", "2026-04-02 14:30:00",
                           "2026-04-02 16:45:00"],
        "quantity":       [5, 2, 10, 1, 3],
        "unit_price":     [15.5, 25.0, 12.0, 100.0, 45.5],
        "order_status":   ["COMPLETED", "PENDING", "COMPLETED", "CANCELLED", "COMPLETED"],
        "payment_method": ["CREDIT_CARD", "PAYPAL", "CREDIT_CARD", "CASH", "CREDIT_CARD"],
    })


def make_silver_data() -> pd.DataFrame:
    """Cleaned, typed, deduplicated — adds total_amount, drops CANCELLED."""
    df = make_bronze_data()
    df = df[df["order_status"] != "CANCELLED"].copy()
    df["order_timestamp"] = pd.to_datetime(df["order_timestamp"])
    df["total_amount"] = df["quantity"] * df["unit_price"]
    df["date"] = df["order_timestamp"].dt.date.astype(str)
    return df


def make_gold_data() -> pd.DataFrame:
    """Daily aggregation — business-ready for BI / Athena queries."""
    df = make_silver_data()
    gold = (
        df.groupby("date")
        .agg(
            total_orders=("order_id", "count"),
            total_revenue=("total_amount", "sum"),
            avg_order_value=("total_amount", "mean"),
        )
        .reset_index()
    )
    gold["avg_order_value"] = gold["avg_order_value"].round(2)
    return gold


# ---------------------------------------------------------------------------
# S3 upload helpers
# ---------------------------------------------------------------------------

def df_to_parquet_bytes(df: pd.DataFrame) -> bytes:
    buf = io.BytesIO()
    df.to_parquet(buf, index=False, engine="pyarrow")
    return buf.getvalue()


def upload_layer(s3, layer: str, df: pd.DataFrame, partition_col: str | None = None):
    """
    Upload a DataFrame as Parquet to s3://{BUCKET}/{layer}/.
    If partition_col is given, write Hive-style partitions:
      s3://bucket/layer/date=2026-04-01/data.parquet
    """
    if partition_col and partition_col in df.columns:
        for val, grp in df.groupby(partition_col):
            key = f"{layer}/{partition_col}={val}/data.parquet"
            s3.put_object(Bucket=BUCKET, Key=key, Body=df_to_parquet_bytes(grp))
            print(f"  uploaded s3://{BUCKET}/{key}  ({len(grp)} rows)")
    else:
        key = f"{layer}/data.parquet"
        s3.put_object(Bucket=BUCKET, Key=key, Body=df_to_parquet_bytes(df))
        print(f"  uploaded s3://{BUCKET}/{key}  ({len(df)} rows)")


def list_bucket(s3):
    resp = s3.list_objects_v2(Bucket=BUCKET)
    objects = resp.get("Contents", [])
    print(f"\n{'Key':<55} {'Size':>8}")
    print("-" * 65)
    for obj in objects:
        print(f"  {obj['Key']:<53} {obj['Size']:>8} B")
    print(f"\nTotal objects: {len(objects)}")


# ---------------------------------------------------------------------------
# Main simulation
# ---------------------------------------------------------------------------

@mock_aws
def run_migration():
    print("=" * 65)
    print("  Demo: Move Data to S3 — Bronze / Silver / Gold Migration")
    print("=" * 65)

    # 1. Create mock S3 bucket (stand-in for real aws s3 mb)
    s3 = boto3.client("s3", region_name=REGION)
    s3.create_bucket(
        Bucket=BUCKET,
        CreateBucketConfiguration={"LocationConstraint": REGION},
    )
    print(f"\n[1] Created bucket: s3://{BUCKET}/\n")

    # 2. Bronze layer — raw CSV-equivalent, no partitioning
    print("[2] Uploading Bronze (raw data, no partition):")
    upload_layer(s3, "bronze", make_bronze_data())

    # 3. Silver layer — cleaned Parquet, partitioned by date (Hive-style)
    print("\n[3] Uploading Silver (cleaned Parquet, partitioned by date):")
    upload_layer(s3, "silver", make_silver_data(), partition_col="date")

    # 4. Gold layer — daily aggregates, partitioned by date
    print("\n[4] Uploading Gold (aggregated, partitioned by date):")
    upload_layer(s3, "gold", make_gold_data(), partition_col="date")

    # 5. Verify — list all objects
    print("\n[5] All objects in bucket:")
    list_bucket(s3)

    # 6. Verify read-back (simulate Athena scanning a Silver partition)
    print("\n[6] Simulate Athena scan — read silver/date=2026-04-02/data.parquet:")
    obj = s3.get_object(Bucket=BUCKET, Key="silver/date=2026-04-02/data.parquet")
    df_check = pd.read_parquet(io.BytesIO(obj["Body"].read()))
    print(df_check[["order_id", "date", "total_amount", "order_status"]].to_string(index=False))

    print("\n[✓] Migration simulation complete.")
    print("    In production: replace @mock_aws with real boto3 + IAM role.")


if __name__ == "__main__":
    run_migration()
