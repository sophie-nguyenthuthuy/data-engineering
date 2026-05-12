"""
PySpark Structured Streaming version of the anomaly detector.
Deploy this on a Spark cluster (or spark-submit locally) instead of anomaly_detector.py.

Requires:
  spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0 spark_detector.py
"""
import json
import os

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, from_json, to_json, struct, udf, lit, hour, current_timestamp
)
from pyspark.sql.types import (
    ArrayType, BooleanType, DoubleType, IntegerType,
    StringType, StructField, StructType, TimestampType
)

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
INPUT_TOPIC = os.getenv("INPUT_TOPIC", "transactions")
ALERTS_TOPIC = os.getenv("ALERTS_TOPIC", "fraud-alerts")
REDIS_HOST = os.getenv("REDIS_HOST", "localhost")

TX_SCHEMA = StructType([
    StructField("transaction_id", StringType()),
    StructField("account_id", StringType()),
    StructField("amount", DoubleType()),
    StructField("currency", StringType()),
    StructField("merchant", StringType()),
    StructField("merchant_category", StringType()),
    StructField("city", StringType()),
    StructField("latitude", DoubleType()),
    StructField("longitude", DoubleType()),
    StructField("card_present", BooleanType()),
    StructField("transaction_type", StringType()),
    StructField("timestamp", StringType()),
])

HIGH_RISK_CATEGORIES = {"wire_transfer", "crypto", "gambling", "unknown"}


@udf(returnType=StringType())
def detect_signals_udf(transaction_json: str) -> str:
    """
    Stateless fraud rules only — stateful (velocity, geo) require external state store.
    For stateful rules, use ForeachBatch + Redis as shown in anomaly_detector.py.
    """
    if not transaction_json:
        return json.dumps([])

    tx = json.loads(transaction_json)
    signals = []
    amount = tx.get("amount", 0)
    category = tx.get("merchant_category", "")
    card_present = tx.get("card_present", True)

    ts_str = tx.get("timestamp", "")
    try:
        from datetime import datetime, timezone
        ts = datetime.fromisoformat(ts_str)
        utc_hour = ts.astimezone(timezone.utc).hour
    except Exception:
        utc_hour = -1

    if amount >= 5000:
        signals.append({"rule": "HIGH_AMOUNT", "severity": "HIGH", "score": min(100, int(amount / 500))})
    if not card_present and amount >= 2000:
        signals.append({"rule": "CARD_NOT_PRESENT_HIGH", "severity": "HIGH", "score": 60})
    if 2 <= utc_hour < 5:
        signals.append({"rule": "ODD_HOURS", "severity": "MEDIUM", "score": 30})
    if amount in {100, 500, 1000, 2000, 5000, 10000}:
        signals.append({"rule": "ROUND_NUMBER", "severity": "LOW", "score": 20})
    if category in HIGH_RISK_CATEGORIES:
        signals.append({"rule": "HIGH_RISK_MERCHANT", "severity": "MEDIUM", "score": 40})

    return json.dumps(signals)


def main():
    spark = (
        SparkSession.builder
        .appName("BankingAnomalyDetector")
        .config("spark.streaming.stopGracefullyOnShutdown", "true")
        .config("spark.sql.streaming.checkpointLocation", "/tmp/checkpoint")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    raw = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
        .option("subscribe", INPUT_TOPIC)
        .option("startingOffsets", "latest")
        .option("maxOffsetsPerTrigger", 1000)
        .load()
    )

    transactions = (
        raw
        .select(from_json(col("value").cast("string"), TX_SCHEMA).alias("tx"))
        .select("tx.*")
    )

    # Apply stateless rules via UDF
    with_signals = transactions.withColumn(
        "signals_json",
        detect_signals_udf(to_json(struct([col(f) for f in TX_SCHEMA.fieldNames()])))
    ).filter("signals_json != '[]'")

    # Build alert payload for Kafka
    alerts = with_signals.select(
        col("account_id").alias("key"),
        to_json(struct(
            col("transaction_id"),
            col("account_id"),
            col("amount"),
            col("merchant"),
            col("city"),
            col("signals_json").alias("signals"),
            current_timestamp().alias("detected_at"),
        )).alias("value")
    )

    query = (
        alerts.writeStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
        .option("topic", ALERTS_TOPIC)
        .option("checkpointLocation", "/tmp/checkpoint/alerts")
        .trigger(processingTime="2 seconds")
        .start()
    )

    print(f"[spark] streaming {INPUT_TOPIC} → {ALERTS_TOPIC}")
    query.awaitTermination()


if __name__ == "__main__":
    main()
