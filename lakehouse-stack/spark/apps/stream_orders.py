from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, to_timestamp, window, sum as _sum, count as _count
from pyspark.sql.types import StructType, StructField, IntegerType, DoubleType, StringType

schema = StructType([
    StructField("order_id", IntegerType()),
    StructField("customer_id", IntegerType()),
    StructField("amount", DoubleType()),
    StructField("event_time", StringType())
])

spark = (SparkSession.builder
    .appName("orders_streaming_lab")
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000")
    .config("spark.hadoop.fs.s3a.access.key", "minio")
    .config("spark.hadoop.fs.s3a.secret.key", "minio12345")
    .config("spark.hadoop.fs.s3a.path.style.access", "true")
    .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    .getOrCreate())

raw = (spark.readStream.format("kafka")
    .option("kafka.bootstrap.servers", "kafka:29092")
    .option("subscribe", "orders_stream")
    .option("startingOffsets", "earliest")
    .load())

parsed = (raw.selectExpr("CAST(value AS STRING) AS json_str")
    .select(from_json(col("json_str"), schema).alias("data"))
    .select("data.*")
    .withColumn("event_time", to_timestamp("event_time")))

agg = (parsed
    .withWatermark("event_time", "5 minutes")
    .groupBy(window(col("event_time"), "5 minutes"), col("customer_id"))
    .agg(_count("*").alias("total_orders"), _sum("amount").alias("total_amount")))

query = (agg.writeStream
    .format("parquet")
    .option("path", "s3a://lakehouse/gold/orders_streaming/")
    .option("checkpointLocation", "s3a://lakehouse/checkpoints/orders_streaming/")
    .outputMode("append")
    .start())

query.awaitTermination()
