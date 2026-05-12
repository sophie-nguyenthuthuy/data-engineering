from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lower, trim, to_timestamp, to_date, sum as _sum, count as _count

spark = (
    SparkSession.builder
    .appName("SparkBatchRawToSilverGold")
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000")
    .config("spark.hadoop.fs.s3a.access.key", "minio")
    .config("spark.hadoop.fs.s3a.secret.key", "minio12345")
    .config("spark.hadoop.fs.s3a.path.style.access", "true")
    .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    .getOrCreate()
)

raw_path = "s3a://lakehouse/raw/orders/"
silver_path = "s3a://lakehouse/silver/orders/"
gold_path = "s3a://lakehouse/gold/daily_sales/"

# 1. Read raw CSV
df_raw = (
    spark.read
    .option("header", True)
    .option("inferSchema", True)
    .csv(raw_path)
)

print("=== RAW SCHEMA ===")
df_raw.printSchema()
df_raw.show(truncate=False)

# 2. Transform to silver
df_silver = (
    df_raw
    .withColumn("order_timestamp", to_timestamp(col("order_timestamp"), "yyyy-MM-dd HH:mm:ss"))
    .withColumn("quantity", col("quantity").cast("int"))
    .withColumn("unit_price", col("unit_price").cast("double"))
    .withColumn("order_status", lower(trim(col("order_status"))))
    .withColumn("payment_method", lower(trim(col("payment_method"))))
    .withColumn("gross_amount", col("quantity") * col("unit_price"))
    .filter(col("order_id").isNotNull())
)

print("=== SILVER SAMPLE ===")
df_silver.show(truncate=False)

# 3. Write silver
(
    df_silver.write
    .mode("overwrite")
    .parquet(silver_path)
)

# 4. Read silver back
df_silver_read = spark.read.parquet(silver_path)

# 5. Transform to gold
df_gold = (
    df_silver_read
    .withColumn("order_date", to_date(col("order_timestamp")))
    .groupBy("order_date", "payment_method")
    .agg(
        _count("*").alias("total_orders"),
        _sum("gross_amount").alias("total_revenue")
    )
    .orderBy("order_date", "payment_method")
)

print("=== GOLD SAMPLE ===")
df_gold.show(truncate=False)

# 6. Write gold
(
    df_gold.write
    .mode("overwrite")
    .parquet(gold_path)
)

# 7. Read back gold
df = spark.read.parquet("s3a://lakehouse/gold/daily_sales/")
df.show(truncate=False)
df.printSchema()

# 8. Write partitioned gold data
print("=== GENERATING PARTITIONED GOLD DATA ===")
silver_df = spark.read.parquet("s3a://lakehouse/silver/orders/")

gold_partitioned_df = (
    silver_df
    .withColumn("order_date", to_date(col("order_timestamp")))
    .groupBy("order_date", "payment_method")
    .sum("gross_amount")
    .withColumnRenamed("sum(gross_amount)", "total_revenue")
)

(
    gold_partitioned_df.write
    .mode("overwrite")
    .partitionBy("order_date")
    .parquet("s3a://lakehouse/gold/daily_sales_partitioned/")
)

# 9. Read back partitioned gold
print("=== READING PARTITIONED GOLD DATA ===")
df_partitioned = spark.read.parquet("s3a://lakehouse/gold/daily_sales_partitioned/")
df_partitioned.show(truncate=False)
df_partitioned.printSchema()

# 10. Simulate small file problem
print("=== SIMULATING SMALL FILE PROBLEM (repartition(20)) ===")
small_df = spark.read.parquet("s3a://lakehouse/silver/orders/")

(
    small_df
    .repartition(20)
    .write
    .mode("overwrite")
    .parquet("s3a://lakehouse/silver/orders_many_small_files/")
)

# 11. Coalesce & Sort
print("=== CLUSTERING & COALESCING ===")
clustered_df = (
    spark.read.parquet("s3a://lakehouse/silver/orders/")
    .sort("payment_method", "order_timestamp")
)

(
    clustered_df
    .coalesce(2)
    .write
    .mode("overwrite")
    .parquet("s3a://lakehouse/silver/orders_clustered/")
)

spark.stop()