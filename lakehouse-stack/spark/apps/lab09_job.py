from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date

spark = (
    SparkSession.builder
    .appName("SparkLab09Optimization")
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000")
    .config("spark.hadoop.fs.s3a.access.key", "minio")
    .config("spark.hadoop.fs.s3a.secret.key", "minio12345")
    .config("spark.hadoop.fs.s3a.path.style.access", "true")
    .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    .getOrCreate()
)

print("=== TASK 1: READ SILVER ===")
silver_df = spark.read.parquet("s3a://lakehouse/silver/orders/")
silver_df.show(truncate=False)

print("=== TASK 2: PARTITION BY ===")
gold_df = (
    silver_df
    .withColumn("order_date", to_date(col("order_timestamp")))
    .groupBy("order_date", "payment_method")
    .sum("gross_amount")
    .withColumnRenamed("sum(gross_amount)", "total_revenue")
)
(gold_df.write
    .mode("overwrite")
    .partitionBy("order_date")
    .parquet("s3a://lakehouse/gold/daily_sales_partitioned/"))

print("=== TASK 3: SMALL FILE PROBLEM ===")
(silver_df
    .repartition(20)
    .write
    .mode("overwrite")
    .parquet("s3a://lakehouse/silver/orders_many_small_files/"))

print("=== TASK 4: COMPACTION ===")
small_df = spark.read.parquet("s3a://lakehouse/silver/orders_many_small_files/")
(small_df
    .coalesce(2)
    .write
    .mode("overwrite")
    .parquet("s3a://lakehouse/silver/orders_compacted/"))

print("=== TASK 5: CLUSTERING ===")
(silver_df
    .sort("payment_method", "order_timestamp")
    .coalesce(2)
    .write
    .mode("overwrite")
    .parquet("s3a://lakehouse/silver/orders_clustered/"))

print("ALL OPTIMIZATIONS COMPLETED SUCCESSFULLY")
spark.stop()
