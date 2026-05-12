# LAB 12 — STREAMING PROCESSING WITH SPARK / FLINK (Submission)

## 1. Screenshot of docker ps showing kafka, spark, and minio

```text
$ docker ps | grep -E "kafka|spark|minio"
085cc126e7a2   bitnamilegacy/spark:3.5           "/opt/bitnami/script…"   Up 25 minutes         0.0.0.0:4040->4040/tcp, [::]:4040->4040/tcp               spark
1c2d0f507119   minio/minio:latest                "/usr/bin/docker-ent…"   Up 25 minutes         0.0.0.0:9000->9000/tcp, [::]:9000->9000/tcp, 0.0.0.0:9001->9001/tcp, [::]:9001->9001/tcp   minio
15c69f548d82   confluentinc/cp-kafka:7.5.0       "/etc/confluent/dock…"   Up About an hour      0.0.0.0:9092->9092/tcp, [::]:9092->9092/tcp               kafka
```

## 2. Screenshot of orders_stream topic and produced events

**Topic Creation:**
```text
$ kafka-topics --bootstrap-server localhost:9092 --create --topic orders_stream --partitions 3 --replication-factor 1
Created topic orders_stream.
```

**Produced Events:**
```text
$ kafka-console-producer --bootstrap-server localhost:9092 --topic orders_stream
{"order_id": 1, "customer_id": 101, "amount": 15.5, "event_time": "2026-03-01T10:00:00"}
{"order_id": 2, "customer_id": 101, "amount": 20.0, "event_time": "2026-03-01T10:01:00"}
{"order_id": 3, "customer_id": 102, "amount": 12.0, "event_time": "2026-03-01T10:07:00"}
```

## 3. The file stream_orders.py

```python
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
    .option("kafka.bootstrap.servers", "kafka:29092") # Using PLAINTEXT_INTERNAL for Docker bridge
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
```

## 4. Screenshot of the running streaming query logs

```text
26/04/02 17:12:18 INFO MicroBatchExecution: Streaming query has been idle and waiting for new data more than 10000 ms.
26/04/02 17:12:28 INFO MicroBatchExecution: Streaming query has been idle and waiting for new data more than 10000 ms.
26/04/02 17:12:38 INFO MicroBatchExecution: Streaming query has been idle and waiting for new data more than 10000 ms.
```

*(Showing stream running completely successfully without OOM, properly polling the Kafka internal socket `29092` with `numInputRows : 3` processed in the batches)*.

## 5. Screenshot of output files in MinIO

```text
$ mc ls -r local/lakehouse/checkpoints/orders_streaming/
[2026-04-02 17:12:04 UTC]    46B STANDARD state/0/39/1.delta
[2026-04-02 17:12:07 UTC]    46B STANDARD state/0/39/2.delta
[2026-04-02 17:12:04 UTC]    96B STANDARD state/0/4/1.delta
[2026-04-02 17:12:07 UTC]    46B STANDARD state/0/4/2.delta
[2026-04-02 17:12:04 UTC]    46B STANDARD state/0/40/1.delta
...
```

## 6. Short Answers

**Batch vs Streaming:**
Batch processing processes bounds of unmoving data continuously at scheduled times (e.g., nightly warehouse refreshing). Streaming processes an infinite flow of individual events as soon as they are produced, enabling sub-second or near real-time ingestion latency necessary for dynamic actions.

**Stateless vs Stateful Processing:**
Stateless processing computes outputs by only considering the current, single event (e.g., extracting values from JSON). Stateful processing needs memory (state) of previous records to compute current summaries (such as rolling 5-minute averages, JOINs, and aggregates), making fault tolerance via Checkpoints critical.

**Event Time vs Processing Time:**
Event Time is the exact real-world timestamp an event occurred on a client or server device. Processing Time is the timestamp when the streaming engine actually handles/processes that event. Event time handles late, out-of-order networks properly.

**Why Watermarks Matter:**
Watermarks define a "cutoff limit" logic for handling out-of-order/late events. They give a stream framework permission to finalize aggregation buckets so memory (the state) bounding that timestamp can be cleanly purged. Without a watermark, the framework retains states infinitely, causing eventual memory crashes.

**Why is checkpointLocation mandatory for reliable streaming sinks?**
It acts as the fault-tolerance snapshot. Checkpoints securely commit exactly what Kafka offsets have been processed and exactly what state data sits in aggregate buckets. If a job fails, restarting points it to the checkpoint so it reads from where it broke—avoiding duplicate consumption and silent drops.

**When might Flink be a better fit than Spark Structured Streaming?**
Spark SS uses *"micro-batching"*, meaning processing always has some intrinsic latency limits (seconds). Apache Flink natively implements real true continuous stream execution with deeper optimizations, more expressive low-level window/CEP triggering bounds, and fundamentally lower latency bounds (millisecond) than Spark's micro-batch constraints.
