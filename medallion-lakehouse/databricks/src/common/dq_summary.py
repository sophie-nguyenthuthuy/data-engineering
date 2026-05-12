# Databricks notebook source
"""Post-run DQ summary. Reads the DLT event log and emits a per-expectation
pass/fail count to a monitoring table. Run as a job task after the pipeline.
"""

dbutils.widgets.text("catalog", "")
CATALOG = dbutils.widgets.get("catalog")

EVENT_LOG_QUERY = f"""
WITH latest AS (
  SELECT *
  FROM event_log(TABLE({CATALOG}.gold.fct_sales))
  WHERE event_type = 'flow_progress'
    AND details:flow_progress.metrics IS NOT NULL
)
SELECT
  timestamp,
  origin.flow_name AS flow,
  explode(details:flow_progress.data_quality.expectations) AS expectation
FROM latest
"""

df = spark.sql(EVENT_LOG_QUERY).selectExpr(
    "timestamp",
    "flow",
    "expectation.name AS rule",
    "expectation.passed_records AS passed",
    "expectation.failed_records AS failed",
)

(
    df.write.mode("append")
    .option("mergeSchema", "true")
    .saveAsTable(f"{CATALOG}.gold.dq_metrics")
)

display(df.groupBy("rule").sum("passed", "failed"))
