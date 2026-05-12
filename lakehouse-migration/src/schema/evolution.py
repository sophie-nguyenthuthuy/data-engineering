"""Schema evolution helpers: detect drift between source and Delta target, apply safe changes."""

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import StructType
from delta import DeltaTable
import logging

logger = logging.getLogger(__name__)


def detect_schema_drift(source_df: DataFrame, target_path: str, spark: SparkSession) -> dict:
    """Return added / removed / type-changed columns between source and existing Delta table."""
    if not DeltaTable.isDeltaTable(spark, target_path):
        return {"added": [], "removed": [], "type_changed": []}

    target_schema: StructType = spark.read.format("delta").load(target_path).schema
    source_fields = {f.name: f.dataType for f in source_df.schema.fields}
    target_fields = {f.name: f.dataType for f in target_schema.fields}

    added = [n for n in source_fields if n not in target_fields]
    removed = [n for n in target_fields if n not in source_fields]
    type_changed = [
        {"column": n, "from": str(target_fields[n]), "to": str(source_fields[n])}
        for n in source_fields
        if n in target_fields and source_fields[n] != target_fields[n]
    ]

    if added or removed or type_changed:
        logger.warning(
            "Schema drift detected — added=%s  removed=%s  type_changed=%s",
            added, removed, type_changed,
        )

    return {"added": added, "removed": removed, "type_changed": type_changed}


def enable_column_mapping(spark: SparkSession, target_path: str) -> None:
    """Enable Delta column mapping (name mode) so columns can be renamed without rewrite."""
    dt = DeltaTable.forPath(spark, target_path)
    dt.upgradeTableProtocol(2, 5)
    spark.sql(f"""
        ALTER TABLE delta.`{target_path}`
        SET TBLPROPERTIES (
            'delta.columnMapping.mode' = 'name',
            'delta.minReaderVersion' = '2',
            'delta.minWriterVersion' = '5'
        )
    """)
    logger.info("Column mapping enabled on %s", target_path)
