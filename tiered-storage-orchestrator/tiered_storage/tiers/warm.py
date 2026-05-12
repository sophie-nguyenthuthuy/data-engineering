"""Warm tier: columnar Parquet files on Amazon S3."""
from __future__ import annotations

import io
import json
import time
from typing import Any, Optional

try:
    import boto3
    from botocore.exceptions import ClientError
except ImportError:
    boto3 = None  # type: ignore
    ClientError = Exception  # type: ignore

try:
    import pyarrow as pa
    import pyarrow.parquet as pq
except ImportError:
    pa = None  # type: ignore
    pq = None  # type: ignore

from tiered_storage.schemas import DataRecord, Tier, TierMetrics
from tiered_storage.tiers.base import BaseTier


class WarmTier(BaseTier):
    """
    Stores data as Parquet objects on S3 under prefix warm/<key>.parquet.
    Each object is a single-row Parquet file containing the serialised record.
    A manifest object (warm/_manifest.json) tracks keys and metadata for
    efficient listing and metrics computation without S3 LIST calls.
    """

    MANIFEST_KEY = "warm/_manifest.json"

    def __init__(
        self,
        bucket: str,
        prefix: str = "warm",
        region: str = "us-east-1",
        aws_access_key_id: Optional[str] = None,
        aws_secret_access_key: Optional[str] = None,
        endpoint_url: Optional[str] = None,  # for LocalStack / MinIO
    ):
        self._bucket = bucket
        self._prefix = prefix.rstrip("/")
        self._region = region
        self._s3: Any = None
        self._s3_kwargs: dict = {
            "region_name": region,
        }
        if aws_access_key_id:
            self._s3_kwargs["aws_access_key_id"] = aws_access_key_id
            self._s3_kwargs["aws_secret_access_key"] = aws_secret_access_key
        if endpoint_url:
            self._s3_kwargs["endpoint_url"] = endpoint_url

        self._manifest: dict[str, dict] = {}  # key → {size_bytes, created_at, last_accessed_at, access_count}

    def connect(self) -> None:
        if boto3:
            self._s3 = boto3.client("s3", **self._s3_kwargs)
            self._load_manifest()

    def _load_manifest(self) -> None:
        try:
            resp = self._s3.get_object(Bucket=self._bucket, Key=self.MANIFEST_KEY)
            self._manifest = json.loads(resp["Body"].read())
        except ClientError:
            self._manifest = {}

    def _save_manifest(self) -> None:
        self._s3.put_object(
            Bucket=self._bucket,
            Key=self.MANIFEST_KEY,
            Body=json.dumps(self._manifest).encode(),
            ContentType="application/json",
        )

    def _s3_key(self, key: str) -> str:
        safe = key.replace("/", "__")
        return f"{self._prefix}/{safe}.parquet"

    # ------------------------------------------------------------------
    # BaseTier interface
    # ------------------------------------------------------------------

    async def get(self, key: str) -> Optional[DataRecord]:
        if not self._s3 or key not in self._manifest:
            return None
        try:
            resp = self._s3.get_object(Bucket=self._bucket, Key=self._s3_key(key))
            buf = io.BytesIO(resp["Body"].read())
        except ClientError:
            return None

        record = self._parquet_to_record(buf)
        record.last_accessed_at = time.time()
        record.access_count += 1

        self._manifest[key]["last_accessed_at"] = record.last_accessed_at
        self._manifest[key]["access_count"] = record.access_count
        self._save_manifest()
        return record

    async def put(self, record: DataRecord) -> None:
        if not self._s3:
            return
        record.tier = Tier.WARM
        buf = self._record_to_parquet(record)
        self._s3.put_object(
            Bucket=self._bucket,
            Key=self._s3_key(record.key),
            Body=buf.getvalue(),
            ContentType="application/octet-stream",
            Metadata={
                "tier": "warm",
                "created-at": str(record.created_at),
                "last-accessed-at": str(record.last_accessed_at),
            },
        )
        self._manifest[record.key] = {
            "size_bytes": record.size_bytes,
            "created_at": record.created_at,
            "last_accessed_at": record.last_accessed_at,
            "access_count": record.access_count,
        }
        self._save_manifest()

    async def delete(self, key: str) -> bool:
        if not self._s3 or key not in self._manifest:
            return False
        try:
            self._s3.delete_object(Bucket=self._bucket, Key=self._s3_key(key))
        except ClientError:
            pass
        del self._manifest[key]
        self._save_manifest()
        return True

    async def exists(self, key: str) -> bool:
        return key in self._manifest

    async def metrics(self) -> TierMetrics:
        now = time.time()
        if not self._manifest:
            return TierMetrics(Tier.WARM, 0, 0, 0.0, 0.0, 0.0)
        sizes = [v["size_bytes"] for v in self._manifest.values()]
        freqs = []
        ages = []
        for v in self._manifest.values():
            age_days = max((now - v["created_at"]) / 86400, 1)
            freqs.append(v["access_count"] / age_days)
            ages.append(age_days)
        return TierMetrics(
            tier=Tier.WARM,
            record_count=len(self._manifest),
            total_size_bytes=sum(sizes),
            avg_access_frequency=sum(freqs) / len(freqs),
            oldest_record_age_days=max(ages),
            newest_record_age_days=min(ages),
        )

    async def list_keys(self, prefix: str = "", limit: int = 1000) -> list[str]:
        keys = [k for k in self._manifest if k.startswith(prefix)]
        return keys[:limit]

    async def get_stale_keys(self, idle_days: float, min_freq: float) -> list[str]:
        now = time.time()
        cutoff = now - idle_days * 86400
        stale = []
        for key, meta in self._manifest.items():
            if meta["last_accessed_at"] < cutoff:
                stale.append(key)
                continue
            age_days = max((now - meta["created_at"]) / 86400, 1)
            freq = meta["access_count"] / age_days
            if freq < min_freq:
                stale.append(key)
        return stale

    # ------------------------------------------------------------------
    # Parquet serialisation helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _record_to_parquet(record: DataRecord) -> io.BytesIO:
        if pa is None or pq is None:
            # Fallback: JSON in a BytesIO
            buf = io.BytesIO()
            data = json.dumps({
                "key": record.key,
                "value": json.dumps(record.value),
                "size_bytes": record.size_bytes,
                "tier": record.tier.value,
                "created_at": record.created_at,
                "last_accessed_at": record.last_accessed_at,
                "access_count": record.access_count,
                "metadata": json.dumps(record.metadata),
            }).encode()
            buf.write(data)
            buf.seek(0)
            return buf

        table = pa.table({
            "key": [record.key],
            "value": [json.dumps(record.value)],
            "size_bytes": pa.array([record.size_bytes], type=pa.int64()),
            "tier": [record.tier.value],
            "created_at": pa.array([record.created_at], type=pa.float64()),
            "last_accessed_at": pa.array([record.last_accessed_at], type=pa.float64()),
            "access_count": pa.array([record.access_count], type=pa.int64()),
            "metadata": [json.dumps(record.metadata)],
        })
        buf = io.BytesIO()
        pq.write_table(table, buf, compression="snappy")
        buf.seek(0)
        return buf

    @staticmethod
    def _parquet_to_record(buf: io.BytesIO) -> DataRecord:
        if pq is None:
            data = json.loads(buf.read().decode())
            return DataRecord(
                key=data["key"],
                value=json.loads(data["value"]),
                size_bytes=data.get("size_bytes", 0),
                tier=Tier(data.get("tier", "warm")),
                created_at=data.get("created_at", time.time()),
                last_accessed_at=data.get("last_accessed_at", time.time()),
                access_count=data.get("access_count", 0),
                metadata=json.loads(data.get("metadata", "{}")),
            )

        table = pq.read_table(buf)
        row = {col: table.column(col)[0].as_py() for col in table.schema.names}
        return DataRecord(
            key=row["key"],
            value=json.loads(row["value"]),
            size_bytes=row.get("size_bytes", 0),
            tier=Tier(row.get("tier", "warm")),
            created_at=row.get("created_at", time.time()),
            last_accessed_at=row.get("last_accessed_at", time.time()),
            access_count=row.get("access_count", 0),
            metadata=json.loads(row.get("metadata", "{}")),
        )
