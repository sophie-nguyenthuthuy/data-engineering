"""Cold tier: gzip-compressed JSON archives on S3 (or local filesystem for testing)."""
from __future__ import annotations

import gzip
import io
import json
import os
import time
from pathlib import Path
from typing import Any, Optional

try:
    import boto3
    from botocore.exceptions import ClientError
except ImportError:
    boto3 = None  # type: ignore
    ClientError = Exception  # type: ignore

from tiered_storage.schemas import DataRecord, Tier, TierMetrics
from tiered_storage.tiers.base import BaseTier


class ColdTier(BaseTier):
    """
    Stores data as gzip-compressed JSON objects in S3 (cold/ prefix) or
    a local directory (for development / testing).

    Objects are named cold/<key>.json.gz. A sidecar manifest tracks
    access metadata to avoid expensive LIST operations.

    In production this maps to S3 Glacier Instant Retrieval or
    Glacier Flexible Retrieval — the restore_object() flow is modeled
    for cost purposes even though the test backend uses local files.
    """

    MANIFEST_KEY = "cold/_manifest.json"

    def __init__(
        self,
        bucket: Optional[str] = None,
        local_path: Optional[str] = None,
        prefix: str = "cold",
        region: str = "us-east-1",
        endpoint_url: Optional[str] = None,
        use_glacier: bool = False,  # set True to emit S3 Glacier storage class
    ):
        if bucket is None and local_path is None:
            raise ValueError("Provide either bucket or local_path")
        self._bucket = bucket
        self._local = Path(local_path) if local_path else None
        self._prefix = prefix.rstrip("/")
        self._use_glacier = use_glacier
        self._s3: Any = None
        self._s3_kwargs: dict = {"region_name": region}
        if endpoint_url:
            self._s3_kwargs["endpoint_url"] = endpoint_url
        self._manifest: dict[str, dict] = {}

    def connect(self) -> None:
        if self._local:
            self._local.mkdir(parents=True, exist_ok=True)
            manifest_path = self._local / "_manifest.json"
            if manifest_path.exists():
                self._manifest = json.loads(manifest_path.read_text())
        elif boto3 and self._bucket:
            self._s3 = boto3.client("s3", **self._s3_kwargs)
            self._load_manifest_s3()

    # ------------------------------------------------------------------
    # Manifest helpers
    # ------------------------------------------------------------------

    def _load_manifest_s3(self) -> None:
        try:
            resp = self._s3.get_object(Bucket=self._bucket, Key=self.MANIFEST_KEY)
            self._manifest = json.loads(resp["Body"].read())
        except ClientError:
            self._manifest = {}

    def _save_manifest(self) -> None:
        if self._local:
            (self._local / "_manifest.json").write_text(json.dumps(self._manifest))
        elif self._s3:
            self._s3.put_object(
                Bucket=self._bucket,
                Key=self.MANIFEST_KEY,
                Body=json.dumps(self._manifest).encode(),
            )

    def _object_path(self, key: str) -> Path:
        safe = key.replace("/", "__")
        return self._local / f"{safe}.json.gz"

    def _s3_key(self, key: str) -> str:
        safe = key.replace("/", "__")
        return f"{self._prefix}/{safe}.json.gz"

    # ------------------------------------------------------------------
    # BaseTier interface
    # ------------------------------------------------------------------

    async def get(self, key: str) -> Optional[DataRecord]:
        if key not in self._manifest:
            return None

        data: Optional[dict] = None

        if self._local:
            path = self._object_path(key)
            if not path.exists():
                return None
            with gzip.open(path, "rt") as f:
                data = json.load(f)
        elif self._s3:
            try:
                resp = self._s3.get_object(Bucket=self._bucket, Key=self._s3_key(key))
                with gzip.open(io.BytesIO(resp["Body"].read()), "rt") as f:
                    data = json.load(f)
            except ClientError:
                return None

        if data is None:
            return None

        record = self._dict_to_record(data)
        record.last_accessed_at = time.time()
        record.access_count += 1
        self._manifest[key]["last_accessed_at"] = record.last_accessed_at
        self._manifest[key]["access_count"] = record.access_count
        self._save_manifest()
        return record

    async def put(self, record: DataRecord) -> None:
        record.tier = Tier.COLD
        payload = json.dumps(self._record_to_dict(record)).encode()
        compressed = gzip.compress(payload, compresslevel=9)

        if self._local:
            with gzip.open(self._object_path(record.key), "wb") as f:
                f.write(payload)
        elif self._s3:
            extra = {}
            if self._use_glacier:
                extra["StorageClass"] = "GLACIER"
            self._s3.put_object(
                Bucket=self._bucket,
                Key=self._s3_key(record.key),
                Body=compressed,
                ContentEncoding="gzip",
                **extra,
            )

        self._manifest[record.key] = {
            "size_bytes": record.size_bytes,
            "compressed_bytes": len(compressed),
            "created_at": record.created_at,
            "last_accessed_at": record.last_accessed_at,
            "access_count": record.access_count,
            "archived_at": time.time(),
        }
        self._save_manifest()

    async def delete(self, key: str) -> bool:
        if key not in self._manifest:
            return False
        if self._local:
            path = self._object_path(key)
            if path.exists():
                path.unlink()
        elif self._s3:
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
            return TierMetrics(Tier.COLD, 0, 0, 0.0, 0.0, 0.0)
        sizes = [v.get("compressed_bytes", v.get("size_bytes", 0)) for v in self._manifest.values()]
        ages = [(now - v["created_at"]) / 86400 for v in self._manifest.values()]
        freqs = [
            v["access_count"] / max((now - v["created_at"]) / 86400, 1)
            for v in self._manifest.values()
        ]
        return TierMetrics(
            tier=Tier.COLD,
            record_count=len(self._manifest),
            total_size_bytes=sum(sizes),
            avg_access_frequency=sum(freqs) / len(freqs),
            oldest_record_age_days=max(ages),
            newest_record_age_days=min(ages),
        )

    async def list_keys(self, prefix: str = "", limit: int = 1000) -> list[str]:
        return [k for k in self._manifest if k.startswith(prefix)][:limit]

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _record_to_dict(r: DataRecord) -> dict:
        return {
            "key": r.key,
            "value": r.value,
            "size_bytes": r.size_bytes,
            "tier": r.tier.value,
            "created_at": r.created_at,
            "last_accessed_at": r.last_accessed_at,
            "access_count": r.access_count,
            "metadata": r.metadata,
        }

    @staticmethod
    def _dict_to_record(d: dict) -> DataRecord:
        return DataRecord(
            key=d["key"],
            value=d["value"],
            size_bytes=d.get("size_bytes", 0),
            tier=Tier(d.get("tier", "cold")),
            created_at=d.get("created_at", time.time()),
            last_accessed_at=d.get("last_accessed_at", time.time()),
            access_count=d.get("access_count", 0),
            metadata=d.get("metadata", {}),
        )
