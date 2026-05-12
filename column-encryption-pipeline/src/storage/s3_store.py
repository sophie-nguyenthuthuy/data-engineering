"""
Record store — local filesystem or S3 (real AWS / LocalStack).

S3 key layout:
  customers/{customer_id}/records/{record_id}.json

This layout lets us efficiently list all records for a customer during
key rotation or RTBF without a full bucket scan.
"""

import json
import threading
from pathlib import Path
from typing import Iterator, Optional

from ..config import get_config
from ..encryption.engine import EncryptedRecord


class RecordStore:
    def __init__(self):
        cfg = get_config()
        self._mode = cfg.storage_mode
        if self._mode == "local":
            self._base = Path(cfg.local_storage_path)
            self._base.mkdir(parents=True, exist_ok=True)
            self._lock = threading.Lock()
        else:
            import boto3
            kwargs = dict(region_name=cfg.aws_region)
            if cfg.s3_endpoint_url:
                kwargs["endpoint_url"] = cfg.s3_endpoint_url
            if cfg.aws_access_key_id:
                kwargs["aws_access_key_id"] = cfg.aws_access_key_id
                kwargs["aws_secret_access_key"] = cfg.aws_secret_access_key
            self._s3 = boto3.client("s3", **kwargs)
            self._bucket = cfg.s3_bucket

    # ------------------------------------------------------------------
    # Write
    # ------------------------------------------------------------------

    def put_record(self, record: EncryptedRecord) -> str:
        """Persist an encrypted record. Returns the storage key."""
        key = self._record_key(record.customer_id, record.record_id)
        body = record.to_json().encode()
        if self._mode == "local":
            path = self._base / key
            path.parent.mkdir(parents=True, exist_ok=True)
            with self._lock:
                path.write_bytes(body)
        else:
            self._s3.put_object(Bucket=self._bucket, Key=key, Body=body)
        return key

    # ------------------------------------------------------------------
    # Read
    # ------------------------------------------------------------------

    def get_record(self, customer_id: str, record_id: str) -> Optional[EncryptedRecord]:
        key = self._record_key(customer_id, record_id)
        try:
            body = self._read_raw(key)
            return EncryptedRecord.from_json(body.decode())
        except FileNotFoundError:
            return None
        except Exception:
            return None

    def _read_raw(self, key: str) -> bytes:
        if self._mode == "local":
            return (self._base / key).read_bytes()
        resp = self._s3.get_object(Bucket=self._bucket, Key=key)
        return resp["Body"].read()

    # ------------------------------------------------------------------
    # List / scan
    # ------------------------------------------------------------------

    def list_record_ids(self, customer_id: str) -> list[str]:
        prefix = f"customers/{customer_id}/records/"
        if self._mode == "local":
            base = self._base / prefix
            if not base.exists():
                return []
            return [p.stem for p in base.glob("*.json")]
        record_ids = []
        paginator = self._s3.get_paginator("list_objects_v2")
        for page in paginator.paginate(Bucket=self._bucket, Prefix=prefix):
            for obj in page.get("Contents", []):
                fname = obj["Key"].split("/")[-1]
                if fname.endswith(".json"):
                    record_ids.append(fname[:-5])
        return record_ids

    def iter_records(self, customer_id: str) -> Iterator[EncryptedRecord]:
        for rid in self.list_record_ids(customer_id):
            rec = self.get_record(customer_id, rid)
            if rec:
                yield rec

    # ------------------------------------------------------------------
    # Delete (used by RTBF for metadata cleanup, optional)
    # ------------------------------------------------------------------

    def delete_all_records(self, customer_id: str) -> int:
        """
        Physically removes all stored records for a customer.
        In the crypto-shredding model this is optional — deleting the key
        is sufficient for inaccessibility.  Call this if you also want
        to reclaim storage.
        """
        ids = self.list_record_ids(customer_id)
        for rid in ids:
            key = self._record_key(customer_id, rid)
            if self._mode == "local":
                p = self._base / key
                if p.exists():
                    p.unlink()
            else:
                self._s3.delete_object(Bucket=self._bucket, Key=key)
        return len(ids)

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _record_key(customer_id: str, record_id: str) -> str:
        return f"customers/{customer_id}/records/{record_id}.json"
