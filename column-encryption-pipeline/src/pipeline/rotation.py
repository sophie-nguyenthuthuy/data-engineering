"""
Key rotation pipeline.

Strategy:
  1. Create a new CMK for the customer.
  2. In the registry, mark old version as 'rotating_out', new version as 'active'.
     → Dual-read window opens: reads try new key first, fall back to old key.
  3. Scan all historical records in S3 for the customer.
  4. For each record: re-encrypt the DEK from old CMK → new CMK (O(1) per record).
  5. Write updated record back to S3 (atomic overwrite by same key).
  6. After all records are migrated, mark old version 'retired' in registry.
     → Dual-read window closes.
  7. (Optional) Disable old CMK in KMS.

Re-encryption is DEK-only: we re-wrap the envelope key rather than
re-encrypting all column ciphertext, making each record update O(1)
in data size.
"""

import logging
import time
from dataclasses import dataclass, field
from typing import Callable, Optional

from tqdm import tqdm

from ..config import get_config
from ..encryption.engine import EncryptionEngine
from ..kms.client import KMSClient
from ..kms.key_registry import KeyRegistry
from ..storage.s3_store import RecordStore

logger = logging.getLogger(__name__)


@dataclass
class RotationResult:
    customer_id: str
    old_version: int
    new_version: int
    records_migrated: int
    records_failed: int
    errors: list[str] = field(default_factory=list)
    duration_seconds: float = 0.0

    @property
    def success(self) -> bool:
        return self.records_failed == 0


class RotationPipeline:
    def __init__(
        self,
        kms_client: KMSClient | None = None,
        engine: EncryptionEngine | None = None,
        registry: KeyRegistry | None = None,
        store: RecordStore | None = None,
        progress: bool = True,
    ):
        cfg = get_config()
        self._kms = kms_client or KMSClient()
        self._engine = engine or EncryptionEngine(self._kms)
        self._registry = registry or KeyRegistry(cfg.key_registry_path)
        self._store = store or RecordStore()
        self._batch_size = cfg.rotation_batch_size
        self._progress = progress

    def rotate_customer_key(
        self,
        customer_id: str,
        on_progress: Optional[Callable[[int, int], None]] = None,
        disable_old_key: bool = True,
    ) -> RotationResult:
        """
        Rotate the CMK for *customer_id*.

        This is a live operation — reads continue to work throughout via
        the dual-read window in the ingest pipeline.
        """
        start = time.monotonic()
        customer = self._registry.get_customer(customer_id)

        if customer.forgotten:
            raise ValueError(f"Customer {customer_id} has been forgotten; cannot rotate keys")
        if customer.rotation_in_progress:
            raise ValueError(f"Rotation already in progress for {customer_id}")

        # 1. Create new CMK
        resp = self._kms.create_key(f"CMK v{customer.current_version + 1} for customer {customer_id}")
        new_cmk_id = resp["KeyMetadata"]["KeyId"]

        # 2. Open dual-read window
        old_ver, new_ver = self._registry.begin_rotation(customer_id, new_cmk_id)
        logger.info(
            "Rotation started for %s: v%d (%s) → v%d (%s)",
            customer_id, old_ver.version, old_ver.cmk_id, new_ver.version, new_ver.cmk_id,
        )

        # 3 & 4. Migrate all records
        record_ids = self._store.list_record_ids(customer_id)
        total = len(record_ids)
        migrated = 0
        failed = 0
        errors: list[str] = []

        iterator = tqdm(record_ids, desc=f"Rotating {customer_id}", unit="rec") if self._progress else record_ids

        for i, record_id in enumerate(iterator):
            try:
                self._migrate_record(customer_id, record_id, old_ver.cmk_id, new_cmk_id, new_ver.version)
                migrated += 1
            except Exception as exc:
                failed += 1
                msg = f"Failed to migrate record {record_id}: {exc}"
                errors.append(msg)
                logger.error(msg)

            if on_progress and (i % self._batch_size == 0 or i == total - 1):
                on_progress(i + 1, total)

        # 5 & 6. Close dual-read window
        self._registry.complete_rotation(customer_id, old_ver.version)
        logger.info("Rotation complete for %s. Migrated=%d Failed=%d", customer_id, migrated, failed)

        # 7. Optionally disable old CMK
        if disable_old_key and failed == 0:
            try:
                self._kms.disable_key(old_ver.cmk_id)
                logger.info("Disabled old CMK %s", old_ver.cmk_id)
            except Exception as exc:
                logger.warning("Could not disable old CMK %s: %s", old_ver.cmk_id, exc)

        return RotationResult(
            customer_id=customer_id,
            old_version=old_ver.version,
            new_version=new_ver.version,
            records_migrated=migrated,
            records_failed=failed,
            errors=errors,
            duration_seconds=time.monotonic() - start,
        )

    def _migrate_record(
        self,
        customer_id: str,
        record_id: str,
        old_cmk_id: str,
        new_cmk_id: str,
        new_key_version: int,
    ) -> None:
        """Re-wrap the DEK for a single record from old CMK to new CMK."""
        record = self._store.get_record(customer_id, record_id)
        if record is None:
            raise FileNotFoundError(f"Record {record_id} not found in store")

        updated = self._engine.re_encrypt_record(
            record=record,
            old_cmk_id=old_cmk_id,
            new_cmk_id=new_cmk_id,
            new_key_version=new_key_version,
        )
        self._store.put_record(updated)

    # ------------------------------------------------------------------
    # Bulk rotate all customers (e.g., scheduled job)
    # ------------------------------------------------------------------

    def rotate_all_customers(self, disable_old_keys: bool = True) -> list[RotationResult]:
        results = []
        for customer_id in self._registry.list_customers():
            try:
                customer = self._registry.get_customer(customer_id)
                if customer.forgotten or customer.rotation_in_progress:
                    continue
                result = self.rotate_customer_key(customer_id, disable_old_key=disable_old_keys)
                results.append(result)
            except Exception as exc:
                logger.error("Failed to rotate %s: %s", customer_id, exc)
        return results
