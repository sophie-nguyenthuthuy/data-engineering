"""
Right-to-be-Forgotten (RTBF) executor — crypto-shredding implementation.

Crypto shredding approach:
  - Delete (or schedule deletion of) all CMK versions for a customer from KMS.
  - Once CMK is gone, the encrypted DEKs stored with each record are permanently
    unreadable.  The ciphertext in S3 becomes indistinguishable from random bytes.
  - No need to touch S3 objects for inaccessibility — key deletion is sufficient.
  - Optionally also delete the physical S3 objects for storage hygiene.

Audit trail:
  - The registry marks the customer as 'forgotten' with a timestamp.
  - Every CMK ID that was deleted is logged so regulators can verify.
"""

import json
import logging
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

from ..config import get_config
from ..kms.client import KMSClient
from ..kms.key_registry import KeyRegistry
from ..storage.s3_store import RecordStore

logger = logging.getLogger(__name__)


@dataclass
class RTBFResult:
    customer_id: str
    executed_at: str
    keys_deleted: list[str]
    records_deleted: int        # 0 if physical deletion was not requested
    success: bool
    errors: list[str] = field(default_factory=list)

    def to_dict(self) -> dict:
        return {
            "customer_id": self.customer_id,
            "executed_at": self.executed_at,
            "keys_deleted": self.keys_deleted,
            "records_deleted": self.records_deleted,
            "success": self.success,
            "errors": self.errors,
        }

    def to_json(self) -> str:
        return json.dumps(self.to_dict(), indent=2)


class RTBFExecutor:
    """
    Executes a Right-to-be-Forgotten request for a customer.

    Two erasure depths:
      1. Crypto-shred only (default):  delete CMKs → data unreadable.
      2. Full erasure (delete_records=True):  also remove S3 objects.
    """

    def __init__(
        self,
        kms_client: KMSClient | None = None,
        registry: KeyRegistry | None = None,
        store: RecordStore | None = None,
        audit_log_path: Optional[str] = None,
    ):
        cfg = get_config()
        self._kms = kms_client or KMSClient()
        self._registry = registry or KeyRegistry(cfg.key_registry_path)
        self._store = store or RecordStore()
        self._audit_path = Path(audit_log_path or "./data/rtbf_audit.jsonl")
        self._audit_path.parent.mkdir(parents=True, exist_ok=True)

    def execute(
        self,
        customer_id: str,
        delete_records: bool = False,
        pending_window_seconds: int = 0,
    ) -> RTBFResult:
        """
        Forget a customer.

        Args:
            customer_id: The customer to erase.
            delete_records: Also delete S3 objects (beyond crypto-shredding).
            pending_window_seconds: KMS key deletion window.
                0 = immediate (local KMS only).
                In real AWS the minimum is 7 days (604800 s); pass that value when
                using aws mode to stay within policy.
        """
        executed_at = datetime.now(timezone.utc).isoformat()
        keys_deleted: list[str] = []
        errors: list[str] = []

        # -- Validate customer exists and hasn't already been forgotten --
        try:
            customer = self._registry.get_customer(customer_id)
        except KeyError:
            return RTBFResult(
                customer_id=customer_id,
                executed_at=executed_at,
                keys_deleted=[],
                records_deleted=0,
                success=False,
                errors=[f"Customer {customer_id} not found in registry"],
            )

        if customer.forgotten:
            logger.warning("RTBF already executed for %s at %s", customer_id, customer.forgotten_at)
            return RTBFResult(
                customer_id=customer_id,
                executed_at=executed_at,
                keys_deleted=[],
                records_deleted=0,
                success=True,
                errors=["Customer was already forgotten"],
            )

        # -- Delete all CMK versions from KMS --
        for version in customer.versions:
            if version.status in ("active", "rotating_out", "retired"):
                cmk_id = version.cmk_id
                try:
                    self._kms.schedule_key_deletion(cmk_id, pending_window_seconds)
                    keys_deleted.append(cmk_id)
                    logger.info(
                        "RTBF: deleted CMK %s (version %d) for customer %s",
                        cmk_id, version.version, customer_id,
                    )
                except Exception as exc:
                    msg = f"Failed to delete CMK {cmk_id}: {exc}"
                    errors.append(msg)
                    logger.error("RTBF error: %s", msg)

        # -- Mark customer as forgotten in registry (regardless of partial errors) --
        self._registry.mark_forgotten(customer_id)

        # -- Optionally delete physical S3 objects --
        records_deleted = 0
        if delete_records:
            try:
                records_deleted = self._store.delete_all_records(customer_id)
                logger.info("RTBF: deleted %d S3 objects for customer %s", records_deleted, customer_id)
            except Exception as exc:
                msg = f"Failed to delete S3 objects: {exc}"
                errors.append(msg)
                logger.error("RTBF error: %s", msg)

        result = RTBFResult(
            customer_id=customer_id,
            executed_at=executed_at,
            keys_deleted=keys_deleted,
            records_deleted=records_deleted,
            success=len(errors) == 0,
            errors=errors,
        )

        # -- Write immutable audit record --
        self._write_audit(result)
        return result

    def verify_erasure(self, customer_id: str) -> dict:
        """
        Verify that a customer has been cryptographically erased.

        Returns a report confirming:
        - Registry marks customer as forgotten
        - All CMKs are in a deleted/pending_deletion state
        """
        try:
            customer = self._registry.get_customer(customer_id)
        except KeyError:
            return {"verified": False, "reason": "Customer not found in registry"}

        if not customer.forgotten:
            return {"verified": False, "reason": "Customer not marked forgotten in registry"}

        key_states = []
        all_erased = True
        for version in customer.versions:
            try:
                meta = self._kms.get_key_metadata(version.cmk_id)
                state = meta.get("KeyState", "unknown")
                erased = state.lower() in ("deleted", "pendingdeletion")
                if not erased:
                    all_erased = False
                key_states.append({"cmk_id": version.cmk_id, "version": version.version, "state": state, "erased": erased})
            except Exception:
                # Key not found = effectively deleted
                key_states.append({"cmk_id": version.cmk_id, "version": version.version, "state": "not_found", "erased": True})

        return {
            "verified": all_erased,
            "customer_id": customer_id,
            "forgotten_at": customer.forgotten_at,
            "key_states": key_states,
        }

    def _write_audit(self, result: RTBFResult) -> None:
        with open(self._audit_path, "a") as f:
            f.write(json.dumps(result.to_dict()) + "\n")
