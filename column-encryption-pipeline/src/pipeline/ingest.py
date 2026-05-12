"""
Ingest pipeline — encrypts PII columns and persists records to the store.

Usage:
    pipeline = IngestPipeline()
    pipeline.register_customer("cust_001")
    record_id = pipeline.ingest(customer_id="cust_001", row={...})
    row = pipeline.read(customer_id="cust_001", record_id=record_id)
"""

from typing import Any

from ..config import get_config
from ..encryption.engine import EncryptedRecord, EncryptionEngine
from ..kms.client import KMSClient
from ..kms.key_registry import CustomerKeyRecord, KeyRegistry
from ..storage.s3_store import RecordStore


class CustomerAlreadyExistsError(Exception):
    pass


class CustomerNotFoundError(Exception):
    pass


class CustomerForgottenError(Exception):
    pass


class IngestPipeline:
    def __init__(
        self,
        kms_client: KMSClient | None = None,
        engine: EncryptionEngine | None = None,
        registry: KeyRegistry | None = None,
        store: RecordStore | None = None,
    ):
        cfg = get_config()
        self._kms = kms_client or KMSClient()
        self._engine = engine or EncryptionEngine(self._kms)
        self._registry = registry or KeyRegistry(cfg.key_registry_path)
        self._store = store or RecordStore()

    # ------------------------------------------------------------------
    # Customer registration
    # ------------------------------------------------------------------

    def register_customer(self, customer_id: str, description: str = "") -> CustomerKeyRecord:
        """Create a CMK for the customer and register them in the key registry."""
        existing = self._try_get_customer(customer_id)
        if existing is not None:
            raise CustomerAlreadyExistsError(f"Customer {customer_id} already registered")

        resp = self._kms.create_key(description or f"CMK for customer {customer_id}")
        cmk_id = resp["KeyMetadata"]["KeyId"]
        return self._registry.register_customer(customer_id, cmk_id)

    # ------------------------------------------------------------------
    # Ingest (write)
    # ------------------------------------------------------------------

    def ingest(self, customer_id: str, row: dict[str, Any]) -> str:
        """
        Encrypt PII columns in *row* and persist to the store.
        Returns the record_id.
        """
        customer = self._require_customer(customer_id)
        active = customer.active_version()
        assert active is not None, f"No active key version for {customer_id}"

        record = self._engine.encrypt_record(
            customer_id=customer_id,
            cmk_id=active.cmk_id,
            key_version=active.version,
            row=row,
        )
        self._store.put_record(record)
        return record.record_id

    def ingest_batch(self, customer_id: str, rows: list[dict[str, Any]]) -> list[str]:
        return [self.ingest(customer_id, row) for row in rows]

    # ------------------------------------------------------------------
    # Read (decrypt)
    # ------------------------------------------------------------------

    def read(self, customer_id: str, record_id: str) -> dict[str, Any] | None:
        """
        Decrypt and return a record.

        Dual-read: if a rotation is in progress the engine will automatically
        fall back to the previous CMK if decryption with the current key fails.
        """
        customer = self._require_customer(customer_id)
        record = self._store.get_record(customer_id, record_id)
        if record is None:
            return None
        return self._decrypt(customer, record)

    def read_all(self, customer_id: str) -> list[dict[str, Any]]:
        customer = self._require_customer(customer_id)
        results = []
        for record in self._store.iter_records(customer_id):
            results.append(self._decrypt(customer, record))
        return results

    def _decrypt(self, customer: CustomerKeyRecord, record: EncryptedRecord) -> dict[str, Any]:
        active = customer.active_version()
        previous = customer.previous_version()
        assert active is not None

        primary_cmk = active.cmk_id
        fallback_cmk = previous.cmk_id if previous else None

        return self._engine.decrypt_record(record, primary_cmk, fallback_cmk)

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    def _try_get_customer(self, customer_id: str) -> CustomerKeyRecord | None:
        try:
            return self._registry.get_customer(customer_id)
        except KeyError:
            return None

    def _require_customer(self, customer_id: str) -> CustomerKeyRecord:
        customer = self._try_get_customer(customer_id)
        if customer is None:
            raise CustomerNotFoundError(f"Customer not registered: {customer_id}")
        if customer.forgotten:
            raise CustomerForgottenError(f"Customer {customer_id} has been forgotten — data is cryptographically inaccessible")
        return customer
