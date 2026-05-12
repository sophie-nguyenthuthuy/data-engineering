"""
Column-level encryption engine using envelope encryption.

Scheme:
  - CMK lives in KMS (local or AWS).
  - Per-record DEK generated via KMS.generate_data_key().
  - Each PII column encrypted with DEK using AES-256-GCM (authenticated).
  - Encrypted DEK + nonces stored alongside ciphertext.

Record wire format (stored as JSON in S3):
{
  "record_id": "uuid",
  "customer_id": "cust_xyz",
  "schema_version": 1,
  "key_version": 2,              # CMK version used
  "encrypted_dek": "<base64>",   # DEK encrypted under CMK
  "encrypted_columns": {
    "ssn":   {"ct": "<base64>", "nonce": "<base64>"},
    "email": {"ct": "<base64>", "nonce": "<base64>"}
  },
  "plaintext_columns": {
    "product_id": "prod_123",
    "event_ts": "2024-01-01T00:00:00Z"
  }
}
"""

import base64
import json
import os
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any

from cryptography.hazmat.primitives.ciphers.aead import AESGCM

from ..kms.client import KMSClient

SCHEMA_VERSION = 1

# Columns that must be encrypted — anything else is stored plaintext
DEFAULT_PII_COLUMNS = frozenset(["ssn", "email", "phone", "dob", "full_name", "address", "ip_address"])


@dataclass
class EncryptedRecord:
    record_id: str
    customer_id: str
    schema_version: int
    key_version: int
    encrypted_dek: str      # base64
    encrypted_columns: dict[str, dict]   # {col: {ct, nonce}}
    plaintext_columns: dict[str, Any]
    created_at: str = field(default_factory=lambda: datetime.now(timezone.utc).isoformat())

    def to_dict(self) -> dict:
        return {
            "record_id": self.record_id,
            "customer_id": self.customer_id,
            "schema_version": self.schema_version,
            "key_version": self.key_version,
            "encrypted_dek": self.encrypted_dek,
            "encrypted_columns": self.encrypted_columns,
            "plaintext_columns": self.plaintext_columns,
            "created_at": self.created_at,
        }

    def to_json(self) -> str:
        return json.dumps(self.to_dict())

    @classmethod
    def from_dict(cls, data: dict) -> "EncryptedRecord":
        return cls(
            record_id=data["record_id"],
            customer_id=data["customer_id"],
            schema_version=data["schema_version"],
            key_version=data["key_version"],
            encrypted_dek=data["encrypted_dek"],
            encrypted_columns=data["encrypted_columns"],
            plaintext_columns=data["plaintext_columns"],
            created_at=data.get("created_at", ""),
        )

    @classmethod
    def from_json(cls, raw: str) -> "EncryptedRecord":
        return cls.from_dict(json.loads(raw))


class EncryptionEngine:
    """
    Encrypts/decrypts records at the column level.

    Dual-read support: when decrypting, the caller can provide a fallback
    CMK key_id (previous version) if decryption with the primary key fails.
    This covers the window between a key rotation starting and completing.
    """

    def __init__(self, kms_client: KMSClient | None = None, pii_columns: frozenset[str] | None = None):
        self._kms = kms_client or KMSClient()
        self._pii_columns = pii_columns or DEFAULT_PII_COLUMNS

    # ------------------------------------------------------------------
    # Encrypt
    # ------------------------------------------------------------------

    def encrypt_record(
        self,
        customer_id: str,
        cmk_id: str,
        key_version: int,
        row: dict[str, Any],
    ) -> EncryptedRecord:
        """
        Encrypt all PII columns in *row* and return an EncryptedRecord.
        Non-PII columns are passed through as plaintext.
        """
        dek_resp = self._kms.generate_data_key(cmk_id)
        dek_plaintext: bytes = dek_resp["Plaintext"]
        dek_ciphertext: bytes = dek_resp["CiphertextBlob"]

        encrypted_columns: dict[str, dict] = {}
        plaintext_columns: dict[str, Any] = {}

        aesgcm = AESGCM(dek_plaintext)

        for col, value in row.items():
            if col in self._pii_columns and value is not None:
                nonce = os.urandom(12)
                payload = json.dumps(value).encode()
                ct = aesgcm.encrypt(nonce, payload, col.encode())
                encrypted_columns[col] = {
                    "ct": _b64(ct),
                    "nonce": _b64(nonce),
                }
            else:
                plaintext_columns[col] = value

        # Zero out plaintext DEK from memory immediately
        dek_bytes = bytearray(dek_plaintext)
        for i in range(len(dek_bytes)):
            dek_bytes[i] = 0

        return EncryptedRecord(
            record_id=str(uuid.uuid4()),
            customer_id=customer_id,
            schema_version=SCHEMA_VERSION,
            key_version=key_version,
            encrypted_dek=_b64(dek_ciphertext),
            encrypted_columns=encrypted_columns,
            plaintext_columns=plaintext_columns,
        )

    # ------------------------------------------------------------------
    # Decrypt
    # ------------------------------------------------------------------

    def decrypt_record(
        self,
        record: EncryptedRecord,
        cmk_id: str,
        fallback_cmk_id: str | None = None,
    ) -> dict[str, Any]:
        """
        Decrypt an EncryptedRecord back to a plain dict.

        If *fallback_cmk_id* is provided and decryption with *cmk_id* fails
        (e.g., during key rotation dual-read window), it retries with the fallback.
        """
        dek_ciphertext = _d64(record.encrypted_dek)

        dek_plaintext = self._try_decrypt_dek(dek_ciphertext, cmk_id, fallback_cmk_id)

        aesgcm = AESGCM(dek_plaintext)
        result = dict(record.plaintext_columns)

        for col, blob in record.encrypted_columns.items():
            nonce = _d64(blob["nonce"])
            ct = _d64(blob["ct"])
            payload = aesgcm.decrypt(nonce, ct, col.encode())
            result[col] = json.loads(payload.decode())

        return result

    def _try_decrypt_dek(self, ciphertext: bytes, primary_key_id: str, fallback_key_id: str | None) -> bytes:
        try:
            return self._kms.decrypt_data_key(ciphertext, primary_key_id)
        except Exception:
            if fallback_key_id:
                return self._kms.decrypt_data_key(ciphertext, fallback_key_id)
            raise

    # ------------------------------------------------------------------
    # Re-encrypt (key rotation)
    # ------------------------------------------------------------------

    def re_encrypt_record(
        self,
        record: EncryptedRecord,
        old_cmk_id: str,
        new_cmk_id: str,
        new_key_version: int,
    ) -> EncryptedRecord:
        """
        Re-encrypt the DEK from old CMK to new CMK.
        The column ciphertext itself does NOT change — only the envelope key changes.
        This is O(1) regardless of column data size.
        """
        old_dek_ct = _d64(record.encrypted_dek)
        new_dek_ct = self._kms.re_encrypt_data_key(old_dek_ct, old_cmk_id, new_cmk_id)

        return EncryptedRecord(
            record_id=record.record_id,
            customer_id=record.customer_id,
            schema_version=record.schema_version,
            key_version=new_key_version,
            encrypted_dek=_b64(new_dek_ct),
            encrypted_columns=record.encrypted_columns,
            plaintext_columns=record.plaintext_columns,
            created_at=record.created_at,
        )


# ------------------------------------------------------------------
# Helpers
# ------------------------------------------------------------------

def _b64(data: bytes) -> str:
    return base64.b64encode(data).decode()


def _d64(s: str) -> bytes:
    return base64.b64decode(s)
