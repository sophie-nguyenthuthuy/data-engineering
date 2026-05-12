"""
Local KMS simulation — no AWS required.

Uses AES-256-GCM to protect key material under a master key stored on disk.
Implements the same interface surface used by the KMS client wrapper so
the rest of the system is storage-agnostic.
"""

import json
import os
import base64
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

from cryptography.hazmat.primitives.ciphers.aead import AESGCM
from cryptography.hazmat.primitives.kdf.scrypt import Scrypt
from cryptography.hazmat.backends import default_backend


class KeyNotFoundError(Exception):
    pass


class KeyDisabledError(Exception):
    pass


class KeyPendingDeletionError(Exception):
    pass


class LocalKMS:
    """
    File-backed KMS simulation.

    Master key:  32 random bytes persisted to disk (master.key).
    Key store:   JSON file; each entry holds key material encrypted under master key.
    """

    def __init__(self, store_path: str, master_key_path: str):
        self._store_path = Path(store_path)
        self._master_key = self._load_or_create_master_key(Path(master_key_path))
        self._store: dict = self._load_store()

    # ------------------------------------------------------------------
    # Public KMS-compatible interface
    # ------------------------------------------------------------------

    def create_key(self, description: str = "") -> dict:
        """Create a new Customer Master Key (CMK)."""
        key_id = str(uuid.uuid4())
        key_material = os.urandom(32)
        encrypted_material, nonce = self._encrypt_with_master(key_material)

        self._store[key_id] = {
            "key_id": key_id,
            "description": description,
            "encrypted_material": base64.b64encode(encrypted_material).decode(),
            "nonce": base64.b64encode(nonce).decode(),
            "created_at": _now_iso(),
            "status": "enabled",
            "pending_deletion_at": None,
        }
        self._save_store()
        return {"KeyMetadata": {"KeyId": key_id, "Description": description, "KeyState": "Enabled"}}

    def generate_data_key(self, key_id: str) -> dict:
        """
        Generate a 256-bit Data Encryption Key (DEK).

        Returns:
            Plaintext DEK + DEK encrypted under the CMK (envelope encryption).
        """
        cmk_material = self._get_key_material(key_id)
        dek_plaintext = os.urandom(32)
        aesgcm = AESGCM(cmk_material)
        nonce = os.urandom(12)
        dek_ciphertext = nonce + aesgcm.encrypt(nonce, dek_plaintext, key_id.encode())

        return {
            "Plaintext": dek_plaintext,
            "CiphertextBlob": dek_ciphertext,
            "KeyId": key_id,
        }

    def decrypt_data_key(self, ciphertext_blob: bytes, key_id: str) -> bytes:
        """Decrypt a DEK ciphertext blob back to plaintext."""
        cmk_material = self._get_key_material(key_id)
        nonce, ciphertext = ciphertext_blob[:12], ciphertext_blob[12:]
        aesgcm = AESGCM(cmk_material)
        return aesgcm.decrypt(nonce, ciphertext, key_id.encode())

    def re_encrypt_data_key(self, ciphertext_blob: bytes, source_key_id: str, dest_key_id: str) -> bytes:
        """Re-encrypt a DEK from one CMK to another (used during key rotation)."""
        plaintext = self.decrypt_data_key(ciphertext_blob, source_key_id)
        dest_material = self._get_key_material(dest_key_id)
        aesgcm = AESGCM(dest_material)
        nonce = os.urandom(12)
        return nonce + aesgcm.encrypt(nonce, plaintext, dest_key_id.encode())

    def disable_key(self, key_id: str) -> None:
        entry = self._get_entry(key_id)
        entry["status"] = "disabled"
        self._save_store()

    def schedule_key_deletion(self, key_id: str, pending_window_seconds: int = 0) -> dict:
        """
        Mark key for deletion.  With pending_window_seconds=0 the key is deleted immediately
        (local mode only — real KMS enforces a minimum 7-day window).
        """
        entry = self._get_entry(key_id)
        if pending_window_seconds == 0:
            del self._store[key_id]
            self._save_store()
            return {"KeyMetadata": {"KeyId": key_id, "KeyState": "Deleted"}}

        entry["status"] = "pending_deletion"
        entry["pending_deletion_at"] = _now_iso()
        self._save_store()
        return {"KeyMetadata": {"KeyId": key_id, "KeyState": "PendingDeletion"}}

    def get_key_metadata(self, key_id: str) -> dict:
        entry = self._get_entry(key_id)
        return {
            "KeyId": entry["key_id"],
            "Description": entry.get("description", ""),
            "KeyState": entry["status"].title().replace("_", ""),
            "CreationDate": entry["created_at"],
        }

    def list_keys(self) -> list[dict]:
        return [{"KeyId": k, "KeyState": v["status"]} for k, v in self._store.items()]

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _get_entry(self, key_id: str) -> dict:
        if key_id not in self._store:
            raise KeyNotFoundError(f"Key not found: {key_id}")
        return self._store[key_id]

    def _get_key_material(self, key_id: str) -> bytes:
        entry = self._get_entry(key_id)
        if entry["status"] == "pending_deletion":
            raise KeyPendingDeletionError(f"Key {key_id} is pending deletion")
        if entry["status"] == "disabled":
            raise KeyDisabledError(f"Key {key_id} is disabled")
        encrypted = base64.b64decode(entry["encrypted_material"])
        nonce = base64.b64decode(entry["nonce"])
        aesgcm = AESGCM(self._master_key)
        return aesgcm.decrypt(nonce, encrypted, None)

    def _encrypt_with_master(self, plaintext: bytes) -> tuple[bytes, bytes]:
        nonce = os.urandom(12)
        aesgcm = AESGCM(self._master_key)
        ciphertext = aesgcm.encrypt(nonce, plaintext, None)
        return ciphertext, nonce

    @staticmethod
    def _load_or_create_master_key(path: Path) -> bytes:
        path.parent.mkdir(parents=True, exist_ok=True)
        if path.exists():
            raw = path.read_bytes()
            if len(raw) == 32:
                return raw
        key = os.urandom(32)
        path.write_bytes(key)
        path.chmod(0o600)
        return key

    def _load_store(self) -> dict:
        self._store_path.parent.mkdir(parents=True, exist_ok=True)
        if self._store_path.exists():
            return json.loads(self._store_path.read_text())
        return {}

    def _save_store(self) -> None:
        self._store_path.write_text(json.dumps(self._store, indent=2))


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()
