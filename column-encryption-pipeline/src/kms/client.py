"""
KMS client facade — routes to LocalKMS or real AWS KMS based on config.
"""

from typing import Optional
from ..config import get_config
from .local_kms import LocalKMS

_local_kms_instance: Optional[LocalKMS] = None


def _get_local_kms() -> LocalKMS:
    global _local_kms_instance
    if _local_kms_instance is None:
        cfg = get_config()
        _local_kms_instance = LocalKMS(
            store_path=cfg.local_kms_store_path,
            master_key_path=cfg.local_kms_master_key_path,
        )
    return _local_kms_instance


def _get_aws_kms():
    import boto3
    cfg = get_config()
    kwargs = dict(region_name=cfg.aws_region)
    if cfg.kms_endpoint_url:
        kwargs["endpoint_url"] = cfg.kms_endpoint_url
    if cfg.aws_access_key_id:
        kwargs["aws_access_key_id"] = cfg.aws_access_key_id
        kwargs["aws_secret_access_key"] = cfg.aws_secret_access_key
    return boto3.client("kms", **kwargs)


class KMSClient:
    """
    Unified KMS interface.

    In local mode wraps LocalKMS.
    In aws mode wraps boto3 KMS using the same method signatures.
    """

    def __init__(self):
        cfg = get_config()
        self._mode = cfg.kms_mode

    # ------------------------------------------------------------------
    # CMK lifecycle
    # ------------------------------------------------------------------

    def create_key(self, description: str = "") -> dict:
        if self._mode == "local":
            return _get_local_kms().create_key(description)
        resp = _get_aws_kms().create_key(Description=description, KeyUsage="ENCRYPT_DECRYPT")
        return resp

    def disable_key(self, key_id: str) -> None:
        if self._mode == "local":
            return _get_local_kms().disable_key(key_id)
        _get_aws_kms().disable_key(KeyId=key_id)

    def schedule_key_deletion(self, key_id: str, pending_window_seconds: int = 0) -> dict:
        if self._mode == "local":
            return _get_local_kms().schedule_key_deletion(key_id, pending_window_seconds)
        # AWS minimum is 7 days; translate seconds to days (floor at 7)
        days = max(7, pending_window_seconds // 86400)
        return _get_aws_kms().schedule_key_deletion(KeyId=key_id, PendingWindowInDays=days)

    def get_key_metadata(self, key_id: str) -> dict:
        if self._mode == "local":
            return _get_local_kms().get_key_metadata(key_id)
        resp = _get_aws_kms().describe_key(KeyId=key_id)
        return resp["KeyMetadata"]

    def list_keys(self) -> list[dict]:
        if self._mode == "local":
            return _get_local_kms().list_keys()
        resp = _get_aws_kms().list_keys()
        return resp.get("Keys", [])

    # ------------------------------------------------------------------
    # Envelope encryption operations
    # ------------------------------------------------------------------

    def generate_data_key(self, key_id: str) -> dict:
        """Returns {Plaintext: bytes, CiphertextBlob: bytes, KeyId: str}."""
        if self._mode == "local":
            return _get_local_kms().generate_data_key(key_id)
        resp = _get_aws_kms().generate_data_key(KeyId=key_id, KeySpec="AES_256")
        return resp

    def decrypt_data_key(self, ciphertext_blob: bytes, key_id: str) -> bytes:
        """Returns plaintext DEK bytes."""
        if self._mode == "local":
            return _get_local_kms().decrypt_data_key(ciphertext_blob, key_id)
        resp = _get_aws_kms().decrypt(CiphertextBlob=ciphertext_blob, KeyId=key_id)
        return resp["Plaintext"]

    def re_encrypt_data_key(self, ciphertext_blob: bytes, source_key_id: str, dest_key_id: str) -> bytes:
        """Re-encrypts DEK from source CMK to dest CMK without exposing plaintext to caller."""
        if self._mode == "local":
            return _get_local_kms().re_encrypt_data_key(ciphertext_blob, source_key_id, dest_key_id)
        resp = _get_aws_kms().re_encrypt(
            CiphertextBlob=ciphertext_blob,
            SourceKeyId=source_key_id,
            DestinationKeyId=dest_key_id,
        )
        return resp["CiphertextBlob"]
