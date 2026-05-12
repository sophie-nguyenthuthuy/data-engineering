"""
Envelope encryption for sensitive tenant fields (PII, secrets).
Uses AES-256-GCM via the cryptography library with a per-tenant key
derived from a master key using HKDF.
"""
import base64
import os

from cryptography.hazmat.primitives.ciphers.aead import AESGCM
from cryptography.hazmat.primitives.kdf.hkdf import HKDF
from cryptography.hazmat.primitives import hashes


def _derive_key(master_key: bytes, tenant_id: str) -> bytes:
    hkdf = HKDF(
        algorithm=hashes.SHA256(),
        length=32,
        salt=None,
        info=f"tenant:{tenant_id}".encode(),
    )
    return hkdf.derive(master_key)


def encrypt_field(plaintext: str, master_key: bytes, tenant_id: str) -> str:
    """Returns base64-encoded `nonce || ciphertext` string."""
    key = _derive_key(master_key, tenant_id)
    aesgcm = AESGCM(key)
    nonce = os.urandom(12)
    ct = aesgcm.encrypt(nonce, plaintext.encode(), None)
    return base64.b64encode(nonce + ct).decode()


def decrypt_field(encoded: str, master_key: bytes, tenant_id: str) -> str:
    key = _derive_key(master_key, tenant_id)
    aesgcm = AESGCM(key)
    raw = base64.b64decode(encoded.encode())
    nonce, ct = raw[:12], raw[12:]
    return aesgcm.decrypt(nonce, ct, None).decode()
