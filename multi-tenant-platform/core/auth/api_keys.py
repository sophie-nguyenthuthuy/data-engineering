import hashlib
import secrets
import string


_ALPHABET = string.ascii_letters + string.digits
_KEY_PREFIX = "mtp_"  # multi-tenant platform
_KEY_LENGTH = 40


def generate_api_key() -> tuple[str, str]:
    """Return (raw_key, hashed_key). Store only the hash."""
    raw = _KEY_PREFIX + "".join(secrets.choice(_ALPHABET) for _ in range(_KEY_LENGTH))
    return raw, hash_api_key(raw)


def hash_api_key(raw_key: str) -> str:
    return hashlib.sha256(raw_key.encode()).hexdigest()


def verify_api_key(raw_key: str, stored_hash: str) -> bool:
    return secrets.compare_digest(hash_api_key(raw_key), stored_hash)
