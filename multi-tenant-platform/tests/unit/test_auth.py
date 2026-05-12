import uuid
from datetime import timedelta

import pytest

from core.auth.jwt import create_access_token, decode_token
from core.auth.api_keys import generate_api_key, verify_api_key, hash_api_key


def test_jwt_roundtrip() -> None:
    user_id = str(uuid.uuid4())
    tenant_id = uuid.uuid4()
    token = create_access_token(user_id, tenant_id, "admin")
    payload = decode_token(token)
    assert payload.sub == user_id
    assert payload.tenant_id == tenant_id
    assert payload.role == "admin"


def test_jwt_expired_raises() -> None:
    user_id = str(uuid.uuid4())
    tenant_id = uuid.uuid4()
    token = create_access_token(user_id, tenant_id, "member", expires_delta=timedelta(seconds=-1))
    with pytest.raises(ValueError, match="Invalid token"):
        decode_token(token)


def test_api_key_verify() -> None:
    raw, hashed = generate_api_key()
    assert raw.startswith("mtp_")
    assert len(raw) > 40
    assert verify_api_key(raw, hashed) is True


def test_api_key_wrong_key_rejected() -> None:
    _, hashed = generate_api_key()
    assert verify_api_key("wrong_key", hashed) is False


def test_api_key_hash_is_deterministic() -> None:
    raw, _ = generate_api_key()
    assert hash_api_key(raw) == hash_api_key(raw)
