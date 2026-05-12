import pytest
from core.security.encryption import encrypt_field, decrypt_field


MASTER_KEY = b"a" * 32
TENANT_A = "tenant-aaa"
TENANT_B = "tenant-bbb"


def test_encrypt_decrypt_roundtrip() -> None:
    plaintext = "secret-value-123"
    ct = encrypt_field(plaintext, MASTER_KEY, TENANT_A)
    assert ct != plaintext
    assert decrypt_field(ct, MASTER_KEY, TENANT_A) == plaintext


def test_different_tenants_different_ciphertext() -> None:
    ct_a = encrypt_field("hello", MASTER_KEY, TENANT_A)
    ct_b = encrypt_field("hello", MASTER_KEY, TENANT_B)
    assert ct_a != ct_b


def test_cross_tenant_decrypt_fails() -> None:
    ct = encrypt_field("secret", MASTER_KEY, TENANT_A)
    with pytest.raises(Exception):
        decrypt_field(ct, MASTER_KEY, TENANT_B)


def test_tampered_ciphertext_fails() -> None:
    ct = encrypt_field("secret", MASTER_KEY, TENANT_A)
    tampered = ct[:-4] + "XXXX"
    with pytest.raises(Exception):
        decrypt_field(tampered, MASTER_KEY, TENANT_A)
