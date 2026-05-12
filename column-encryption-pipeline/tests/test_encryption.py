"""Tests for the encryption engine — envelope encryption, re-encryption."""

import pytest


def test_encrypt_decrypt_roundtrip(engine, kms_client, sample_row):
    resp = kms_client.create_key("test-cmk")
    cmk_id = resp["KeyMetadata"]["KeyId"]

    record = engine.encrypt_record("cust_1", cmk_id, 1, sample_row)

    # PII columns must not appear in plaintext
    assert "ssn" not in record.plaintext_columns
    assert "email" not in record.plaintext_columns
    assert "ssn" in record.encrypted_columns
    assert "email" in record.encrypted_columns

    # Non-PII columns pass through
    assert record.plaintext_columns["product_id"] == "prod_abc"
    assert record.plaintext_columns["amount"] == 99.99

    # Round-trip decryption
    decrypted = engine.decrypt_record(record, cmk_id)
    assert decrypted["ssn"] == "123-45-6789"
    assert decrypted["email"] == "alice@example.com"
    assert decrypted["product_id"] == "prod_abc"


def test_different_records_have_different_deks(engine, kms_client, sample_row):
    resp = kms_client.create_key("test-cmk")
    cmk_id = resp["KeyMetadata"]["KeyId"]

    r1 = engine.encrypt_record("cust_1", cmk_id, 1, sample_row)
    r2 = engine.encrypt_record("cust_1", cmk_id, 1, sample_row)

    # Each record gets a unique DEK
    assert r1.encrypted_dek != r2.encrypted_dek
    # And unique nonces
    assert r1.encrypted_columns["ssn"]["nonce"] != r2.encrypted_columns["ssn"]["nonce"]


def test_re_encrypt_roundtrip(engine, kms_client, sample_row):
    old_resp = kms_client.create_key("old-cmk")
    new_resp = kms_client.create_key("new-cmk")
    old_cmk = old_resp["KeyMetadata"]["KeyId"]
    new_cmk = new_resp["KeyMetadata"]["KeyId"]

    original = engine.encrypt_record("cust_1", old_cmk, 1, sample_row)

    # Re-encrypt DEK under new CMK
    updated = engine.re_encrypt_record(original, old_cmk, new_cmk, 2)

    assert updated.key_version == 2
    assert updated.encrypted_dek != original.encrypted_dek
    # Column ciphertext is unchanged
    assert updated.encrypted_columns["ssn"]["ct"] == original.encrypted_columns["ssn"]["ct"]

    # Can decrypt with new CMK
    decrypted = engine.decrypt_record(updated, new_cmk)
    assert decrypted["ssn"] == "123-45-6789"

    # Cannot decrypt updated record with old CMK
    from cryptography.exceptions import InvalidTag
    with pytest.raises(Exception):
        engine.decrypt_record(updated, old_cmk)


def test_dual_read_fallback(engine, kms_client, sample_row):
    """Decryption falls back to old CMK when new CMK can't decrypt the DEK."""
    old_resp = kms_client.create_key("old-cmk")
    new_resp = kms_client.create_key("new-cmk")
    old_cmk = old_resp["KeyMetadata"]["KeyId"]
    new_cmk = new_resp["KeyMetadata"]["KeyId"]

    # Record encrypted under old CMK
    record = engine.encrypt_record("cust_1", old_cmk, 1, sample_row)

    # Primary = new CMK (fails), fallback = old CMK (succeeds)
    decrypted = engine.decrypt_record(record, new_cmk, fallback_cmk_id=old_cmk)
    assert decrypted["ssn"] == "123-45-6789"


def test_tampered_ciphertext_rejected(engine, kms_client, sample_row):
    resp = kms_client.create_key("test-cmk")
    cmk_id = resp["KeyMetadata"]["KeyId"]

    record = engine.encrypt_record("cust_1", cmk_id, 1, sample_row)

    # Flip a byte in the SSN ciphertext
    import base64
    ct_bytes = bytearray(base64.b64decode(record.encrypted_columns["ssn"]["ct"]))
    ct_bytes[5] ^= 0xFF
    record.encrypted_columns["ssn"]["ct"] = base64.b64encode(bytes(ct_bytes)).decode()

    with pytest.raises(Exception):
        engine.decrypt_record(record, cmk_id)
