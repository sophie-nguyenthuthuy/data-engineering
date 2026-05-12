"""Tests for the key rotation pipeline — live rotation with dual-read."""

import pytest


def _setup_customer_with_records(pipeline, customer_id, n=5):
    pipeline.register_customer(customer_id)
    rows = [
        {"ssn": f"000-00-{i:04d}", "email": f"user{i}@example.com", "product_id": f"p{i}"}
        for i in range(n)
    ]
    return pipeline.ingest_batch(customer_id, rows)


def test_rotation_migrates_all_records(pipeline, rotation_pipeline):
    record_ids = _setup_customer_with_records(pipeline, "cust_rot", 10)

    result = rotation_pipeline.rotate_customer_key("cust_rot")

    assert result.success
    assert result.records_migrated == 10
    assert result.records_failed == 0
    assert result.old_version == 1
    assert result.new_version == 2


def test_records_readable_after_rotation(pipeline, rotation_pipeline):
    record_ids = _setup_customer_with_records(pipeline, "cust_post_rot", 5)

    rotation_pipeline.rotate_customer_key("cust_post_rot")

    # All records must still decrypt correctly after rotation
    all_records = pipeline.read_all("cust_post_rot")
    assert len(all_records) == 5
    ssns = {r["ssn"] for r in all_records}
    assert ssns == {f"000-00-{i:04d}" for i in range(5)}


def test_dual_read_during_rotation(pipeline, kms_client, registry, store, engine, cfg):
    """Simulate mid-rotation state where some records use old key, some use new key."""
    from src.pipeline.rotation import RotationPipeline

    pipeline.register_customer("cust_dual")
    row = {"ssn": "111-22-3333", "email": "x@x.com", "product_id": "p0"}
    record_id = pipeline.ingest("cust_dual", row)

    # Manually begin rotation without migrating records
    customer = registry.get_customer("cust_dual")
    old_ver = customer.active_version()
    resp = kms_client.create_key("new-cmk-for-dual")
    new_cmk_id = resp["KeyMetadata"]["KeyId"]
    old_v, new_v = registry.begin_rotation("cust_dual", new_cmk_id)

    # Record is still encrypted under old CMK.
    # IngestPipeline should fall back to old CMK during dual-read window.
    result = pipeline.read("cust_dual", record_id)
    assert result["ssn"] == "111-22-3333"


def test_rotation_idempotent_on_already_rotated_key(pipeline, rotation_pipeline):
    _setup_customer_with_records(pipeline, "cust_idem", 3)
    rotation_pipeline.rotate_customer_key("cust_idem")

    # Second rotation should also succeed (now on v2 → v3)
    result = rotation_pipeline.rotate_customer_key("cust_idem")
    assert result.success
    assert result.old_version == 2
    assert result.new_version == 3


def test_rotation_raises_if_already_in_progress(pipeline, registry, kms_client):
    _setup_customer_with_records(pipeline, "cust_dup_rot", 2)

    # Manually set rotation_in_progress without completing
    customer = registry.get_customer("cust_dup_rot")
    resp = kms_client.create_key("extra-cmk")
    registry.begin_rotation("cust_dup_rot", resp["KeyMetadata"]["KeyId"])

    from src.pipeline.rotation import RotationPipeline
    rp = RotationPipeline(kms_client, progress=False)
    with pytest.raises(ValueError, match="already in progress"):
        rp.rotate_customer_key("cust_dup_rot")
