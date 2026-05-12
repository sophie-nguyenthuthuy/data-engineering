"""Tests for Right-to-be-Forgotten executor — crypto-shredding."""

import pytest


def _register_with_records(pipeline, customer_id, n=3):
    pipeline.register_customer(customer_id)
    rows = [{"ssn": f"999-00-{i:04d}", "email": f"user{i}@delete.me", "product_id": f"p{i}"} for i in range(n)]
    return pipeline.ingest_batch(customer_id, rows)


def test_rtbf_makes_data_unreadable(pipeline, rtbf_executor):
    record_ids = _register_with_records(pipeline, "cust_forget")

    result = rtbf_executor.execute("cust_forget")

    assert result.success
    assert len(result.keys_deleted) >= 1

    # Attempting to read now raises
    from src.pipeline.ingest import CustomerForgottenError
    with pytest.raises(CustomerForgottenError):
        pipeline.read("cust_forget", record_ids[0])


def test_rtbf_physical_deletion(pipeline, rtbf_executor, store):
    _register_with_records(pipeline, "cust_phys", 5)

    result = rtbf_executor.execute("cust_phys", delete_records=True)

    assert result.success
    assert result.records_deleted == 5

    remaining = store.list_record_ids("cust_phys")
    assert len(remaining) == 0


def test_rtbf_audit_log_written(pipeline, rtbf_executor, tmp_path):
    import json
    _register_with_records(pipeline, "cust_audit")

    rtbf_executor.execute("cust_audit")

    audit_path = tmp_path / "audit.jsonl"
    assert audit_path.exists()
    lines = audit_path.read_text().strip().split("\n")
    assert len(lines) == 1
    entry = json.loads(lines[0])
    assert entry["customer_id"] == "cust_audit"
    assert entry["success"] is True


def test_rtbf_idempotent(pipeline, rtbf_executor):
    _register_with_records(pipeline, "cust_idem_rtbf")

    r1 = rtbf_executor.execute("cust_idem_rtbf")
    r2 = rtbf_executor.execute("cust_idem_rtbf")

    assert r1.success
    assert r2.success
    # Second call is a no-op
    assert r2.errors == ["Customer was already forgotten"]


def test_rtbf_unknown_customer(rtbf_executor):
    result = rtbf_executor.execute("does_not_exist")
    assert not result.success
    assert "not found" in result.errors[0].lower()


def test_verify_erasure(pipeline, rtbf_executor):
    _register_with_records(pipeline, "cust_verify")

    # Before erasure: not verified
    report = rtbf_executor.verify_erasure("cust_verify")
    assert not report["verified"]

    rtbf_executor.execute("cust_verify")

    # After erasure: verified
    report = rtbf_executor.verify_erasure("cust_verify")
    assert report["verified"]
    assert all(ks["erased"] for ks in report["key_states"])


def test_rtbf_after_rotation(pipeline, rotation_pipeline, rtbf_executor):
    """RTBF must delete ALL key versions (including rotated-out ones)."""
    _register_with_records(pipeline, "cust_rot_rtbf", 3)
    rotation_pipeline.rotate_customer_key("cust_rot_rtbf")

    result = rtbf_executor.execute("cust_rot_rtbf")

    assert result.success
    # Both v1 and v2 CMKs should be deleted
    assert len(result.keys_deleted) == 2

    report = rtbf_executor.verify_erasure("cust_rot_rtbf")
    assert report["verified"]
