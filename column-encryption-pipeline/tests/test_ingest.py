"""Tests for the ingest pipeline — write, read, dual-read during rotation."""

import pytest
from src.pipeline.ingest import CustomerAlreadyExistsError, CustomerForgottenError, CustomerNotFoundError


def test_register_and_ingest(pipeline, sample_row):
    pipeline.register_customer("cust_001")
    record_id = pipeline.ingest("cust_001", sample_row)
    assert record_id

    result = pipeline.read("cust_001", record_id)
    assert result["ssn"] == "123-45-6789"
    assert result["email"] == "alice@example.com"
    assert result["product_id"] == "prod_abc"


def test_double_registration_raises(pipeline):
    pipeline.register_customer("cust_dup")
    with pytest.raises(CustomerAlreadyExistsError):
        pipeline.register_customer("cust_dup")


def test_ingest_unknown_customer_raises(pipeline, sample_row):
    with pytest.raises(CustomerNotFoundError):
        pipeline.ingest("nobody", sample_row)


def test_read_nonexistent_record_returns_none(pipeline):
    pipeline.register_customer("cust_read")
    result = pipeline.read("cust_read", "does-not-exist")
    assert result is None


def test_ingest_batch(pipeline, sample_row):
    pipeline.register_customer("cust_batch")
    rows = [dict(sample_row, product_id=f"prod_{i}") for i in range(10)]
    ids = pipeline.ingest_batch("cust_batch", rows)
    assert len(ids) == 10

    all_records = pipeline.read_all("cust_batch")
    assert len(all_records) == 10
    product_ids = {r["product_id"] for r in all_records}
    assert product_ids == {f"prod_{i}" for i in range(10)}


def test_none_pii_value_not_stored_encrypted(pipeline):
    pipeline.register_customer("cust_null")
    row = {"ssn": None, "email": "bob@example.com", "product_id": "prod_x"}
    record_id = pipeline.ingest("cust_null", row)
    result = pipeline.read("cust_null", record_id)
    assert result["ssn"] is None
    assert result["email"] == "bob@example.com"
