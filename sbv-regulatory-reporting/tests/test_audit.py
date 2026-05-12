"""Tests for the audit trail."""
import json
import tempfile
from pathlib import Path

import pytest

from sbv_reporting.audit.trail import AuditTrail


@pytest.fixture
def tmp_trail(tmp_path):
    return AuditTrail("TEST001", log_dir=tmp_path)


class TestAuditTrail:
    def test_log_creates_file(self, tmp_trail, tmp_path):
        tmp_trail.log("TEST_EVENT", {"key": "value"})
        log_files = list(tmp_path.glob("audit_*.jsonl"))
        assert len(log_files) == 1

    def test_log_entry_structure(self, tmp_trail, tmp_path):
        entry = tmp_trail.log("TEST_EVENT", {"detail": "abc"}, operator="OP001")
        assert "entry_hash" in entry
        assert "prev_hash" in entry
        assert entry["event"] == "TEST_EVENT"
        assert entry["operator"] == "OP001"

    def test_chain_verification_passes(self, tmp_trail):
        tmp_trail.log("STEP_1", {})
        tmp_trail.log("STEP_2", {})
        tmp_trail.log("STEP_3", {})
        ok, errors = tmp_trail.verify()
        assert ok
        assert errors == []

    def test_chain_verification_detects_tampering(self, tmp_trail, tmp_path):
        tmp_trail.log("REAL_EVENT", {"amount": 1000})
        log_path = tmp_path / "audit_TEST001.jsonl"

        # Tamper with the entry
        content = log_path.read_text()
        tampered = content.replace('"amount": 1000', '"amount": 9999999')
        log_path.write_text(tampered)

        ok, errors = tmp_trail.verify()
        assert not ok
        assert len(errors) > 0

    def test_summary(self, tmp_trail):
        tmp_trail.log("EVENT_A", {})
        tmp_trail.log("EVENT_A", {})
        tmp_trail.log("EVENT_B", {})
        s = tmp_trail.summary()
        assert s["total_entries"] == 3
        assert s["events"]["EVENT_A"] == 2
        assert s["events"]["EVENT_B"] == 1

    def test_chain_hash_changes(self, tmp_trail):
        h0 = tmp_trail._chain_hash
        tmp_trail.log("E1", {})
        h1 = tmp_trail._chain_hash
        tmp_trail.log("E2", {})
        h2 = tmp_trail._chain_hash
        assert h0 != h1 != h2
