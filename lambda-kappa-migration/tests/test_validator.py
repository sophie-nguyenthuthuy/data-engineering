"""Tests for the tolerance checker and correctness validator."""

from __future__ import annotations

import json
import uuid
from datetime import datetime
from pathlib import Path

import pytest

from src.validator.tolerance import FieldComparison, MatchResult, ToleranceChecker


class TestToleranceCheckerExact:
    def test_equal_integers_exact_match(self) -> None:
        checker = ToleranceChecker()
        result = checker._compare_exact("key", "count", 42, 42)
        assert result.result == MatchResult.EXACT_MATCH
        assert result.passed

    def test_different_integers_mismatch(self) -> None:
        checker = ToleranceChecker()
        result = checker._compare_exact("key", "count", 42, 43)
        assert result.result == MatchResult.MISMATCH
        assert not result.passed

    def test_count_field_must_be_exact(self) -> None:
        """Count fields must match exactly even when using compare_dicts."""
        checker = ToleranceChecker(amount_rel_tolerance=0.01)
        comps = checker.compare_dicts("k", {"count": 100}, {"count": 101})
        assert len(comps) == 1
        assert comps[0].result == MatchResult.MISMATCH

    def test_event_count_must_be_exact(self) -> None:
        checker = ToleranceChecker()
        comps = checker.compare_dicts("k", {"event_count": 50}, {"event_count": 51})
        assert comps[0].result == MatchResult.MISMATCH


class TestToleranceCheckerRelative:
    def test_identical_amounts_exact_match(self) -> None:
        checker = ToleranceChecker(amount_rel_tolerance=0.0001)
        result = checker._compare_relative("k", "total_amount", 100.0, 100.0)
        assert result.result == MatchResult.EXACT_MATCH
        assert result.delta_pct == 0.0

    def test_within_tolerance_passes(self) -> None:
        """A 0.005% difference should pass with 0.01% tolerance."""
        checker = ToleranceChecker(amount_rel_tolerance=0.0001)
        # 0.005% difference
        result = checker._compare_relative("k", "total_amount", 10000.0, 10000.5)
        assert result.result == MatchResult.WITHIN_TOLERANCE
        assert result.passed

    def test_outside_tolerance_fails(self) -> None:
        """A 1% difference should fail with 0.01% tolerance."""
        checker = ToleranceChecker(amount_rel_tolerance=0.0001)
        result = checker._compare_relative("k", "total_amount", 100.0, 101.0)
        assert result.result == MatchResult.MISMATCH
        assert not result.passed

    def test_delta_pct_computed_correctly(self) -> None:
        checker = ToleranceChecker(amount_rel_tolerance=0.01)
        result = checker._compare_relative("k", "avg_amount", 200.0, 202.0)
        assert abs(result.delta_pct - 1.0) < 1e-6  # 1% delta


class TestToleranceCheckerMissingKeys:
    def test_missing_in_lambda_is_mismatch(self) -> None:
        checker = ToleranceChecker(missing_key_is_mismatch=True)
        comps = checker.compare_dicts("k", {}, {"total_amount": 50.0})
        assert len(comps) == 1
        assert comps[0].result == MatchResult.MISSING_LEFT
        assert not comps[0].passed

    def test_missing_in_kappa_is_mismatch(self) -> None:
        checker = ToleranceChecker(missing_key_is_mismatch=True)
        comps = checker.compare_dicts("k", {"total_amount": 50.0}, {})
        assert comps[0].result == MatchResult.MISSING_RIGHT

    def test_missing_key_ignored_when_flag_off(self) -> None:
        checker = ToleranceChecker(missing_key_is_mismatch=False)
        comps = checker.compare_dicts("k", {}, {"total_amount": 50.0})
        assert len(comps) == 0


class TestCorrectnessValidatorEndToEnd:
    def _write_events(self, tmp_path: Path, n: int = 50) -> Path:
        import random
        rng = random.Random(7)
        events = []
        types = ["purchase", "view", "click", "signup"]
        for i in range(n):
            etype = rng.choice(types)
            events.append({
                "event_id": str(uuid.uuid4()),
                "user_id": f"user_{i % 10}",
                "event_type": etype,
                "amount": round(rng.uniform(1, 100), 2) if etype == "purchase" else 0.0,
                "timestamp": datetime(2024, 1, 1, rng.randint(8, 20), 0).isoformat(),
                "metadata": {},
            })
        out = tmp_path / "2024-01-01.json"
        out.write_text(json.dumps(events))
        return tmp_path

    def test_lambda_kappa_produce_identical_results(self, tmp_path: Path) -> None:
        """On the same dataset, Lambda and Kappa should produce matching reports."""
        from src.validator.correctness_validator import CorrectnessValidator
        hist_dir = self._write_events(tmp_path)
        validator = CorrectnessValidator(historical_dir=hist_dir, local_mode=True)
        report = validator.run()
        # All comparisons should pass
        mismatches = [c for c in report.comparisons if not c.passed]
        assert len(mismatches) == 0, f"Unexpected mismatches: {mismatches}"
        assert report.passed

    def test_report_has_comparisons(self, tmp_path: Path) -> None:
        """Validation report must contain at least one comparison."""
        from src.validator.correctness_validator import CorrectnessValidator
        hist_dir = self._write_events(tmp_path, n=10)
        validator = CorrectnessValidator(historical_dir=hist_dir, local_mode=True)
        report = validator.run()
        assert report.total_count > 0

    def test_report_to_dict_serialisable(self, tmp_path: Path) -> None:
        """Report to_dict() must produce a JSON-serialisable structure."""
        from src.validator.correctness_validator import CorrectnessValidator
        hist_dir = self._write_events(tmp_path, n=5)
        validator = CorrectnessValidator(historical_dir=hist_dir, local_mode=True)
        report = validator.run()
        d = report.to_dict()
        # Should not raise
        json.dumps(d)
        assert d["summary"]["passed"] is True
