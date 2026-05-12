"""Integration and unit tests for the reconciliation pipeline."""
import pytest
from pathlib import Path
from datetime import date

from src.ingestion.loader import NormalizedTransaction, SourceLoader
from src.matching.engine import MatchingEngine
from src.classification.classifier import DiscrepancyClassifier, DiscrepancyType
from src.pipeline import load_config

CONFIG_PATH = Path("config/settings.yaml")
SAMPLES = Path("data/samples")


@pytest.fixture
def cfg():
    return load_config(CONFIG_PATH)


@pytest.fixture
def sample_sources(cfg):
    loader = SourceLoader(cfg)
    return loader.load_all({
        "core_banking": SAMPLES / "core_banking.csv",
        "reporting_system": SAMPLES / "reporting_system.csv",
        "third_party_aggregator": SAMPLES / "aggregator.csv",
        "manual_entries": SAMPLES / "manual_entries.csv",
    })


def _make_txn(source, source_id, amount, value_date, description, reference):
    return NormalizedTransaction(
        source=source,
        source_id=source_id,
        amount=amount,
        currency="USD",
        value_date=value_date,
        description=description,
        reference=reference,
    )


class TestIngestion:
    def test_all_sources_loaded(self, sample_sources):
        assert set(sample_sources.keys()) == {
            "core_banking", "reporting_system",
            "third_party_aggregator", "manual_entries"
        }

    def test_amounts_are_float(self, sample_sources):
        for txns in sample_sources.values():
            for t in txns:
                assert isinstance(t.amount, float)

    def test_dates_are_date_objects(self, sample_sources):
        for txns in sample_sources.values():
            for t in txns:
                assert isinstance(t.value_date, date)

    def test_core_banking_count(self, sample_sources):
        assert len(sample_sources["core_banking"]) == 12

    def test_description_uppercased(self, sample_sources):
        for txns in sample_sources.values():
            for t in txns:
                assert t.description == t.description.upper()


class TestMatching:
    def test_groups_formed(self, cfg, sample_sources):
        engine = MatchingEngine(cfg)
        groups = engine.match(sample_sources)
        assert len(groups) > 0

    def test_no_duplicate_sources_in_group(self, cfg, sample_sources):
        engine = MatchingEngine(cfg)
        groups = engine.match(sample_sources)
        for g in groups:
            sources = list(g.transactions.keys())
            assert len(sources) == len(set(sources))

    def test_exact_match_high_confidence(self, cfg):
        engine = MatchingEngine(cfg)
        t1 = _make_txn("core_banking", "A1", 1500.0, date(2024, 1, 15), "WIRE ACME", "REF-001")
        t2 = _make_txn("reporting_system", "B1", 1500.0, date(2024, 1, 15), "WIRE ACME", "REF-001")
        score = engine._score_pair(t1, t2)
        assert score >= 90

    def test_amount_diff_lowers_score(self, cfg):
        engine = MatchingEngine(cfg)
        t1 = _make_txn("core_banking", "A1", 1500.0, date(2024, 1, 15), "WIRE ACME", "REF-001")
        t2 = _make_txn("reporting_system", "B1", 999.0, date(2024, 1, 15), "WIRE ACME", "REF-001")
        score = engine._score_pair(t1, t2)
        assert score < 80


class TestClassification:
    def _run(self, cfg, sources):
        engine = MatchingEngine(cfg)
        groups = engine.match(sources)
        classifier = DiscrepancyClassifier(cfg)
        return groups, classifier.classify(groups)

    def test_rounding_detected(self, cfg):
        t1 = _make_txn("core_banking", "X1", 75.50, date(2024, 1, 16), "FUEL CARD", "REF-2024-004")
        t2 = _make_txn("reporting_system", "X2", 75.51, date(2024, 1, 16), "FUEL CARD", "REF-2024-004")
        classifier = DiscrepancyClassifier(cfg)

        from src.matching.engine import MatchGroup
        g = MatchGroup(
            group_id="GRP00001",
            transactions={"core_banking": t1, "reporting_system": t2},
            confidence=0.9,
        )
        disc = classifier._classify_group(g)
        assert disc is not None
        assert DiscrepancyType.ROUNDING in disc.types

    def test_timing_detected(self, cfg):
        t1 = _make_txn("core_banking", "X1", 4500.0, date(2024, 1, 18), "CHECK DEPOSIT", "REF-2024-007")
        t2 = _make_txn("manual_entries", "X2", 4500.0, date(2024, 1, 19), "CHECK DEPOSIT", "REF-2024-007")
        classifier = DiscrepancyClassifier(cfg)

        from src.matching.engine import MatchGroup
        g = MatchGroup(
            group_id="GRP00002",
            transactions={"core_banking": t1, "manual_entries": t2},
            confidence=0.85,
        )
        disc = classifier._classify_group(g)
        assert disc is not None
        assert DiscrepancyType.TIMING in disc.types

    def test_missing_detected(self, cfg, sample_sources):
        _, discs = self._run(cfg, sample_sources)
        missing = [d for d in discs if DiscrepancyType.MISSING in d.types]
        assert len(missing) > 0

    def test_amount_mismatch_detected(self, cfg):
        """Amount mismatch is classified when groups are formed with significant diff."""
        from src.matching.engine import MatchGroup
        t1 = _make_txn("core_banking", "Z1", 1200.00, date(2024, 1, 20), "WIRE DELTA CO", "REF-2024-012")
        t2 = _make_txn("reporting_system", "Z2", 1250.00, date(2024, 1, 20), "WIRE DELTA CO", "REF-2024-012")
        classifier = DiscrepancyClassifier(cfg)
        g = MatchGroup(
            group_id="GRP00099",
            transactions={"core_banking": t1, "reporting_system": t2},
            confidence=0.75,
        )
        disc = classifier._classify_group(g)
        assert disc is not None
        assert DiscrepancyType.AMOUNT_MISMATCH in disc.types
        assert disc.severity in ("MEDIUM", "HIGH", "CRITICAL")


class TestPipeline:
    def test_full_run(self, cfg, sample_sources, tmp_path):
        import time
        from src.matching.engine import MatchingEngine
        from src.classification.classifier import DiscrepancyClassifier
        from src.reporting.generator import ReportGenerator

        cfg["reporting"]["output_dir"] = str(tmp_path / "reports")

        t0 = time.perf_counter()
        groups = MatchingEngine(cfg).match(sample_sources)
        discs = DiscrepancyClassifier(cfg).classify(groups)
        elapsed = time.perf_counter() - t0

        reporter = ReportGenerator(cfg)
        outputs = reporter.generate("TEST-RUN", groups, discs, elapsed)

        assert "json" in outputs
        assert Path(outputs["json"]).exists()
        assert elapsed < cfg["reconciliation"]["sla_minutes"] * 60

    def test_sla_respected(self, cfg, sample_sources):
        import time
        t0 = time.perf_counter()
        MatchingEngine(cfg).match(sample_sources)
        elapsed = time.perf_counter() - t0
        sla = cfg["reconciliation"]["sla_minutes"] * 60
        assert elapsed < sla, f"Pipeline exceeded SLA: {elapsed:.1f}s > {sla}s"
