"""Correctness validator: runs Lambda and Kappa on the same dataset and compares results."""

from __future__ import annotations

import logging
from pathlib import Path
from typing import Any

from src.config import HISTORICAL_DIR, config
from src.kappa_arch.stream_processor import KappaProcessor
from src.lambda_arch.batch_layer import BatchProcessor
from src.validator.report import ValidationReport
from src.validator.tolerance import ToleranceChecker

logger = logging.getLogger(__name__)


class CorrectnessValidator:
    """Runs Lambda batch and Kappa replay on the same historical dataset, then compares.

    Comparison sections:
    - ``hourly_event_counts``: counts must be exact
    - ``user_totals``: counts exact, amounts within relative tolerance
    - ``event_type_summary``: counts exact, amounts/averages within relative tolerance
    """

    def __init__(
        self,
        historical_dir: Path = HISTORICAL_DIR,
        local_mode: bool | None = None,
        amount_rel_tolerance: float | None = None,
    ) -> None:
        self.historical_dir = historical_dir
        self.local_mode = config.local_mode if local_mode is None else local_mode
        self.tolerance = ToleranceChecker(
            amount_rel_tolerance=(
                amount_rel_tolerance
                if amount_rel_tolerance is not None
                else config.tolerance.amount_rel_tolerance
            ),
            missing_key_is_mismatch=config.tolerance.missing_key_is_mismatch,
        )

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def run(self, dataset_name: str = "historical") -> ValidationReport:
        """Execute full validation; returns a ValidationReport."""
        logger.info("CorrectnessValidator: starting")

        lambda_results = self._run_lambda()
        kappa_results = self._run_kappa()

        report = ValidationReport(dataset_name=dataset_name)
        report.comparisons = self._compare_all(lambda_results, kappa_results)

        logger.info(
            "Validation complete: %d checks, %d mismatches",
            report.total_count,
            report.mismatch_count,
        )
        return report

    # ------------------------------------------------------------------
    # Internal: run each architecture
    # ------------------------------------------------------------------

    def _run_lambda(self) -> dict[str, Any]:
        """Run the Lambda batch processor and return its aggregation results."""
        logger.info("Running Lambda batch layer...")
        processor = BatchProcessor(historical_dir=self.historical_dir)
        view = processor.run()
        return {
            "hourly_event_counts": view.hourly_event_counts.data,
            "user_totals": view.user_totals.data,
            "event_type_summary": view.event_type_summary.data,
        }

    def _run_kappa(self) -> dict[str, Any]:
        """Run the Kappa replay processor and return its aggregation results."""
        logger.info("Running Kappa replay processor...")
        processor = KappaProcessor(local_mode=self.local_mode)
        processor.run_replay(historical_dir=self.historical_dir)
        snap = processor.get_results()
        return {
            "hourly_event_counts": snap["hourly_event_counts"],
            "user_totals": snap["user_totals"],
            "event_type_summary": snap["event_type_summary"],
        }

    # ------------------------------------------------------------------
    # Internal: comparison logic
    # ------------------------------------------------------------------

    def _compare_all(
        self,
        lambda_results: dict[str, Any],
        kappa_results: dict[str, Any],
    ) -> list:
        from src.validator.tolerance import FieldComparison, MatchResult

        comparisons = []

        # hourly_event_counts: {hour: {event_type: count}}
        comparisons.extend(
            self._compare_nested_counts(
                section="hourly_event_counts",
                lambda_data=lambda_results["hourly_event_counts"],
                kappa_data=kappa_results["hourly_event_counts"],
            )
        )

        # user_totals: {user_id: {total_amount, event_count}}
        for key in sorted(set(lambda_results["user_totals"]) | set(kappa_results["user_totals"])):
            lv = lambda_results["user_totals"].get(key)
            kv = kappa_results["user_totals"].get(key)
            if lv is None or kv is None:
                result = MatchResult.MISSING_LEFT if lv is None else MatchResult.MISSING_RIGHT
                comparisons.append(
                    FieldComparison(
                        key=f"user_totals/{key}",
                        field="*",
                        lambda_value=lv,
                        kappa_value=kv,
                        result=result,
                        delta_pct=None,
                    )
                )
            else:
                comparisons.extend(
                    self.tolerance.compare_dicts(f"user_totals/{key}", lv, kv)
                )

        # event_type_summary: {event_type: {count, total_amount, avg_amount}}
        for key in sorted(
            set(lambda_results["event_type_summary"]) | set(kappa_results["event_type_summary"])
        ):
            lv = lambda_results["event_type_summary"].get(key)
            kv = kappa_results["event_type_summary"].get(key)
            if lv is None or kv is None:
                result = MatchResult.MISSING_LEFT if lv is None else MatchResult.MISSING_RIGHT
                comparisons.append(
                    FieldComparison(
                        key=f"event_type_summary/{key}",
                        field="*",
                        lambda_value=lv,
                        kappa_value=kv,
                        result=result,
                        delta_pct=None,
                    )
                )
            else:
                comparisons.extend(
                    self.tolerance.compare_dicts(f"event_type_summary/{key}", lv, kv)
                )

        return comparisons

    def _compare_nested_counts(
        self,
        section: str,
        lambda_data: dict[str, dict[str, int]],
        kappa_data: dict[str, dict[str, int]],
    ) -> list:
        from src.validator.tolerance import FieldComparison, MatchResult

        comparisons = []
        all_hours = sorted(set(lambda_data) | set(kappa_data))
        for hour in all_hours:
            lv = lambda_data.get(hour, {})
            kv = kappa_data.get(hour, {})
            all_types = sorted(set(lv) | set(kv))
            for et in all_types:
                l_count = lv.get(et, 0)
                k_count = kv.get(et, 0)
                result = MatchResult.EXACT_MATCH if l_count == k_count else MatchResult.MISMATCH
                comparisons.append(
                    FieldComparison(
                        key=f"{section}/{hour}",
                        field=et,
                        lambda_value=l_count,
                        kappa_value=k_count,
                        result=result,
                        delta_pct=0.0 if l_count == k_count else None,
                    )
                )
        return comparisons
