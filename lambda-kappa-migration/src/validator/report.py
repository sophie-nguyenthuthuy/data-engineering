"""Validation report: rich terminal output and JSON file generation."""

from __future__ import annotations

import json
import logging
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Any

from src.validator.tolerance import FieldComparison, MatchResult

logger = logging.getLogger(__name__)


@dataclass
class ValidationReport:
    """Aggregated results from comparing Lambda and Kappa outputs."""

    run_at: datetime = field(default_factory=datetime.utcnow)
    comparisons: list[FieldComparison] = field(default_factory=list)
    dataset_name: str = "default"

    # ------------------------------------------------------------------
    # Summary properties
    # ------------------------------------------------------------------

    @property
    def passed(self) -> bool:
        """True if every comparison passed (exact or within tolerance)."""
        return all(c.passed for c in self.comparisons)

    @property
    def total_count(self) -> int:
        return len(self.comparisons)

    @property
    def match_count(self) -> int:
        return sum(1 for c in self.comparisons if c.result == MatchResult.EXACT_MATCH)

    @property
    def within_tolerance_count(self) -> int:
        return sum(1 for c in self.comparisons if c.result == MatchResult.WITHIN_TOLERANCE)

    @property
    def mismatch_count(self) -> int:
        return sum(1 for c in self.comparisons if not c.passed)

    # ------------------------------------------------------------------
    # Output helpers
    # ------------------------------------------------------------------

    def to_dict(self) -> dict[str, Any]:
        """Serialise the report to a plain dict."""
        return {
            "run_at": self.run_at.isoformat(),
            "dataset": self.dataset_name,
            "summary": {
                "passed": self.passed,
                "total": self.total_count,
                "exact_matches": self.match_count,
                "within_tolerance": self.within_tolerance_count,
                "mismatches": self.mismatch_count,
            },
            "comparisons": [
                {
                    "key": c.key,
                    "field": c.field,
                    "lambda_value": c.lambda_value,
                    "kappa_value": c.kappa_value,
                    "result": c.result.value,
                    "delta_pct": c.delta_pct,
                }
                for c in self.comparisons
            ],
        }

    def save_json(self, path: Path) -> None:
        """Write the report as a JSON file."""
        path.parent.mkdir(parents=True, exist_ok=True)
        with open(path, "w") as fh:
            json.dump(self.to_dict(), fh, indent=2)
        logger.info("Validation report saved to %s", path)

    def print_rich(self) -> None:
        """Print a formatted table to the terminal using the rich library."""
        try:
            from rich.console import Console
            from rich.table import Table
            from rich import box

            console = Console()
            table = Table(
                title=f"[bold]Correctness Validation Report[/bold]  ({self.dataset_name})",
                box=box.ROUNDED,
                show_lines=True,
            )
            table.add_column("Key", style="cyan", no_wrap=True)
            table.add_column("Field", style="magenta")
            table.add_column("Lambda", justify="right")
            table.add_column("Kappa", justify="right")
            table.add_column("Result", justify="center")
            table.add_column("Delta %", justify="right")

            for c in self.comparisons:
                result_style = {
                    MatchResult.EXACT_MATCH: "green",
                    MatchResult.WITHIN_TOLERANCE: "yellow",
                    MatchResult.MISMATCH: "bold red",
                    MatchResult.MISSING_LEFT: "red",
                    MatchResult.MISSING_RIGHT: "red",
                }.get(c.result, "white")

                delta_str = f"{c.delta_pct:.4f}%" if c.delta_pct is not None else "N/A"
                table.add_row(
                    c.key[:40],
                    c.field,
                    str(c.lambda_value),
                    str(c.kappa_value),
                    f"[{result_style}]{c.result.value}[/{result_style}]",
                    delta_str,
                )

            console.print(table)

            status_color = "bold green" if self.passed else "bold red"
            status_text = "PASSED" if self.passed else "FAILED"
            console.print(
                f"\n[{status_color}]Overall: {status_text}[/{status_color}]  "
                f"  {self.match_count} exact  |  "
                f"{self.within_tolerance_count} within tolerance  |  "
                f"{self.mismatch_count} mismatches  "
                f"(total {self.total_count} checks)\n"
            )
        except ImportError:
            # Fallback without rich
            self._print_plain()

    def _print_plain(self) -> None:
        """Plain-text fallback for environments without rich."""
        print(f"\n=== Correctness Validation Report ({self.dataset_name}) ===")
        for c in self.comparisons:
            status = "OK" if c.passed else "FAIL"
            delta = f" delta={c.delta_pct:.4f}%" if c.delta_pct is not None else ""
            print(f"  [{status}] {c.key} / {c.field}: lambda={c.lambda_value} kappa={c.kappa_value}{delta}")
        overall = "PASSED" if self.passed else "FAILED"
        print(
            f"\n{overall}: {self.match_count} exact | "
            f"{self.within_tolerance_count} within tolerance | "
            f"{self.mismatch_count} mismatches\n"
        )
