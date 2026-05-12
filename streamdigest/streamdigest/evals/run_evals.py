"""Tiny eval harness — runs fixtures through the real enrichment pipeline
against a local Ollama instance and prints a pass/fail matrix.

Not statistically rigorous. Intent: catch regressions when you change the
prompt or swap the model. Run manually with `streamdigest evals` or
`python -m streamdigest.evals.run_evals`.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from rich.console import Console
from rich.table import Table

from streamdigest.ai import OllamaClient, enrich_one
from streamdigest.ai.ollama_client import OllamaError

FIXTURES_PATH = Path(__file__).parent / "fixtures.json"


def _check(fixture: dict[str, Any], enrichment: Any) -> tuple[bool, list[str]]:
    exp = fixture["expect"]
    failures: list[str] = []

    if "priority_min" in exp and enrichment.priority < exp["priority_min"]:
        failures.append(f"priority {enrichment.priority} < min {exp['priority_min']}")
    if "priority_max" in exp and enrichment.priority > exp["priority_max"]:
        failures.append(f"priority {enrichment.priority} > max {exp['priority_max']}")
    if "sentiment_any_of" in exp and enrichment.sentiment not in exp["sentiment_any_of"]:
        failures.append(f"sentiment {enrichment.sentiment!r} not in {exp['sentiment_any_of']}")
    if "summary_must_mention_any" in exp:
        low = enrichment.summary.lower()
        needles = [n.lower() for n in exp["summary_must_mention_any"]]
        if not any(n in low for n in needles):
            failures.append(f"summary missing any of {exp['summary_must_mention_any']}")

    return (not failures, failures)


def run() -> int:
    console = Console()
    fixtures = json.loads(FIXTURES_PATH.read_text())

    table = Table(title="streamdigest evals", show_lines=True)
    table.add_column("case", style="cyan", no_wrap=True)
    table.add_column("priority")
    table.add_column("sentiment")
    table.add_column("summary", overflow="fold")
    table.add_column("result")

    passed = failed = 0
    with OllamaClient() as client:
        if not client.ping():
            console.print(
                f"[red]Ollama not reachable at {client.host}. "
                "Start it with `ollama serve` and `ollama pull "
                f"{client.model}`.[/red]"
            )
            return 2

        for fx in fixtures:
            try:
                e = enrich_one(fx["input"], client)
            except OllamaError as exc:
                table.add_row(fx["name"], "-", "-", str(exc)[:80], "[red]ERROR[/red]")
                failed += 1
                continue

            ok, reasons = _check(fx, e)
            result = "[green]PASS[/green]" if ok else f"[red]FAIL[/red] — {'; '.join(reasons)}"
            table.add_row(fx["name"], str(e.priority), e.sentiment, e.summary, result)
            if ok:
                passed += 1
            else:
                failed += 1

    console.print(table)
    console.print(f"\n[bold]{passed} passed, {failed} failed[/bold]")
    return 0 if failed == 0 else 1


if __name__ == "__main__":
    raise SystemExit(run())
